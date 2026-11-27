"""
@pyne

The spot inventory manager's own trouble ends in the running process: the
plugin wires both quarantine sinks into it, so the entry block it latched at
startup is released through the engine's release sink once a reconcile
measured the venue again.
"""
import asyncio
from time import time as epoch_time

from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.models import InstrumentInfo


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


class __test_helper_SpotVenue(Bybit):
    """A spot Bybit whose REST answers by endpoint; the wallet read can fail."""

    def __init__(self):
        super().__init__(config=BybitConfig(), symbol='BTCUSDT', timeframe='1')
        self._market = InstrumentInfo(
            category='spot', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
            settle_coin='', status='Trading', tick_size_str='0.01', tick_size=0.01,
            qty_step_str='0.000001', qty_step=0.000001, min_order_qty=0.0,
            min_order_amt=5.0, min_notional=0.0, max_limit_order_qty=100.0,
            max_market_order_qty=50.0, contract_type='', delivery_time=None,
        )
        self.wallet_fails = True
        self.calls: list[str] = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append(endpoint)
        if endpoint == '/v5/account/wallet-balance':
            if self.wallet_fails:
                raise BybitConnectionError("HTTP 503")
            return {'list': [{'coin': [{'coin': 'BTC', 'walletBalance': '0', 'locked': '0'}]}]}
        if endpoint == '/v5/execution/list':
            return {'list': [], 'nextPageCursor': ''}
        raise AssertionError(f"Unexpected REST call: {endpoint} {params} {body}")


async def __test_helper_no_recovery() -> None:
    """Stand-in for the startup in-flight recovery (no pending rows)."""


def __test_spot_inventory_entry_block_is_released_through_the_engine_sink__(tmp_path):
    """The manager latches its startup trouble under its cause and releases it itself.

    The wallet read fails at startup: the manager latches an entry block
    through the plugin's ``quarantine_sink`` under the cause
    ``spot_inventory:BTCUSDT``. Once the venue answers, its reconcile
    measures the balance again and ends that block through the plugin's
    ``quarantine_release_sink``, in this process, without a restart.
    """
    plugin = __test_helper_SpotVenue()
    plugin._account_id = 'bybit-demo-1'
    store = BrokerStore(tmp_path / "release.sqlite", plugin_name=plugin.plugin_name)
    plugin.store_ctx = store.open_run(RunIdentity(
        strategy_id="release", symbol="BTCUSDT", timeframe="1", account_id="release",
    ), script_source="// release")
    latched: list[dict] = []
    released: list[tuple[str, str]] = []

    def latch(_reason, _context=None, **kwargs):
        latched.append(kwargs)
        return True

    def release(cause, resolution):
        released.append((cause, resolution))
        return True

    plugin.quarantine_sink = latch
    plugin.quarantine_release_sink = release
    plugin._recover_in_flight_submissions = __test_helper_no_recovery
    asyncio.run(plugin._ensure_broker_started())
    manager = plugin._spot_manager
    assert manager is not None
    assert latched == [{'cause': 'spot_inventory:BTCUSDT'}]
    assert released == []

    # Still failing: nothing is released.
    asyncio.run(manager.reconcile(int(epoch_time() * 1000) + 10_000_000))
    assert released == []

    plugin.wallet_fails = False
    asyncio.run(manager.reconcile(int(epoch_time() * 1000) + 20_000_000))
    assert [cause for cause, _ in released] == ['spot_inventory:BTCUSDT']
    assert 'measured again' in released[0][1]
    plugin.store_ctx.close()
