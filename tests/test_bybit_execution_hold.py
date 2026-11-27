"""
@pyne

Startup recovery of an entry an earlier process sent and never learnt the
outcome of: an order that ended on the venue whose execution this recovery
cannot book is handed to the engine's held-execution path
(``request_execution_hold``); one whose executions the venue history proves is
confirmed as before. While the engine holds an order, its absence is never
taken for a disappearance.
"""
import asyncio
from time import time as epoch_time

from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import (
    ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND,
    BrokerStore,
)
from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION, ENTRY_KIND_WORKING

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.helpers import ENTRY_EXECUTION_HOLD_EXTRA
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_ONE_WAY


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


#: The pending entry row an earlier process left behind.
__test_helper_COID = 'coid-pending-entry'


class __test_helper_Venue(Bybit):
    """A linear Bybit whose REST answers per endpoint from queued responses.

    ``answers`` maps an endpoint to a list of results or exceptions, taken in
    order; an endpoint without one answers an empty list.
    """

    def __init__(self, answers: dict[str, list]):
        super().__init__(config=BybitConfig(), symbol='BTCUSDT', timeframe='1')
        self._market = InstrumentInfo(
            category='linear', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
            settle_coin='USDT', status='Trading', tick_size_str='0.10', tick_size=0.1,
            qty_step_str='0.001', qty_step=0.001, min_order_qty=0.001,
            min_order_amt=0.0, min_notional=5.0, max_limit_order_qty=1500.0,
            max_market_order_qty=150.0, contract_type='LinearPerpetual',
            delivery_time=None,
        )
        self._broker_started = True
        self._position_mode = POSITION_MODE_ONE_WAY
        self.answers = answers
        self.calls: list[str] = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append(endpoint)
        queue = self.answers.get(endpoint)
        if not queue:
            return {'list': [], 'nextPageCursor': ''}
        answer = queue.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def __test_helper_restarted_run(tmp_path, name: str, answers: dict[str, list], *,
                                kind: str = ENTRY_KIND_WORKING):
    """A run whose earlier process left the entry's dispatch pending; the restarted plugin."""
    store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name='bybit')
    identity = RunIdentity(strategy_id="hold", symbol="BTCUSDT", timeframe="1",
                           account_id=f"hold-{name}")
    first = store.open_run(identity, script_source="// hold")
    first.upsert_order(
        __test_helper_COID, symbol='BTCUSDT', side='buy', qty=0.004, filled_qty=0.0,
        state='submitted', intent_key='Long', pine_entry_id='Long',
        extras={'kind': kind},
    )
    first.close()
    plugin = __test_helper_Venue(answers)
    plugin.store_ctx = store.open_run(identity, script_source="// hold")
    return plugin


def __test_helper_order(status: str, cum_exec: str) -> dict:
    """The venue order object of the pending entry."""
    return {
        'orderId': '901', 'orderLinkId': __test_helper_COID, 'symbol': 'BTCUSDT',
        'side': 'Buy', 'orderType': 'Limit', 'qty': '0.004', 'price': '100000',
        'orderStatus': status, 'cumExecQty': cum_exec,
        'createdTime': str(int(epoch_time() * 1000) - 60_000),
    }


def __test_ended_entry_whose_executions_cannot_be_read_goes_to_the_engine__(tmp_path):
    """A Filled entry whose executions the recovery cannot read is handed to the engine.

    The row stays the engine's to settle (it confirms it at its next sync),
    no fill cursor moves, and the row is flagged for the plugin's own passes.
    """
    plugin = __test_helper_restarted_run(tmp_path, "read-failed", {
        '/v5/order/realtime': [{'list': [__test_helper_order('Filled', '0.004')]}],
        '/v5/execution/list': [BybitConnectionError("HTTP 503")],
    })
    asyncio.run(plugin._recover_in_flight_submissions())
    requests = plugin.store_ctx.execution_hold_requests()
    assert set(requests) == {__test_helper_COID}
    assert requests[__test_helper_COID]['order_id'] == '901'
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None
    assert row.state == 'submitted'
    assert row.filled_qty == 0.0
    assert (row.extras or {}).get(ENTRY_EXECUTION_HOLD_EXTRA) is True
    plugin.store_ctx.close()


def __test_ended_entry_without_listed_executions_goes_to_the_engine__(tmp_path):
    """A dead entry with fills whose execution list drains empty is handed over too."""
    plugin = __test_helper_restarted_run(tmp_path, "none-listed", {
        '/v5/order/realtime': [{'list': [__test_helper_order('PartiallyFilledCanceled',
                                                             '0.002')]}],
        '/v5/execution/list': [{'list': [], 'nextPageCursor': ''}],
    })
    asyncio.run(plugin._recover_in_flight_submissions())
    assert set(plugin.store_ctx.execution_hold_requests()) == {__test_helper_COID}
    plugin.store_ctx.close()


def __test_ended_entry_without_a_usable_executed_quantity_goes_to_the_engine__(tmp_path):
    """An ended order whose ``cumExecQty`` proves nothing is handed over, unread."""
    plugin = __test_helper_restarted_run(tmp_path, "no-figure", {
        '/v5/order/realtime': [{'list': [__test_helper_order('Filled', '')]}],
    }, kind=ENTRY_KIND_POSITION)
    asyncio.run(plugin._recover_in_flight_submissions())
    assert set(plugin.store_ctx.execution_hold_requests()) == {__test_helper_COID}
    assert '/v5/execution/list' not in plugin.calls
    plugin.store_ctx.close()


def __test_entry_whose_executions_the_history_proves_is_confirmed_as_before__(tmp_path):
    """Readable executions: the row is confirmed from them, nothing is handed over."""
    plugin = __test_helper_restarted_run(tmp_path, "proven", {
        '/v5/order/realtime': [{'list': [__test_helper_order('Filled', '0.004')]}],
        '/v5/execution/list': [{'list': [{
            'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'e-1', 'orderId': '901',
            'orderLinkId': __test_helper_COID, 'side': 'Buy', 'execQty': '0.004',
            'execPrice': '100000', 'execFee': '0.02',
            'execTime': str(int(epoch_time() * 1000) - 30_000),
        }], 'nextPageCursor': ''}],
    })
    asyncio.run(plugin._recover_in_flight_submissions())
    assert plugin.store_ctx.execution_hold_requests() == {}
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None
    assert (row.state, row.exchange_order_id, row.filled_qty) == ('confirmed', '901', 0.004)
    assert ENTRY_EXECUTION_HOLD_EXTRA not in (row.extras or {})
    assert 'e-1' in plugin._seen_exec_ids
    plugin.store_ctx.close()


def __test_working_entry_or_close_row_is_not_handed_over__(tmp_path):
    """A still working order keeps its stream; a close row is no entry: both stay pending."""
    plugin = __test_helper_restarted_run(tmp_path, "working", {
        '/v5/order/realtime': [{'list': [__test_helper_order('PartiallyFilled', '0.001')]}],
        '/v5/execution/list': [BybitConnectionError("HTTP 503")],
    })
    asyncio.run(plugin._recover_in_flight_submissions())
    assert plugin.store_ctx.execution_hold_requests() == {}
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and row.state == 'submitted'
    plugin.store_ctx.close()

    plugin = __test_helper_restarted_run(tmp_path, "close-row", {
        '/v5/order/realtime': [{'list': [__test_helper_order('Filled', '0.004')]}],
        '/v5/execution/list': [BybitConnectionError("HTTP 503")],
    }, kind='close')
    asyncio.run(plugin._recover_in_flight_submissions())
    assert plugin.store_ctx.execution_hold_requests() == {}
    plugin.store_ctx.close()


def __test_held_order_is_never_tracked_for_disappearance__(tmp_path):
    """While the engine owes its book an order's execution, its absence is no disappearance."""
    plugin = __test_helper_restarted_run(tmp_path, "tracked", {})
    plugin.store_ctx.upsert_order(
        'coid-working', symbol='BTCUSDT', side='buy', qty=0.004, filled_qty=0.0,
        state='confirmed', intent_key='L2', pine_entry_id='L2', exchange_order_id='902',
        extras={'kind': ENTRY_KIND_WORKING, 'order_id': '902'},
    )
    row = plugin.store_ctx.get_order('coid-working')
    assert row is not None
    assert plugin._tracked_refs(row) == {('orders', '902')}

    plugin.store_ctx.replace_keyed_events(ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND, {
        'L2': {'order_ids': ['902'], 'since_ms': 0, 'expired': True},
    })
    assert plugin._tracked_refs(row) == set()

    # A row the plugin handed over stays untracked after the hold ends: its
    # order ended on the venue, nothing of it is left to vanish.
    plugin.store_ctx.replace_keyed_events(ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND, {},
                                          removed=['L2'])
    plugin.store_ctx.upsert_order(
        'coid-working', extras={'kind': ENTRY_KIND_WORKING, 'order_id': '902',
                                ENTRY_EXECUTION_HOLD_EXTRA: True},
    )
    row = plugin.store_ctx.get_order('coid-working')
    assert row is not None
    assert plugin._tracked_refs(row) == set()
    plugin.store_ctx.close()
