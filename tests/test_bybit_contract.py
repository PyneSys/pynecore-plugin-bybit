"""
@pyne

The plugin passes the PyneCore broker plugin contract probe: the checks a live
run makes at startup, and the port probe the order sync engine makes once the
plugin has wired its ports.
"""
from pynecore.core.broker.validation import validate_plugin_contract, validate_plugin_ports

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.inventory import spot_port_for
from pynecore_bybit.models import InstrumentInfo


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


class __test_helper_OfflineBybit(Bybit):
    """The real plugin with its instrument seeded and no transport.

    The probes read the capability profile, which resolves the chart
    instrument; a request that reaches the transport fails the test at once.
    """

    def __init__(self, market: InstrumentInfo) -> None:
        super().__init__(config=BybitConfig(), symbol='BTCUSDT', timeframe='1')
        self._market = market

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        raise AssertionError(f"the contract probe reached the venue: {method} {endpoint}")


def __test_helper_instrument(category: str) -> InstrumentInfo:
    spot = category == 'spot'
    return InstrumentInfo(
        category=category, symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='' if spot else 'USDT', status='Trading',
        tick_size_str='0.01' if spot else '0.10', tick_size=0.01 if spot else 0.1,
        qty_step_str='0.000001' if spot else '0.001', qty_step=0.000001 if spot else 0.001,
        min_order_qty=0.0 if spot else 0.001, min_order_amt=5.0 if spot else 0.0,
        min_notional=0.0 if spot else 5.0, max_limit_order_qty=100.0,
        max_market_order_qty=50.0, contract_type='' if spot else 'LinearPerpetual',
        delivery_time=None,
    )


def __test_plugin_passes_the_contract_probe__():
    plugin = __test_helper_OfflineBybit(__test_helper_instrument('linear'))
    errors, _warnings = validate_plugin_contract(plugin)
    assert errors == []


def __test_hedge_mode_port_passes_the_port_probe__():
    # A hedge-mode derivatives account opts into core one-way emulation with
    # the plugin itself as the port (the broker start wires it).
    plugin = __test_helper_OfflineBybit(__test_helper_instrument('linear'))
    plugin.position_port = plugin
    assert validate_plugin_ports(plugin) == []


def __test_spot_inventory_port_passes_the_port_probe__():
    # On spot the broker start wires the inventory port; the engine probes it
    # on its first sync.
    market = __test_helper_instrument('spot')
    plugin = __test_helper_OfflineBybit(market)
    plugin.spot_inventory_port = spot_port_for(plugin, market)
    assert validate_plugin_ports(plugin) == []
