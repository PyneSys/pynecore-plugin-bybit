"""
@pyne

``fetch_order_execution``: the venue read behind the engine's own recovery of
an execution the stream never delivered (an entry held for its evidence, a
close no journal record settles), for every category, and the spot close over
a held entry that sells only what the inventory ledger holds.
"""
import asyncio
from time import time as epoch_time

import pytest

import pynecore_bybit.history as history_module
from pynecore.core.broker.exceptions import ExchangeConnectionError, OrderSkippedByPlugin
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import (
    CloseIntent,
    DispatchEnvelope,
    ExchangePosition,
    LegType,
    OrderStatus,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import ENTRY_KIND_WORKING

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.helpers import ENTRY_REMAINDER_CANCELLED_EXTRA
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_ONE_WAY


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


#: The journal row id of the held entry ``Long``.
__test_helper_COID = 'coid-held-entry'


class __test_helper_FakeBybit(Bybit):
    """Bybit with the REST dispatcher replaced by canned responses.

    ``responses`` items are ``result`` payloads (returned in order) or
    exceptions (raised in order); every request is recorded as
    ``(endpoint, params)``.
    """

    def __init__(self, market: InstrumentInfo, responses=None):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self.calls: list[tuple[str, dict]] = []
        self.responses = list(responses or [])
        self._market = market
        self._broker_started = True
        if market.category != 'spot':
            self._position_mode = POSITION_MODE_ONE_WAY

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or body or {})))
        if not self.responses:
            raise AssertionError(f"Unexpected REST call: {endpoint} {params} {body}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def __test_helper_instrument(category: str) -> InstrumentInfo:
    """The chart instrument of ``category`` (spot / linear / inverse)."""
    if category == 'inverse':
        return InstrumentInfo(
            category='inverse', symbol='BTCUSD', base_coin='BTC', quote_coin='USD',
            settle_coin='BTC', status='Trading', tick_size_str='0.10', tick_size=0.1,
            qty_step_str='1', qty_step=1.0, min_order_qty=1.0, min_order_amt=0.0,
            min_notional=5.0, max_limit_order_qty=25_000_000.0,
            max_market_order_qty=5_000_000.0, contract_type='InversePerpetual',
            delivery_time=None,
        )
    if category == 'spot':
        return InstrumentInfo(
            category='spot', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
            settle_coin='', status='Trading', tick_size_str='0.01', tick_size=0.01,
            qty_step_str='0.000001', qty_step=0.000001, min_order_qty=0.0,
            min_order_amt=5.0, min_notional=0.0, max_limit_order_qty=100.0,
            max_market_order_qty=50.0, contract_type='', delivery_time=None,
        )
    return InstrumentInfo(
        category='linear', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='USDT', status='Trading', tick_size_str='0.10', tick_size=0.1,
        qty_step_str='0.001', qty_step=0.001, min_order_qty=0.001, min_order_amt=0.0,
        min_notional=5.0, max_limit_order_qty=1500.0, max_market_order_qty=150.0,
        contract_type='LinearPerpetual', delivery_time=None,
    )


def __test_helper_plugin(category: str, responses=None, tmp_path=None,
                         name: str = '') -> __test_helper_FakeBybit:
    """A fake plugin of ``category``, store-backed when ``tmp_path`` is given."""
    plugin = __test_helper_FakeBybit(__test_helper_instrument(category), responses)
    if tmp_path is not None:
        store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name=plugin.plugin_name)
        identity = RunIdentity(strategy_id="recover", symbol=plugin._market.symbol,
                               timeframe="1", account_id=f"recover-{name}")
        plugin.store_ctx = store.open_run(identity, script_source="// recover")
        plugin._adoption_baselined = True
    return plugin


def __test_helper_held_entry_row(plugin, *, qty: float, side: str = 'buy',
                                 anchor: str | None = None) -> None:
    """Journal the held entry ``Long``: its order ``601`` ended, nothing booked."""
    extras: dict = {'kind': ENTRY_KIND_WORKING, 'order_id': '601',
                    ENTRY_REMAINDER_CANCELLED_EXTRA: True}
    if anchor is not None:
        extras['anchor'] = anchor
    plugin.store_ctx.upsert_order(
        __test_helper_COID, symbol=plugin._market.symbol, side=side, qty=qty,
        filled_qty=0.0, state='confirmed', intent_key='Long', pine_entry_id='Long',
        exchange_order_id='601', extras=extras,
    )
    # The dispatch's confirmation records the venue id as a ref (``_confirm_row``).
    plugin.store_ctx.add_ref(__test_helper_COID, 'order_id', '601')


def __test_helper_order(**overrides) -> dict:
    """A venue order object (``/v5/order/history`` row) of the held entry."""
    values = dict(
        orderId='601', orderLinkId=__test_helper_COID, symbol='BTCUSDT', side='Buy',
        orderType='Limit', qty='0.01', price='100000', avgPrice='100000',
        orderStatus='PartiallyFilledCanceled', cumExecQty='0.004', cumExecFee='',
        createdTime=str(int(epoch_time() * 1000) - 60_000),
    )
    values.update(overrides)
    return values


def __test_helper_trade(exec_id: str, qty: str, **overrides) -> dict:
    """One ``/v5/execution/list`` row of order ``601``."""
    values = dict(
        symbol='BTCUSDT', execType='Trade', execId=exec_id, orderId='601',
        orderLinkId=__test_helper_COID, side='Buy', execQty=qty, execPrice='100000',
        execFee='0.02', feeCurrency='USDT', execTime=str(int(epoch_time() * 1000) - 30_000),
        seq='10',
    )
    values.update(overrides)
    return values


def __test_helper_endpoints(plugin) -> list[str]:
    return [endpoint for endpoint, _ in plugin.calls]


# --- found order: the record and every execution ------------------------------


def __test_recovered_execution_is_booked_like_the_stream_and_returned__(tmp_path):
    """An execution the plugin never booked is booked as the backfill would, and returned.

    The order is read by ``orderId`` (open set, then history), its executions
    by ``orderId``; the record is in journal units with the fee summed from
    the executions (``cumExecFee`` is deprecated on linear). The fills carry
    the stream's ``execId`` and also go to the stream's outbox.
    """
    plugin = __test_helper_plugin('linear', [
        {'list': []},
        {'list': [__test_helper_order()]},
        {'list': [__test_helper_trade('e-2', '0.003', seq='11'),
                  __test_helper_trade('e-1', '0.001')], 'nextPageCursor': ''},
    ], tmp_path, 'booked')
    __test_helper_held_entry_row(plugin, qty=0.01)

    execution = asyncio.run(plugin.fetch_order_execution('601', None))

    assert __test_helper_endpoints(plugin) == [
        '/v5/order/realtime', '/v5/order/history', '/v5/execution/list']
    assert plugin.calls[0][1]['orderId'] == '601'
    assert plugin.calls[2][1]['orderId'] == '601'
    assert execution is not None and execution.order is not None
    order = execution.order
    assert (order.id, order.status, order.filled_qty, order.filled_qty_known, order.qty) == (
        '601', OrderStatus.CANCELLED, 0.004, True, 0.01)
    assert order.client_order_id == __test_helper_COID
    assert order.fee == pytest.approx(0.04)
    assert order.fee_currency == 'USDT'
    assert execution.fee_reported is True
    assert execution.fills is not None
    assert [(fill.fill_id, fill.fill_qty, fill.journal_fill_qty, fill.leg_type, fill.pine_id)
            for fill in execution.fills] == [
        ('e-1', 0.001, 0.001, LegType.ENTRY, 'Long'),
        ('e-2', 0.003, 0.003, LegType.ENTRY, 'Long')]
    assert [event.fill_id for event in plugin._event_outbox] == ['e-1', 'e-2']
    assert {'e-1', 'e-2'} <= plugin._seen_exec_ids
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == pytest.approx(0.004)

    # A second read books nothing again: the same fills are built without state.
    plugin.responses = [
        {'list': [__test_helper_order()]},
        {'list': [__test_helper_trade('e-1', '0.001'),
                  __test_helper_trade('e-2', '0.003', seq='11')], 'nextPageCursor': ''},
    ]
    plugin._event_outbox.clear()
    again = asyncio.run(plugin.fetch_order_execution('601', None))
    assert again is not None and again.fills is not None
    assert [fill.fill_id for fill in again.fills] == ['e-1', 'e-2']
    assert [fill.event_type for fill in again.fills] == ['partial', 'partial']
    assert plugin._event_outbox == []
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == pytest.approx(0.004)
    plugin.store_ctx.close()


def __test_inverse_record_is_in_contracts_and_fills_in_base__(tmp_path):
    """Inverse: the record counts contracts (the journal's unit), the fills base.

    Each fill converts at its dispatch's recorded anchor, its settle-coin fee
    to the quote currency at the execution price, as the stream does.
    """
    plugin = __test_helper_plugin('inverse', tmp_path=tmp_path, name='inverse')
    __test_helper_held_entry_row(plugin, qty=200.0, anchor='100000')
    plugin._seen_exec_ids.add('i-1')
    plugin.responses = [
        {'list': [__test_helper_order(symbol='BTCUSD', qty='200', cumExecQty='200',
                                      orderStatus='Filled', avgPrice='100500')]},
        {'list': [__test_helper_trade('i-1', '200', symbol='BTCUSD', execPrice='100500',
                                      execFee='0.000001', feeCurrency='BTC')],
         'nextPageCursor': ''},
    ]
    execution = asyncio.run(plugin.fetch_order_execution('601', None))
    assert execution is not None and execution.order is not None
    assert (execution.order.status, execution.order.qty, execution.order.filled_qty) == (
        OrderStatus.FILLED, 200.0, 200.0)
    (fill,) = execution.fills or ()
    assert fill.fill_id == 'i-1'
    assert fill.event_type == 'filled'
    assert fill.fill_qty == pytest.approx(200 / 100000)
    assert fill.journal_fill_qty == 200.0
    assert fill.fee == pytest.approx(0.000001 * 100500)
    assert fill.fee_currency == 'USD'
    assert execution.order.fee == pytest.approx(fill.fee)
    assert execution.order.fee_currency == 'USD'
    plugin.store_ctx.close()


def __test_spot_fill_is_fee_net_and_booked_into_the_stream__():
    """Spot: a base-coin buy fee nets the fill's base; the journal unit stays gross."""
    plugin = __test_helper_plugin('spot', [
        {'list': [__test_helper_order(orderStatus='Filled', cumExecQty='0.002',
                                      qty='0.002')]},
        {'list': [__test_helper_trade('s-1', '0.002', execFee='0.000002',
                                      feeCurrency='BTC')], 'nextPageCursor': ''},
    ])
    plugin._record_identity(__test_helper_COID, pine_id='Long', from_entry=None,
                            leg_type=LegType.ENTRY, qty=0.002)
    execution = asyncio.run(plugin.fetch_order_execution('601', __test_helper_COID))
    assert execution is not None and execution.order is not None
    (fill,) = execution.fills or ()
    assert fill.fill_qty == pytest.approx(0.002 - 0.000002)
    assert fill.journal_fill_qty == 0.002
    assert (fill.fee, fill.fee_currency) == (pytest.approx(0.000002), 'BTC')
    assert execution.order.filled_qty == 0.002
    assert [event.fill_id for event in plugin._event_outbox] == ['s-1']
    assert 's-1' in plugin._seen_exec_ids


def __test_execution_list_behind_the_order_is_no_answer__(tmp_path):
    """An ended order whose executions do not add up to ``cumExecQty`` is asked again."""
    plugin = __test_helper_plugin('linear', [
        {'list': [__test_helper_order()]},
        {'list': [__test_helper_trade('e-1', '0.001')], 'nextPageCursor': ''},
    ], tmp_path, 'lag')
    __test_helper_held_entry_row(plugin, qty=0.01)
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None
    assert plugin._event_outbox == []
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == 0.0

    # A failed execution read is no answer either.
    plugin.responses = [{'list': [__test_helper_order()]}, BybitConnectionError("HTTP 503")]
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None
    plugin.store_ctx.close()


def __test_fee_without_its_figure_is_reported_as_unreported__():
    """An execution that carries no ``execFee`` makes the order's fee unreported."""
    plugin = __test_helper_plugin('linear', [
        {'list': [__test_helper_order(orderStatus='Filled', cumExecQty='0.001',
                                      qty='0.001')]},
        {'list': [__test_helper_trade('e-1', '0.001', execFee='')], 'nextPageCursor': ''},
    ])
    plugin._seen_exec_ids.add('e-1')
    execution = asyncio.run(plugin.fetch_order_execution('601', __test_helper_COID))
    assert execution is not None
    assert execution.fee_reported is False


def __test_working_order_is_returned_open__():
    """A still working order is reported as it is; the engine asks again later."""
    plugin = __test_helper_plugin('linear', [
        {'list': [__test_helper_order(orderStatus='New', cumExecQty='0')]},
        {'list': [], 'nextPageCursor': ''},
    ])
    execution = asyncio.run(plugin.fetch_order_execution('601', None))
    assert execution is not None and execution.order is not None
    assert execution.order.status is OrderStatus.OPEN
    assert execution.fills == ()


def __test_close_is_read_by_its_client_order_id__(tmp_path):
    """A close the journal cannot settle is found by ``orderLinkId`` and labelled CLOSE."""
    plugin = __test_helper_plugin('linear', tmp_path=tmp_path, name='close')
    envelope = DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='BTCUSDT', side='sell', qty=0.002),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )
    coid = envelope.client_order_id(KIND_CLOSE)
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side='sell', qty=0.002, state='confirmed',
        intent_key='Long', from_entry='Long', exchange_order_id='701',
        extras={'kind': 'close', 'close_of_entry': 'Long'},
    )
    plugin.responses = [
        {'list': [__test_helper_order(orderId='701', orderLinkId=coid, side='Sell',
                                      orderType='Market', qty='0.002', cumExecQty='0.002',
                                      orderStatus='Filled',
                                      createdTime=str(int(epoch_time() * 1000)))]},
        {'list': [__test_helper_trade('c-1', '0.002', orderId='701', orderLinkId=coid,
                                      side='Sell')], 'nextPageCursor': ''},
    ]
    execution = asyncio.run(plugin.fetch_order_execution(None, coid))
    assert plugin.calls[0] == ('/v5/order/realtime', {
        'category': 'linear', 'symbol': 'BTCUSDT', 'orderLinkId': coid})
    assert execution is not None and execution.order is not None
    assert (execution.order.status, execution.order.qty, execution.order.filled_qty) == (
        OrderStatus.FILLED, 0.002, 0.002)
    assert execution.order.client_order_id == coid
    (fill,) = execution.fills or ()
    assert (fill.fill_id, fill.leg_type, fill.from_entry) == ('c-1', LegType.CLOSE, 'Long')
    # The fill reaches the engine through the stream as well: the close's
    # outcome read books no fill itself.
    assert [event.fill_id for event in plugin._event_outbox] == ['c-1']
    plugin.store_ctx.close()


# --- absent order: proven only when nothing of it can exist -------------------


def __test_absent_order_is_proven_unexecuted_only_past_the_grace__(tmp_path, monkeypatch):
    """Neither order endpoint shows it, nor the execution list: nothing executed.

    Only after the indexing grace counted from the journal's dispatch, and
    only through a lookup by ``orderLinkId``.
    """
    plugin = __test_helper_plugin('linear', tmp_path=tmp_path, name='absent')
    __test_helper_held_entry_row(plugin, qty=0.01)
    empty_lookups = [{'list': []}, {'list': []}, {'list': []}, {'list': []}]

    plugin.responses = list(empty_lookups)
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None
    assert '/v5/execution/list' not in __test_helper_endpoints(plugin)

    later = epoch_time() + 60.0
    monkeypatch.setattr(history_module, 'epoch_time', lambda: later)
    plugin.calls.clear()
    plugin.responses = [*empty_lookups, {'list': [], 'nextPageCursor': ''}]
    execution = asyncio.run(plugin.fetch_order_execution('601', None))
    assert execution is not None and execution.order is None
    assert [params.get('orderId') or params.get('orderLinkId')
            for _, params in plugin.calls[:4]] == ['601', '601', __test_helper_COID,
                                                    __test_helper_COID]

    # Executions without an order record: the history lags, the read is asked again.
    plugin.responses = [*empty_lookups,
                        {'list': [__test_helper_trade('e-1', '0.001')], 'nextPageCursor': ''}]
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None
    plugin.store_ctx.close()


def __test_absent_order_without_its_link_id_proves_nothing__():
    """With only an ``orderId`` and no journal row, an empty lookup is no answer."""
    plugin = __test_helper_plugin('linear', [{'list': []}, {'list': []}])
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None


def __test_link_id_naming_another_order_is_not_ours__(tmp_path, monkeypatch):
    """A link id lookup that finds an order with another ``orderId`` leaves ours unseen."""
    plugin = __test_helper_plugin('linear', tmp_path=tmp_path, name='other')
    __test_helper_held_entry_row(plugin, qty=0.01)
    later = epoch_time() + 60.0
    monkeypatch.setattr(history_module, 'epoch_time', lambda: later)
    plugin.responses = [{'list': []}, {'list': []},
                        {'list': [__test_helper_order(orderId='999')]},
                        {'list': [], 'nextPageCursor': ''}]
    execution = asyncio.run(plugin.fetch_order_execution('601', None))
    assert execution is not None and execution.order is None
    plugin.store_ctx.close()


def __test_failed_order_read_raises_and_unreadable_link_lookup_is_no_answer__(tmp_path):
    """An ``orderId`` read that fails raises the broker taxonomy; a failed link lookup is ``None``."""
    plugin = __test_helper_plugin('linear', [BybitConnectionError("HTTP 503")])
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(plugin.fetch_order_execution('601', None))

    plugin = __test_helper_plugin('linear', tmp_path=tmp_path, name='link-fail')
    __test_helper_held_entry_row(plugin, qty=0.01)
    plugin.responses = [{'list': []}, {'list': []}, BybitConnectionError("HTTP 503")]
    assert asyncio.run(plugin.fetch_order_execution('601', None)) is None
    plugin.store_ctx.close()


# --- spot close over a held entry ---------------------------------------------


def __test_helper_spot_close(plugin, qty: float, side: str = 'sell'):
    envelope = DispatchEnvelope(
        intent=CloseIntent(pine_id='__pyne_held_execution_close__0', symbol='BTCUSDT',
                           side=side, qty=qty, immediately=True,
                           synthetic_kind='held_execution_close'),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )
    return asyncio.run(plugin.execute_close(envelope))


def __test_spot_held_close_sells_only_what_the_ledger_holds__(monkeypatch):
    """The spot close sells at most the inventory ledger's base, without borrowing."""
    plugin = __test_helper_plugin('spot', [{'orderId': '801'}])
    plugin._last_price = 100000.0

    async def ledger(_symbol):
        return ExchangePosition(symbol='BTCUSDT', side='long', size=0.003,
                                entry_price=100000.0, unrealized_pnl=0.0,
                                liquidation_price=None, leverage=1.0, margin_mode='cross')

    monkeypatch.setattr(plugin, 'get_position', ledger)
    __test_helper_spot_close(plugin, 0.005)
    ((endpoint, body),) = plugin.calls
    assert endpoint == '/v5/order/create'
    assert (body['side'], body['qty'], body['marketUnit'], body['isLeverage']) == (
        'Sell', '0.003', 'baseCoin', 0)
    assert 'reduceOnly' not in body

    plugin = __test_helper_plugin('spot', [{'orderId': '802'}])
    plugin._last_price = 100000.0
    monkeypatch.setattr(plugin, 'get_position', ledger)
    __test_helper_spot_close(plugin, 0.002)
    ((_, body),) = plugin.calls
    assert body['qty'] == '0.002'


def __test_spot_held_close_over_an_empty_ledger_or_a_buy_sends_nothing__(monkeypatch):
    """A flat ledger, or a buy close (spot holds no short), is declined unsent."""
    plugin = __test_helper_plugin('spot')

    async def flat(_symbol):
        return None

    monkeypatch.setattr(plugin, 'get_position', flat)
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        __test_helper_spot_close(plugin, 0.005)
    assert skipped.value.reason == 'position_flat'
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        __test_helper_spot_close(plugin, 0.005, side='buy')
    assert skipped.value.reason == 'unsupported'
    assert plugin.calls == []
