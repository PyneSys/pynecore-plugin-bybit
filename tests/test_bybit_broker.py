"""
@pyne
"""
import asyncio
from decimal import Decimal

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.models import (
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderType,
)
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError
from pynecore_bybit.helpers import format_decimal, quantize_qty, round_price
from pynecore_bybit.inventory import spot_port_for
from pynecore_bybit.models import InstrumentInfo


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


def _instrument(**overrides) -> InstrumentInfo:
    """Build a spot BTCUSDT instrument for broker tests."""
    values = dict(
        category='spot',
        symbol='BTCUSDT',
        base_coin='BTC',
        quote_coin='USDT',
        settle_coin='',
        status='Trading',
        tick_size_str='0.01',
        tick_size=0.01,
        qty_step_str='0.000001',
        qty_step=0.000001,
        min_order_qty=0.0,
        min_order_amt=5.0,
        min_notional=0.0,
        max_limit_order_qty=100.0,
        max_market_order_qty=50.0,
        contract_type='',
        delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


class _FakeBrokerBybit(Bybit):
    """Bybit with the REST dispatcher replaced by a canned-response fake.

    ``responses`` items are either ``result`` payload dicts (returned in
    order) or exceptions (raised in order). Every request is recorded as
    ``(endpoint, params, body)``.
    """

    def __init__(self, responses=None, **kwargs):
        kwargs.setdefault('config', BybitConfig())
        kwargs.setdefault('symbol', 'BTCUSDT')
        kwargs.setdefault('timeframe', '1')
        super().__init__(**kwargs)
        self.calls = []
        self._responses = list(responses or [])
        self._market = _instrument()

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or {}), dict(body or {})))
        if not self._responses:
            raise AssertionError(f"Unexpected REST call: {endpoint} {params} {body}")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _entry_envelope(**overrides) -> DispatchEnvelope:
    values = dict(
        pine_id='Long', symbol='BTCUSDT', side='buy', qty=0.0015,
        order_type=OrderType.MARKET,
    )
    values.update(overrides)
    return DispatchEnvelope(
        intent=EntryIntent(**values), run_tag='t3st',
        bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )


def __test_bybit_quantize_helpers__():
    """Exact decimal grid helpers: floor qty, snap price, wire format"""
    assert quantize_qty(0.0015678, '0.000001') == Decimal('0.001567')
    assert quantize_qty(0.0009, '0.001') == Decimal('0')
    assert round_price(102345.678, '0.01') == Decimal('102345.68')
    assert round_price(102345.674, '0.01') == Decimal('102345.67')
    assert format_decimal(Decimal('0.001500')) == '0.0015'
    assert format_decimal(Decimal('0')) == '0'
    assert format_decimal(Decimal('100')) == '100'


def __test_bybit_execute_entry__():
    """Entry dispatch: wire bodies, quantization and loud skips"""
    # MARKET entry: base-unit market order with the explicit marketUnit.
    plugin = _FakeBrokerBybit(responses=[{'orderId': '111', 'orderLinkId': 'x'}])
    orders = asyncio.run(plugin.execute_entry(_entry_envelope()))
    endpoint, _, body = plugin.calls[-1]
    assert endpoint == '/v5/order/create'
    assert body['orderType'] == 'Market'
    assert body['marketUnit'] == 'baseCoin'
    assert body['isLeverage'] == 0
    assert body['qty'] == '0.0015'
    assert body['side'] == 'Buy'
    assert orders[0].id == '111'
    assert orders[0].client_order_id == body['orderLinkId']
    # The dispatch identity is recorded for event reverse-mapping.
    assert plugin._order_identity[body['orderLinkId']] == ('Long', None, LegType.ENTRY)

    # LIMIT entry: price snapped to tick, GTC.
    plugin = _FakeBrokerBybit(responses=[{'orderId': '112'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        order_type=OrderType.LIMIT, limit=99999.996,
    )))
    _, _, body = plugin.calls[-1]
    assert body['orderType'] == 'Limit'
    assert body['price'] == '100000'
    assert body['timeInForce'] == 'GTC'

    # STOP entry: conditional market order.
    plugin = _FakeBrokerBybit(responses=[{'orderId': '113'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        order_type=OrderType.STOP, stop=120000.0,
    )))
    _, _, body = plugin.calls[-1]
    assert body['orderType'] == 'Market'
    assert body['orderFilter'] == 'StopOrder'
    assert body['triggerPrice'] == '120000'

    # Below the quantity grid -> loud skip, no order sent.
    plugin = _FakeBrokerBybit()
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.0000005)))
    assert exc.value.reason == 'below_min_size'
    assert not plugin.calls

    # Above the market-order quantity ceiling -> loud skip.
    plugin = _FakeBrokerBybit()
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(qty=51.0)))
    assert exc.value.reason == 'above_max_size'

    # Below the QUOTE-denominated spot minimum (price known) -> loud skip.
    plugin = _FakeBrokerBybit()
    plugin._last_price = 100000.0
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.00004)))
    assert exc.value.reason == 'below_min_notional'


def __test_bybit_execute_exit_and_close__():
    """SOFTWARE bracket legs and the market close"""
    plugin = _FakeBrokerBybit(responses=[
        {'orderId': '201'}, {'orderId': '202'},
    ])
    envelope = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=0.001, tp_price=110000.0, sl_price=90000.0,
        ),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )
    legs = asyncio.run(plugin.execute_exit(envelope))
    assert len(legs) == 2
    _, _, tp_body = plugin.calls[0]
    _, _, sl_body = plugin.calls[1]
    assert tp_body['orderType'] == 'Limit'
    assert tp_body['price'] == '110000'
    assert sl_body['orderFilter'] == 'StopOrder'
    assert sl_body['triggerPrice'] == '90000'
    assert sl_body['orderType'] == 'Market'
    assert {legs[0].client_order_id, legs[1].client_order_id} == \
           {tp_body['orderLinkId'], sl_body['orderLinkId']}
    assert plugin._order_identity[tp_body['orderLinkId']] == \
           ('TP/SL', 'Long', LegType.TAKE_PROFIT)
    assert plugin._order_identity[sl_body['orderLinkId']] == \
           ('TP/SL', 'Long', LegType.STOP_LOSS)

    # Trailing exits are refused loudly (capability is UNSUPPORTED).
    plugin = _FakeBrokerBybit()
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(plugin.execute_exit(DispatchEnvelope(
            intent=ExitIntent(
                pine_id='T', from_entry='Long', symbol='BTCUSDT', side='sell',
                qty=0.001, trail_offset=100.0,
            ),
            run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
        )))

    # Close: reduce-side market order.
    plugin = _FakeBrokerBybit(responses=[{'orderId': '203'}])
    order = asyncio.run(plugin.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='BTCUSDT', side='sell', qty=0.001),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    _, _, body = plugin.calls[-1]
    assert body['orderType'] == 'Market'
    assert body['side'] == 'Sell'
    assert order.reduce_only is True
    assert plugin._order_identity[body['orderLinkId']] == \
           (None, 'Long', LegType.CLOSE)


def __test_bybit_duplicate_coid_adoption__():
    """A duplicate orderLinkId reject adopts the already-landed order"""
    plugin = _FakeBrokerBybit(responses=[
        BybitAPIError("duplicate", ret_code=170141),
        # realtime lookup by orderLinkId finds the original
        {'list': [{'orderId': '999', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Market',
                   'qty': '0.0015', 'cumExecQty': '0.0015',
                   'orderStatus': 'Filled', 'createdTime': '1752600000000'}]},
    ])
    orders = asyncio.run(plugin.execute_entry(_entry_envelope()))
    assert orders[0].id == '999'
    assert plugin.calls[1][0] == '/v5/order/realtime'


def __test_bybit_execute_cancel__():
    """Cancel by orderLinkId; order-not-found is a benign no-op"""
    plugin = _FakeBrokerBybit(responses=[{'orderId': '301'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        order_type=OrderType.LIMIT, limit=90000.0,
    )))
    coid = plugin.calls[-1][2]['orderLinkId']
    plugin._responses = [{}]
    envelope = DispatchEnvelope(
        intent=CancelIntent(pine_id='Long', symbol='BTCUSDT'),
        run_tag='t3st', bar_ts_ms=1_752_600_060_000, coid_max_len=36,
    )
    assert asyncio.run(plugin.execute_cancel(envelope)) is True
    endpoint, _, body = plugin.calls[-1]
    assert endpoint == '/v5/order/cancel'
    assert body['orderLinkId'] == coid

    # Already gone: retCode 170213 normalizes to a benign True.
    plugin._responses = [BybitAPIError("gone", ret_code=170213)]
    assert asyncio.run(plugin.execute_cancel(envelope)) is True

    # cancel-all returns the cancelled count.
    plugin._responses = [{'list': [{'orderId': '1'}, {'orderId': '2'}]}]
    assert asyncio.run(plugin.execute_cancel_all()) == 2


def __test_bybit_event_stream_translation__():
    """Private execution/order pushes -> OrderEvents with Pine identity"""
    plugin = _FakeBrokerBybit()
    market = plugin._market
    assert market is not None
    plugin._record_identity('coid-entry', pine_id='Long', from_entry=None,
                            leg_type=LegType.ENTRY, qty=0.002)

    # First slice: partial fill with incremental qty and execId as fill_id.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'e-1',
            'orderId': '401', 'orderLinkId': 'coid-entry', 'side': 'Buy',
            'execQty': '0.001', 'execPrice': '100000', 'execFee': '0.000001',
            'feeCurrency': 'BTC', 'execTime': '1752600000500',
        }],
    }, market)
    assert len(events) == 1
    event = events[0]
    assert event.event_type == 'partial'
    assert event.fill_qty == 0.001
    assert event.fill_id == 'e-1'
    assert event.pine_id == 'Long'
    assert event.leg_type is LegType.ENTRY
    assert event.order.client_order_id == 'coid-entry'

    # Second slice completes the dispatch quantity -> 'filled'.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'e-2',
            'orderId': '401', 'orderLinkId': 'coid-entry', 'side': 'Buy',
            'execQty': '0.001', 'execPrice': '100010', 'execFee': '0',
            'execTime': '1752600001000',
        }],
    }, market)
    assert events[0].event_type == 'filled'
    assert events[0].order.remaining_qty == 0.0

    # A replayed execId is dropped.
    assert plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'e-2',
            'orderId': '401', 'orderLinkId': 'coid-entry', 'side': 'Buy',
            'execQty': '0.001', 'execPrice': '100010', 'execTime': '1',
        }],
    }, market) == []

    # An external fill (foreign orderLinkId) must not reach the engine.
    assert plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'e-3',
            'orderId': '402', 'orderLinkId': 'someone-else', 'side': 'Sell',
            'execQty': '1', 'execPrice': '100000', 'execTime': '1',
        }],
    }, market) == []

    # Order topic: a cancel transition maps with identity; fills are skipped.
    plugin._record_identity('coid-tp', pine_id='TP/SL', from_entry='Long',
                            leg_type=LegType.TAKE_PROFIT, qty=0.002)
    events = plugin._translate_order_rows({
        'topic': 'order',
        'data': [
            {'symbol': 'BTCUSDT', 'orderId': '403', 'orderLinkId': 'coid-tp',
             'orderStatus': 'Cancelled', 'side': 'Sell', 'orderType': 'Limit',
             'qty': '0.002', 'cumExecQty': '0', 'price': '110000',
             'createdTime': '1752600000000'},
            {'symbol': 'BTCUSDT', 'orderId': '403', 'orderLinkId': 'coid-tp',
             'orderStatus': 'Filled', 'side': 'Sell', 'orderType': 'Limit',
             'qty': '0.002', 'cumExecQty': '0.002', 'price': '110000',
             'createdTime': '1752600000000'},
        ],
    }, market)
    assert len(events) == 1
    assert events[0].event_type == 'cancelled'
    assert events[0].pine_id == 'TP/SL'
    assert events[0].from_entry == 'Long'
    assert events[0].leg_type is LegType.TAKE_PROFIT


def __test_bybit_spot_port_execution_mapping__():
    """Inventory port: canonical deltas, fee inference and attribution"""
    plugin = _FakeBrokerBybit()
    market = plugin._market
    assert market is not None
    port = spot_port_for(plugin, market)
    plugin._record_identity('coid-b', pine_id='L', from_entry=None,
                            leg_type=LegType.ENTRY, qty=1.0)
    plugin._record_identity('coid-s', pine_id=None, from_entry='L',
                            leg_type=LegType.CLOSE, qty=1.0)

    # Buy with base-coin fee: fee reduces the received base.
    buy = port.to_execution({
        'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'x-1',
        'orderId': '501', 'orderLinkId': 'coid-b', 'side': 'Buy',
        'execQty': '0.002', 'execPrice': '100000', 'execValue': '200',
        'execFee': '0.000002', 'feeCurrency': 'BTC', 'execTime': '1752600000000',
    })
    assert buy is not None
    assert buy.base_delta == Decimal('0.001998')
    assert buy.quote_delta == Decimal('-200')
    assert buy.client_order_id == 'coid-b'

    # Sell without feeCurrency: the quote-fee default is inferred.
    sell = port.to_execution({
        'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'x-2',
        'orderId': '502', 'orderLinkId': 'coid-s', 'side': 'Sell',
        'execQty': '0.002', 'execPrice': '101000', 'execValue': '202',
        'execFee': '0.202', 'execTime': '1752600001000',
    })
    assert sell is not None
    assert sell.base_delta == Decimal('-0.002')
    assert sell.quote_delta == Decimal('201.798')
    assert sell.fee_currency == 'USDT'

    # An unattributable fill stays out of the ledger.
    assert port.to_execution({
        'symbol': 'BTCUSDT', 'execType': 'Trade', 'execId': 'x-3',
        'orderId': '503', 'orderLinkId': '', 'side': 'Buy',
        'execQty': '1', 'execPrice': '100000', 'execTime': '1',
    }) is None

    # First startup: empty batch anchored at the venue's current clock.
    batch = asyncio.run(port.fetch_executions(None))
    assert batch.executions == ()
    assert batch.next_cursor is not None
    assert not batch.has_more
