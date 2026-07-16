"""
@pyne
"""
import asyncio
from decimal import Decimal

import pytest

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    ClientOrderIdSpentError,
    ExchangeCapabilityError,
    ExchangeOrderRejectedError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.models import (
    CancelIntent,
    CapabilityLevel,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderType,
)
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError
from pynecore_bybit.helpers import (
    base_to_contracts,
    contracts_to_base,
    format_decimal,
    quantize_qty,
    round_price,
)
from pynecore_bybit.inventory import spot_port_for
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import (
    POSITION_MODE_HEDGE,
    POSITION_MODE_ONE_WAY,
)


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


def _linear_instrument(**overrides) -> InstrumentInfo:
    """Build a linear BTCUSDT perpetual for broker tests."""
    values = dict(
        category='linear',
        symbol='BTCUSDT',
        base_coin='BTC',
        quote_coin='USDT',
        settle_coin='USDT',
        status='Trading',
        tick_size_str='0.10',
        tick_size=0.1,
        qty_step_str='0.001',
        qty_step=0.001,
        min_order_qty=0.001,
        min_order_amt=0.0,
        min_notional=5.0,
        max_limit_order_qty=1500.0,
        max_market_order_qty=150.0,
        contract_type='LinearPerpetual',
        delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


def _position_row(**overrides) -> dict:
    """Build one raw ``/v5/position/list`` row (flat one-way default)."""
    values = dict(
        symbol='BTCUSDT', positionIdx=0, size='0', side='',
        avgPrice='', unrealisedPnl='', liqPrice='', leverage='10',
        tradeMode=0, createdTime='',
    )
    values.update(overrides)
    return values


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


def __test_bybit_duplicate_coid_spent__():
    """A duplicate orderLinkId with a DEAD original raises the spent error.

    Bybit never allows client-id reuse (measured live: a re-create after a
    confirmed cancel is rejected with 170141/110072), so adopting the
    cancelled row would silently report a working order that does not
    exist. The plugin must raise ClientOrderIdSpentError instead — the
    engine re-anchors and re-dispatches under a fresh id.
    """
    # Entry create collides with a CANCELLED original -> spent, not adopted.
    plugin = _FakeBrokerBybit(responses=[
        BybitAPIError("duplicate", ret_code=170141),
        {'list': [{'orderId': '888', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Limit',
                   'qty': '0.0015', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled', 'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(ClientOrderIdSpentError):
        asyncio.run(plugin.execute_entry(_entry_envelope(
            order_type=OrderType.LIMIT, limit=90000.0,
        )))

    # A LIVE original is still adopted (idempotent retry).
    plugin = _FakeBrokerBybit(responses=[
        BybitAPIError("duplicate", ret_code=170141),
        {'list': [{'orderId': '889', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Buy', 'orderType': 'Limit',
                   'qty': '0.0015', 'cumExecQty': '0',
                   'orderStatus': 'New', 'createdTime': '1752600000000'}]},
    ])
    orders = asyncio.run(plugin.execute_entry(_entry_envelope(
        order_type=OrderType.LIMIT, limit=90000.0,
    )))
    assert orders[0].id == '889'


def __test_bybit_exit_leg_spent_rolls_back_sibling__():
    """A spent exit-leg id cleans up the already-placed sibling leg.

    The engine answers ClientOrderIdSpentError by re-dispatching the WHOLE
    bracket under fresh ids, so a sibling this attempt already placed must
    be cancelled first or the fresh bracket would double it. The spent
    error must propagate as-is — NOT wrapped into
    BracketAttachAfterFillRejectedError (which would trigger a defensive
    flatten for a recoverable condition).
    """
    plugin = _FakeBrokerBybit(responses=[
        {'orderId': '201'},                          # TP leg lands
        BybitAPIError("duplicate", ret_code=170141),  # SL leg id collides
        {'list': [{'orderId': '777', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled', 'createdTime': '1752600000000'}]},
        {},                                           # rollback cancel of TP
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled',        # confirming readback
                   'createdTime': '1752600000000'}]},
    ])
    envelope = DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=0.001, tp_price=110000.0, sl_price=90000.0,
        ),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )
    with pytest.raises(ClientOrderIdSpentError):
        asyncio.run(plugin.execute_exit(envelope))
    tp_coid = plugin.calls[0][2]['orderLinkId']
    cancel_endpoint, _, cancel_body = plugin.calls[3]
    assert cancel_endpoint == '/v5/order/cancel'
    assert cancel_body['orderLinkId'] == tp_coid


def _spent_sl_prefix() -> list:
    """Canned responses: TP lands, SL id collides with a CANCELLED original."""
    return [
        {'orderId': '201'},                           # TP leg lands
        BybitAPIError("duplicate", ret_code=170141),  # SL leg id collides
        {'list': [{'orderId': '777', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled', 'createdTime': '1752600000000'}]},
    ]


def _bracket_envelope() -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=0.001, tp_price=110000.0, sl_price=90000.0,
        ),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )


def __test_bybit_exit_leg_spent_ambiguous_rollback__():
    """An ambiguous sibling rollback must not replace the spent signal.

    If the rollback cancel of the already-placed TP leg fails ambiguously,
    letting OrderDispositionUnknownError leave execute_exit would park the
    WHOLE bracket dispatch on the TP coid — pending verification could then
    adopt that lone TP as the complete bracket (position without its stop).
    The plugin must resolve the ambiguity locally (readback + one cancel
    retry) and only when that fails escalate to the defensive-flatten path
    (BracketAttachAfterFillRejectedError).
    """
    # (1) Ambiguous cancel, readback shows the cancel actually LANDED
    # (Cancelled, nothing filled) -> rollback verified, spent propagates.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        BybitAPIError("server error", ret_code=10016),  # rollback cancel
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled', 'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(ClientOrderIdSpentError):
        asyncio.run(plugin.execute_exit(_bracket_envelope()))

    # (2) Ambiguous cancel, readback shows the TP still LIVE -> one cancel
    # retry lands -> confirming readback (dead, zero fills) -> rollback
    # verified, spent propagates.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        BybitAPIError("server error", ret_code=10016),  # rollback cancel
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'New', 'createdTime': '1752600000000'}]},
        {},                                             # cancel retry lands
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0',
                   'orderStatus': 'Cancelled',          # confirming readback
                   'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(ClientOrderIdSpentError):
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    cancels = [c for c in plugin.calls if c[0] == '/v5/order/cancel']
    assert len(cancels) == 2

    # (3) Ambiguous cancel, readback unreadable (both lookup endpoints
    # fail), cancel retry ambiguous again -> disposition cannot be pinned
    # down -> defensive flatten, NOT a whole-dispatch park. The executed
    # amount is unmeasurable, so the close keeps the full bracket size.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        BybitAPIError("server error", ret_code=10016),  # rollback cancel
        BybitAPIError("server error", ret_code=10016),  # realtime lookup
        BybitAPIError("server error", ret_code=10016),  # history lookup
        BybitAPIError("server error", ret_code=10016),  # cancel retry
    ])
    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    assert exc.value.qty == pytest.approx(0.001)

    # (4) Ambiguous cancel, readback shows the TP (partly) EXECUTED -> the
    # bracket can never be cleanly re-dispatched (a fresh TP would double
    # the exit) -> defensive flatten sized to the CONFIRMED RESIDUAL (the
    # booked fill already reduced the position; a full-size spot close
    # would oversell), and no second cancel attempt.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        BybitAPIError("server error", ret_code=10016),  # rollback cancel
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0.0004',
                   'orderStatus': 'PartiallyFilledCanceled',
                   'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    assert exc.value.qty == pytest.approx(0.0006)
    cancels = [c for c in plugin.calls if c[0] == '/v5/order/cancel']
    assert len(cancels) == 1

    # (5) The rollback cancel SUCCEEDS but a fill raced it: the confirming
    # readback shows PartiallyFilledCanceled with executed quantity -> the
    # nominal cancel answer must not be trusted, a fresh bracket would
    # double the already-booked exit -> defensive flatten sized to the
    # residual.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        {},                                             # rollback cancel OK
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0.0004',
                   'orderStatus': 'PartiallyFilledCanceled',
                   'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    assert exc.value.qty == pytest.approx(0.0006)
    cancels = [c for c in plugin.calls if c[0] == '/v5/order/cancel']
    assert len(cancels) == 1

    # (6) The rollback cancel rejects with "order not found" because the
    # TP already FILLED completely: the benign not-found mapping must not
    # count as a rollback -> readback shows Filled -> defensive flatten
    # with a ZERO residual (the full fill already flattened the exposure;
    # a full-size close would sell inventory the position no longer has).
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        BybitAPIError("not found", ret_code=170213),    # rollback cancel
        {'list': [{'orderId': '201', 'orderLinkId': 'whatever',
                   'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Limit',
                   'qty': '0.001', 'cumExecQty': '0.001',
                   'orderStatus': 'Filled', 'createdTime': '1752600000000'}]},
    ])
    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    assert exc.value.qty == 0.0

    # (7) The rollback cancel succeeds but the confirming readback cannot
    # be obtained (both lookup endpoints fail) -> the zero-fill invariant
    # is unproven -> defensive flatten at the full (conservative) size,
    # no second cancel attempt.
    plugin = _FakeBrokerBybit(responses=_spent_sl_prefix() + [
        {},                                             # rollback cancel OK
        BybitAPIError("server error", ret_code=10016),  # realtime lookup
        BybitAPIError("server error", ret_code=10016),  # history lookup
    ])
    with pytest.raises(BracketAttachAfterFillRejectedError) as exc:
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
    assert exc.value.qty == pytest.approx(0.001)
    cancels = [c for c in plugin.calls if c[0] == '/v5/order/cancel']
    assert len(cancels) == 1


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


def _linear_plugin(responses=None, *, started=True) -> '_FakeBrokerBybit':
    """Fake plugin pinned to the linear perpetual.

    ``started=True`` skips the position-mode detection REST call so
    body-shape tests need no canned position rows.
    """
    plugin = _FakeBrokerBybit(responses=responses)
    plugin._market = _linear_instrument()
    if started:
        plugin._broker_started = True
        plugin._position_mode = POSITION_MODE_ONE_WAY
    return plugin


def _inverse_instrument(**overrides) -> InstrumentInfo:
    """Build the inverse BTCUSD perpetual for broker tests."""
    values = dict(
        category='inverse',
        symbol='BTCUSD',
        base_coin='BTC',
        quote_coin='USD',
        settle_coin='BTC',
        status='Trading',
        tick_size_str='0.10',
        tick_size=0.1,
        qty_step_str='1',
        qty_step=1.0,
        min_order_qty=1.0,
        min_order_amt=0.0,
        min_notional=5.0,
        max_limit_order_qty=25_000_000.0,
        max_market_order_qty=5_000_000.0,
        contract_type='InversePerpetual',
        delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


def _inverse_plugin(responses=None, *, started=True) -> '_FakeBrokerBybit':
    """Fake plugin pinned to the inverse BTCUSD perpetual (one-way)."""
    plugin = _FakeBrokerBybit(responses=responses)
    plugin._market = _inverse_instrument()
    if started:
        plugin._broker_started = True
        plugin._position_mode = POSITION_MODE_ONE_WAY
    return plugin


def __test_bybit_linear_capabilities_and_gate__():
    """Position-based capability profile on linear AND inverse"""
    plugin = _linear_plugin()
    caps = plugin.get_capabilities()
    assert caps.reduce_only is CapabilityLevel.NATIVE
    assert caps.fetch_position is CapabilityLevel.NATIVE
    assert caps.short_selling is CapabilityLevel.NATIVE
    assert caps.stop_order is CapabilityLevel.NATIVE
    assert caps.idempotency is CapabilityLevel.NATIVE
    # Conservative until verified live on the demo.
    assert caps.trailing_stop is CapabilityLevel.UNSUPPORTED
    assert caps.tp_sl_bracket is CapabilityLevel.SOFTWARE
    assert caps.oca_cancel is CapabilityLevel.SOFTWARE

    # Inverse shares the position-based profile since M4.
    plugin = _inverse_plugin()
    caps = plugin.get_capabilities()
    assert caps.reduce_only is CapabilityLevel.NATIVE
    assert caps.fetch_position is CapabilityLevel.NATIVE
    assert caps.short_selling is CapabilityLevel.NATIVE


def __test_bybit_position_mode_detection__():
    """positionIdx row pattern -> mode; hedge opts into the PositionPort"""
    # One-way: the symbol query serves a single positionIdx=0 row.
    plugin = _linear_plugin(
        responses=[{'list': [_position_row()]}], started=False,
    )
    asyncio.run(plugin._ensure_broker_started())
    assert plugin._position_mode == POSITION_MODE_ONE_WAY
    assert plugin.position_port is None

    # Hedge: two aggregate legs (positionIdx 1 and 2), zero size included.
    plugin = _linear_plugin(
        responses=[{'list': [_position_row(positionIdx=1),
                             _position_row(positionIdx=2)]}],
        started=False,
    )
    asyncio.run(plugin._ensure_broker_started())
    assert plugin._position_mode == POSITION_MODE_HEDGE
    assert plugin.position_port is plugin


def __test_bybit_linear_entry_bodies__():
    """Linear entries: no spot-isms, trigger orders, hedge positionIdx"""
    # MARKET: no isLeverage / marketUnit (derivative qty is base contracts).
    plugin = _linear_plugin(responses=[{'orderId': '601'}])
    orders = asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.002)))
    _, _, body = plugin.calls[-1]
    assert body['category'] == 'linear'
    assert 'isLeverage' not in body
    assert 'marketUnit' not in body
    assert 'positionIdx' not in body
    assert body['qty'] == '0.002'
    assert orders[0].id == '601'

    # STOP entry: plain trigger order, direction follows the side.
    plugin = _linear_plugin(responses=[{'orderId': '602'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        qty=0.002, order_type=OrderType.STOP, stop=120000.0,
    )))
    _, _, body = plugin.calls[-1]
    assert 'orderFilter' not in body
    assert body['triggerPrice'] == '120000'
    assert body['triggerDirection'] == 1  # buy stop triggers on a rise

    plugin = _linear_plugin(responses=[{'orderId': '603'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        qty=0.002, side='sell', order_type=OrderType.STOP, stop=80000.0,
    )))
    _, _, body = plugin.calls[-1]
    assert body['triggerDirection'] == 2  # sell stop triggers on a fall

    # Hedge account: the entry stamps the intent side's leg index.
    plugin = _linear_plugin(responses=[{'orderId': '604'}])
    plugin._position_mode = POSITION_MODE_HEDGE
    asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.002)))
    assert plugin.calls[-1][2]['positionIdx'] == 1
    plugin._responses = [{'orderId': '605'}]
    asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.002, side='sell')))
    assert plugin.calls[-1][2]['positionIdx'] == 2

    # The linear base-denominated minimum skips loudly.
    plugin = _linear_plugin()
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(qty=0.0004)))
    assert exc.value.reason == 'below_min_size'


def __test_bybit_linear_exit_close_reduce_only__():
    """Linear bracket legs and closes carry the native reduceOnly flag"""
    plugin = _linear_plugin(responses=[{'orderId': '611'}, {'orderId': '612'}])
    legs = asyncio.run(plugin.execute_exit(DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=0.002, tp_price=110000.0, sl_price=90000.0,
        ),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    assert len(legs) == 2
    _, _, tp_body = plugin.calls[0]
    _, _, sl_body = plugin.calls[1]
    assert tp_body['reduceOnly'] is True
    assert 'isLeverage' not in tp_body
    assert sl_body['reduceOnly'] is True
    assert 'orderFilter' not in sl_body
    assert sl_body['triggerDirection'] == 2  # sell SL triggers on a fall
    assert sl_body['triggerPrice'] == '90000'

    plugin = _linear_plugin(responses=[{'orderId': '613'}])
    order = asyncio.run(plugin.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='BTCUSDT', side='sell',
                           qty=0.002),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    _, _, body = plugin.calls[-1]
    assert body['reduceOnly'] is True
    assert 'marketUnit' not in body
    assert order.reduce_only is True


def __test_bybit_linear_get_position__():
    """Venue position read: zero-size row is flat, live row maps fully"""
    plugin = _linear_plugin(responses=[{'list': [_position_row()]}])
    assert asyncio.run(plugin.get_position('BTCUSDT')) is None

    plugin = _linear_plugin(responses=[{'list': [_position_row(
        size='0.002', side='Sell', avgPrice='100000', unrealisedPnl='-1.5',
        liqPrice='198000', leverage='10', tradeMode=1,
        createdTime='1752600000000',
    )]}])
    position = asyncio.run(plugin.get_position('BTCUSDT'))
    assert position is not None
    assert position.side == 'short'
    assert position.size == 0.002
    assert position.entry_price == 100000.0
    assert position.unrealized_pnl == -1.5
    assert position.liquidation_price == 198000.0
    assert position.margin_mode == 'isolated'

    # Hedge mode: the raw legs are netted through the core emulator.
    plugin = _linear_plugin(responses=[{'list': [
        _position_row(positionIdx=1, size='0.003', side='Buy',
                      avgPrice='100000', createdTime='1752600000000'),
        _position_row(positionIdx=2, size='0.001', side='Sell',
                      avgPrice='101000', createdTime='1752600060000'),
    ]}])
    plugin._position_mode = POSITION_MODE_HEDGE
    position = asyncio.run(plugin.get_position('BTCUSDT'))
    assert position is not None
    assert position.side == 'long'
    assert position.size == pytest.approx(0.002)


def __test_bybit_position_port_primitives__():
    """PositionPort: leg parse/order, quantizer, close_leg, trading-stop"""
    plugin = _linear_plugin(responses=[{'list': [
        _position_row(positionIdx=2, size='0.001', side='Sell',
                      avgPrice='101000', createdTime='1752600060000'),
        _position_row(positionIdx=1, size='0.003', side='Buy',
                      avgPrice='100000', createdTime='1752600000000'),
        _position_row(positionIdx=0, size='0'),
    ]}])
    legs = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert [leg.leg_id for leg in legs] == ['1', '2']  # oldest first
    assert legs[0].side == 'buy' and legs[0].qty == 0.003
    assert legs[1].side == 'sell' and legs[1].entry_price == 101000.0

    quantizer = asyncio.run(plugin.get_volume_quantizer('BTCUSDT'))
    assert quantizer(0.0025) == 2  # 2 whole 0.001 steps, floored

    # close_leg: reduce-only market order against the leg's index. The
    # emulator composes the coid as ``{parent}:{leg_id}`` — the colon is
    # outside Bybit's orderLinkId charset and must map to an underscore
    # on the wire (identity keyed by the same mapped id).
    plugin = _linear_plugin(responses=[{'orderId': '701'}])
    asyncio.run(plugin.close_leg(
        'BTCUSDT', '1', 2, 't3st-0a1b2c3d-0sgls2iio-c0:1',
    ))
    endpoint, _, body = plugin.calls[-1]
    assert endpoint == '/v5/order/create'
    assert body['side'] == 'Sell'  # closing the Buy leg
    assert body['qty'] == '0.002'
    assert body['reduceOnly'] is True
    assert body['positionIdx'] == 1
    assert body['orderLinkId'] == 't3st-0a1b2c3d-0sgls2iio-c0_1'
    assert plugin._order_identity['t3st-0a1b2c3d-0sgls2iio-c0_1'] == \
           (None, None, LegType.CLOSE)

    # amend_bracket: trading-stop attach; all-None clears with "0"s.
    plugin = _linear_plugin(responses=[{}])
    asyncio.run(plugin.amend_bracket(
        'BTCUSDT', '2', side='buy', tp_price=90000.04, sl_price=110000.0,
        trail_offset=None, coid='coid-bracket',
    ))
    endpoint, _, body = plugin.calls[-1]
    assert endpoint == '/v5/position/trading-stop'
    assert body['positionIdx'] == 2
    assert body['tpslMode'] == 'Full'
    assert body['takeProfit'] == '90000'
    assert body['stopLoss'] == '110000'
    assert body['trailingStop'] == '0'

    # Measured benign rejects: zero position (10001 + message) and
    # "not modified" (34040) are no-ops; a real reject propagates.
    plugin = _linear_plugin(responses=[
        BybitAPIError("can not set tp/sl/ts for zero position", ret_code=10001),
    ])
    asyncio.run(plugin.amend_bracket(
        'BTCUSDT', '1', side='sell', tp_price=None, sl_price=None,
        trail_offset=None, coid='c1',
    ))
    plugin._responses = [BybitAPIError("not modified", ret_code=34040)]
    asyncio.run(plugin.amend_bracket(
        'BTCUSDT', '1', side='sell', tp_price=None, sl_price=None,
        trail_offset=None, coid='c2',
    ))
    plugin._responses = [BybitAPIError("bad params", ret_code=10001)]
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(plugin.amend_bracket(
            'BTCUSDT', '1', side='sell', tp_price=90000.0, sl_price=None,
            trail_offset=None, coid='c3',
        ))


def __test_bybit_linear_event_filters_and_flat_sweep__():
    """Category/execType filters and the position-cache flat sweep"""
    plugin = _linear_plugin()
    market = plugin._market
    assert market is not None
    plugin._record_identity('coid-lin', pine_id='Long', from_entry=None,
                            leg_type=LegType.ENTRY, qty=0.002)

    # A spot fill of the same symbol name must not be attributed.
    assert plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'spot', 'symbol': 'BTCUSDT', 'execType': 'Trade',
            'execId': 'lx-1', 'orderId': '801', 'orderLinkId': 'coid-lin',
            'side': 'Buy', 'execQty': '0.002', 'execPrice': '100000',
            'execTime': '1752600000000',
        }],
    }, market) == []

    # Funding rows carry no order fill -> skipped.
    assert plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'linear', 'symbol': 'BTCUSDT', 'execType': 'Funding',
            'execId': 'lx-2', 'orderId': '', 'orderLinkId': '',
            'side': 'Buy', 'execQty': '0.002', 'execPrice': '100000',
            'execTime': '1752600000000',
        }],
    }, market) == []

    # A real linear fill translates; the fee currency is the settle coin.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'linear', 'symbol': 'BTCUSDT', 'execType': 'Trade',
            'execId': 'lx-3', 'orderId': '801', 'orderLinkId': 'coid-lin',
            'side': 'Buy', 'execQty': '0.002', 'execPrice': '100000',
            'execFee': '0.11', 'execTime': '1752600000000',
        }],
    }, market)
    assert len(events) == 1
    assert events[0].event_type == 'filled'
    assert events[0].fee_currency == 'USDT'

    # Flat sweep gating: unknown -> not flat; sized -> not flat; zero -> flat.
    assert plugin._deriv_is_flat() is False
    plugin._ingest_position_frame({
        'topic': 'position',
        'data': [{'category': 'linear', 'symbol': 'BTCUSDT',
                  'positionIdx': 0, 'size': '0.002', 'side': 'Buy'}],
    }, market)
    assert plugin._deriv_is_flat() is False
    plugin._ingest_position_frame({
        'topic': 'position',
        'data': [{'category': 'linear', 'symbol': 'BTCUSDT',
                  'positionIdx': 0, 'size': '0', 'side': ''}],
    }, market)
    assert plugin._deriv_is_flat() is True


def __test_bybit_inverse_conversions__():
    """base<->contract helpers: floor to whole contracts, exact inverse"""
    assert base_to_contracts(0.1, 100000, '1') == Decimal('100000') * Decimal('0.1')
    assert base_to_contracts(0.0015, Decimal('100000'), '1') == Decimal('150')
    # Flooring onto the contract grid.
    assert base_to_contracts(0.0000151, 100000, '1') == Decimal('1')
    assert base_to_contracts(0.0000099, 100000, '1') == Decimal('0')
    assert contracts_to_base(150, 100000) == pytest.approx(0.0015)
    assert contracts_to_base(Decimal('100'), Decimal('50000')) == pytest.approx(0.002)


def __test_bybit_inverse_entry_dispatch__():
    """Inverse entries convert base->contracts at the order's anchor"""
    # MARKET entry anchors at the last trade price.
    plugin = _inverse_plugin(responses=[{'orderId': '901'}])
    plugin._last_price = 100000.0
    orders = asyncio.run(plugin.execute_entry(_entry_envelope(
        symbol='BTCUSD', qty=0.0015,
    )))
    _, _, body = plugin.calls[-1]
    assert body['category'] == 'inverse'
    assert body['qty'] == '150'  # 0.0015 BTC * 100000 USD -> 150 contracts
    assert 'marketUnit' not in body and 'isLeverage' not in body
    # The core-facing order reports base units, exactly round-tripped.
    assert orders[0].qty == pytest.approx(0.0015)
    coid = body['orderLinkId']
    assert plugin._dispatch_qty[coid] == 150.0  # wire domain
    assert float(plugin._wire_anchor[coid]) == 100000.0

    # LIMIT entry anchors at its own (tick-snapped) limit price.
    plugin = _inverse_plugin(responses=[{'orderId': '902'}])
    asyncio.run(plugin.execute_entry(_entry_envelope(
        symbol='BTCUSD', qty=0.001, order_type=OrderType.LIMIT, limit=90000.0,
    )))
    _, _, body = plugin.calls[-1]
    assert body['orderType'] == 'Limit'
    assert body['qty'] == '90'
    assert body['price'] == '90000'

    # With no stream price yet a MARKET entry pulls one ticker read.
    plugin = _inverse_plugin(responses=[
        {'list': [{'lastPrice': '50000'}]}, {'orderId': '903'},
    ])
    asyncio.run(plugin.execute_entry(_entry_envelope(symbol='BTCUSD', qty=0.001)))
    endpoint, params, _ = plugin.calls[0]
    assert endpoint == '/v5/market/tickers'
    _, _, body = plugin.calls[-1]
    assert body['qty'] == '50'

    # Below one whole contract -> loud skip, no order sent.
    plugin = _inverse_plugin()
    plugin._last_price = 100000.0
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(
            symbol='BTCUSD', qty=0.0000099,
        )))
    assert exc.value.reason == 'below_min_size'
    assert not plugin.calls

    # Below the 5-USD (== 5-contract) minimum notional -> loud skip.
    plugin = _inverse_plugin()
    plugin._last_price = 100000.0
    with pytest.raises(OrderSkippedByPlugin) as exc:
        asyncio.run(plugin.execute_entry(_entry_envelope(
            symbol='BTCUSD', qty=0.00004,
        )))
    assert exc.value.reason == 'below_min_notional'
    assert not plugin.calls


def __test_bybit_inverse_reduce_and_mirror__():
    """Reduce paths convert through the mirror anchor; full close snaps"""
    # Mirror the post-reversal drift scenario: the venue holds 10200
    # contracts while the core's fill-ledger base is exactly 0.1 — a
    # last-price conversion would leave 100 contracts of residue.
    plugin = _inverse_plugin(responses=[{'orderId': '911'}])
    plugin._last_price = 101000.0
    plugin._inverse_net_contracts = -10200.0
    plugin._inverse_net_base = -0.1
    order = asyncio.run(plugin.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Short', symbol='BTCUSD', side='buy',
                           qty=0.1),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    _, _, body = plugin.calls[-1]
    assert body['qty'] == '10200'  # the venue's exact contract count
    assert body['reduceOnly'] is True
    assert order.qty == pytest.approx(0.1)

    # A partial close maps proportionally through the same anchor.
    plugin = _inverse_plugin(responses=[{'orderId': '912'}])
    plugin._inverse_net_contracts = 10200.0
    plugin._inverse_net_base = 0.1
    asyncio.run(plugin.execute_close(DispatchEnvelope(
        intent=CloseIntent(pine_id='Long', symbol='BTCUSD', side='sell',
                           qty=0.05),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    _, _, body = plugin.calls[-1]
    assert body['qty'] == '5100'

    # Exit bracket legs share the reduce-side conversion.
    plugin = _inverse_plugin(responses=[{'orderId': '913'}, {'orderId': '914'}])
    plugin._inverse_net_contracts = 150.0
    plugin._inverse_net_base = 0.0015
    legs = asyncio.run(plugin.execute_exit(DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSD', side='sell',
            qty=0.0015, tp_price=110000.0, sl_price=90000.0,
        ),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )))
    _, _, tp_body = plugin.calls[0]
    _, _, sl_body = plugin.calls[1]
    assert tp_body['qty'] == '150' and sl_body['qty'] == '150'
    assert tp_body['reduceOnly'] is True
    assert legs[0].qty == pytest.approx(0.0015)


def __test_bybit_inverse_amend_anchor__():
    """In-place amends keep the dispatched anchor pinned per coid"""
    # Unfilled LIMIT amend: the price moves, the anchor does not — a
    # fill can race the amend request, so the coid's conversion rate
    # must never change while the order lives.
    plugin = _inverse_plugin(responses=[{'orderId': '931'}, {'orderId': '931'}])
    old = _entry_envelope(symbol='BTCUSD', qty=0.001,
                          order_type=OrderType.LIMIT, limit=100000.0)
    asyncio.run(plugin.execute_entry(old))
    _, _, body = plugin.calls[-1]
    coid = body['orderLinkId']
    assert body['qty'] == '100'
    new = _entry_envelope(symbol='BTCUSD', qty=0.001,
                          order_type=OrderType.LIMIT, limit=80000.0)
    asyncio.run(plugin.modify_entry(old, new))
    _, _, body = plugin.calls[-1]
    assert body['price'] == '80000'
    assert body['qty'] == '100'  # still the dispatched 100000 anchor
    assert float(plugin._wire_anchor[coid]) == 100000.0

    # Partially filled: the amended total converts at the original
    # 100000, not the amended 80000, so the already-reported slices and
    # the new total share one rate.
    plugin = _inverse_plugin(responses=[{'orderId': '941'}, {'orderId': '941'}])
    market = plugin._market
    assert market is not None
    asyncio.run(plugin.execute_entry(old))
    _, _, body = plugin.calls[-1]
    coid = body['orderLinkId']
    plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'inverse', 'symbol': 'BTCUSD', 'execType': 'Trade',
            'execId': 'ax-1', 'orderId': '941', 'orderLinkId': coid,
            'side': 'Buy', 'execQty': '50', 'execPrice': '100000',
            'execTime': '1752600000000',
        }],
    }, market)
    orders = asyncio.run(plugin.modify_entry(old, new))
    _, _, body = plugin.calls[-1]
    assert body['qty'] == '100'  # 0.001 * the pinned 100000 anchor
    assert float(plugin._wire_anchor[coid]) == 100000.0
    assert orders[0].qty == pytest.approx(0.001)

    # In-flight fill race: a slice lands while the amend request is on
    # the wire. The pinned anchor keeps the racing slice, the amended
    # total and the closing slice on one conversion rate.
    plugin = _inverse_plugin(responses=[{'orderId': '951'}, {'orderId': '951'}])
    market = plugin._market
    assert market is not None
    asyncio.run(plugin.execute_entry(old))
    _, _, body = plugin.calls[-1]
    coid = body['orderLinkId']
    real_call = plugin._call
    fired = []

    async def racing_call(endpoint, params=None, *, method='get',
                          body=None, auth=False):
        if endpoint == '/v5/order/amend' and not fired:
            fired.append(True)
            plugin._translate_executions({
                'topic': 'execution',
                'data': [{
                    'category': 'inverse', 'symbol': 'BTCUSD',
                    'execType': 'Trade', 'execId': 'rx-1', 'orderId': '951',
                    'orderLinkId': coid, 'side': 'Buy', 'execQty': '50',
                    'execPrice': '99000', 'execTime': '1752600000000',
                }],
            }, market)
        return await real_call(endpoint, params, method=method,
                               body=body, auth=auth)

    plugin._call = racing_call
    asyncio.run(plugin.modify_entry(old, new))
    _, _, body = plugin.calls[-1]
    assert body['qty'] == '100'  # pinned anchor, not the amended price
    assert float(plugin._wire_anchor[coid]) == 100000.0
    # The racing slice converted at the dispatched anchor...
    assert plugin._inverse_net_base == pytest.approx(50 / 100000)
    # ...and the closing slice completes the base total exactly.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'inverse', 'symbol': 'BTCUSD', 'execType': 'Trade',
            'execId': 'rx-2', 'orderId': '951', 'orderLinkId': coid,
            'side': 'Buy', 'execQty': '50', 'execPrice': '99500',
            'execTime': '1752600001000',
        }],
    }, market)
    assert events[0].event_type == 'filled'
    assert events[0].order.filled_qty == pytest.approx(0.001)
    assert plugin._inverse_net_base == pytest.approx(0.001)


def __test_bybit_inverse_fill_events_and_seed__():
    """Fills convert at the dispatch anchor; the mirror folds and snaps"""
    plugin = _inverse_plugin(responses=[{'orderId': '921'}])
    market = plugin._market
    assert market is not None
    plugin._last_price = 100000.0
    asyncio.run(plugin.execute_entry(_entry_envelope(
        symbol='BTCUSD', qty=0.0015,
    )))
    _, _, body = plugin.calls[-1]
    coid = body['orderLinkId']

    # Partial slice: 100 of 150 contracts at a drifted execution price —
    # the base view converts at the dispatch anchor, not the exec price.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'inverse', 'symbol': 'BTCUSD', 'execType': 'Trade',
            'execId': 'ix-1', 'orderId': '921', 'orderLinkId': coid,
            'side': 'Buy', 'execQty': '100', 'execPrice': '100500',
            'execFee': '0.0000006', 'execTime': '1752600000000',
        }],
    }, market)
    assert len(events) == 1
    assert events[0].event_type == 'partial'
    assert events[0].fill_qty == pytest.approx(0.001)  # 100 / 100000
    assert events[0].fill_price == 100500.0
    assert events[0].order.remaining_qty == pytest.approx(0.0005)
    # The settle-coin fee normalizes to the quote currency at the
    # execution price — the core books fees in the quote P&L domain.
    assert events[0].fee_currency == 'USD'
    assert events[0].fee == pytest.approx(0.0000006 * 100500)
    assert plugin._inverse_net_contracts == 100.0
    assert plugin._inverse_net_base == pytest.approx(0.001)

    # Closing slice completes the wire total exactly.
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'inverse', 'symbol': 'BTCUSD', 'execType': 'Trade',
            'execId': 'ix-2', 'orderId': '921', 'orderLinkId': coid,
            'side': 'Buy', 'execQty': '50', 'execPrice': '100700',
            'execTime': '1752600001000',
        }],
    }, market)
    assert events[0].event_type == 'filled'
    assert events[0].order.filled_qty == pytest.approx(0.0015)
    assert plugin._inverse_net_contracts == 150.0

    # A sell of the full contract count snaps the mirror to exactly flat.
    plugin._record_identity('coid-close', pine_id=None, from_entry='Long',
                            leg_type=LegType.CLOSE, qty=150.0)
    plugin._wire_anchor['coid-close'] = Decimal('100000')
    events = plugin._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'inverse', 'symbol': 'BTCUSD', 'execType': 'Trade',
            'execId': 'ix-3', 'orderId': '922', 'orderLinkId': 'coid-close',
            'side': 'Sell', 'execQty': '150', 'execPrice': '101000',
            'execTime': '1752600002000',
        }],
    }, market)
    assert events[0].event_type == 'filled'
    assert plugin._inverse_net_contracts == 0.0
    assert plugin._inverse_net_base == 0.0

    # Startup seed: the venue position anchors at its average entry price.
    plugin = _inverse_plugin()
    plugin._inverse_seed_net([_position_row(
        symbol='BTCUSD', size='10200', side='Sell', avgPrice='102000',
    )])
    assert plugin._inverse_net_contracts == -10200.0
    assert plugin._inverse_net_base == pytest.approx(-0.1)


def __test_bybit_inverse_position_and_hedge_gate__():
    """Position synthesis in base units; hedge mode refused on inverse"""
    plugin = _inverse_plugin(responses=[{'list': [_position_row(
        symbol='BTCUSD', size='10200', side='Sell', avgPrice='102000',
        unrealisedPnl='-0.001', markPrice='101000', leverage='10',
    )]}])
    position = asyncio.run(plugin.get_position('BTCUSD'))
    assert position is not None
    assert position.side == 'short'
    assert position.size == pytest.approx(0.1)  # 10200 / 102000
    assert position.entry_price == 102000.0
    # Settle-coin PnL converts to the quote currency at the mark price.
    assert position.unrealized_pnl == pytest.approx(-0.001 * 101000)

    # Hedge-mode inverse account -> clear startup refusal (one-way only).
    plugin = _inverse_plugin(
        responses=[{'list': [_position_row(symbol='BTCUSD', positionIdx=1),
                             _position_row(symbol='BTCUSD', positionIdx=2)]}],
        started=False,
    )
    with pytest.raises(ExchangeCapabilityError):
        asyncio.run(plugin._ensure_broker_started())
