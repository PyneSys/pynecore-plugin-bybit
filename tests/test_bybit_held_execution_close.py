"""
@pyne

The engine's close over an entry whose execution evidence never arrived
(``CloseIntent(synthetic_kind='held_execution_close')``): the capability
declaration per account mode, the venue-sized one-way dispatch (linear and
inverse), its reject / unknown-outcome / restart paths, and the hedge leg
the held order opened, which the core one-way emulation closes.
"""
import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import KIND_CLOSE
from pynecore.core.broker.models import CloseIntent, DispatchEnvelope, LegType
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import (
    ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND,
    HELD_EXECUTION_CLOSE_EVENT_KIND,
    BrokerStore,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    STATE_DISPOSITION_UNKNOWN,
    STATE_REJECTED,
    find_pending_dispatch,
)

import pynecore_bybit.inventory as inventory_module
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, BybitConnectionError
from pynecore_bybit.helpers import (
    ENTRY_CANCEL_CUM_EXEC_EXTRA,
    ENTRY_REMAINDER_CANCELLED_EXTRA,
)
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_HEDGE, POSITION_MODE_ONE_WAY


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


#: The pine id the engine gives its first held-execution close.
__test_helper_HELD_PINE_ID = '__pyne_held_execution_close__0'


class __test_helper_FakeBybit(Bybit):
    """Bybit with the REST dispatcher replaced by canned responses.

    ``responses`` items are ``result`` payloads (returned in order) or
    exceptions (raised in order); every request is recorded as
    ``(endpoint, params, body)``.
    """

    def __init__(self, market: InstrumentInfo, responses=None):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self.calls: list[tuple[str, dict, dict]] = []
        self.responses = list(responses or [])
        self._market = market

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or {}), dict(body or {})))
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


def __test_helper_plugin(category: str, responses=None, *, started: bool = True,
                         mode: str = POSITION_MODE_ONE_WAY) -> __test_helper_FakeBybit:
    """A fake plugin of ``category``; ``started`` skips the startup reads."""
    plugin = __test_helper_FakeBybit(__test_helper_instrument(category), responses)
    if started:
        plugin._broker_started = True
        if category != 'spot':
            plugin._position_mode = mode
    return plugin


def __test_helper_open_run(plugin, tmp_path, name: str):
    """Attach a fresh store run; returns ``(store, identity)``."""
    store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name=plugin.plugin_name)
    identity = RunIdentity(strategy_id="held", symbol=plugin._market.symbol,
                           timeframe="1", account_id=f"held-{name}")
    plugin.store_ctx = store.open_run(identity, script_source="// held")
    return store, identity


def __test_helper_position_row(**overrides) -> dict:
    """One raw ``/v5/position/list`` row (flat one-way default)."""
    values = dict(
        symbol='BTCUSDT', positionIdx=0, size='0', side='', avgPrice='',
        unrealisedPnl='', liqPrice='', leverage='10', tradeMode=0, createdTime='',
    )
    values.update(overrides)
    return values


def __test_helper_envelope(symbol: str, side: str, qty: float, *,
                           kind: str | None = 'held_execution_close') -> DispatchEnvelope:
    """The envelope of the engine's held-execution close (or a plain close)."""
    return DispatchEnvelope(
        intent=CloseIntent(pine_id=__test_helper_HELD_PINE_ID, symbol=symbol, side=side,
                           qty=qty, immediately=True, synthetic_kind=kind),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )


def __test_helper_order_creates(plugin) -> list[dict]:
    """The bodies of every order create the plugin sent."""
    return [body for endpoint, _, body in plugin.calls if endpoint == '/v5/order/create']


def __test_helper_execution(coid: str, *, side: str, qty: str, price: str,
                            category: str) -> dict:
    """An ``execution`` push of one fill of the order ``coid``."""
    return {
        'topic': 'execution',
        'data': [{
            'category': category, 'symbol': 'BTCUSD' if category == 'inverse' else 'BTCUSDT',
            'execType': 'Trade', 'execId': f'x-{coid[-6:]}', 'orderId': '9001',
            'orderLinkId': coid, 'side': side, 'execQty': qty, 'execPrice': price,
            'execFee': '0', 'execTime': '1752600000500',
        }],
    }


# --- capability declaration ---------------------------------------------------


def __test_held_execution_close_is_declared_on_one_way_derivatives_only__():
    """One-way linear and inverse opt in at startup; hedge and spot do not.

    A hedge account needs no opt-in (the core one-way emulation closes its
    legs, ``position_port``); spot has no reduce-only order and its position is
    the inventory ledger's, which cannot show an unbooked execution.
    """
    assert Bybit.supports_held_execution_close is False

    plugin = __test_helper_plugin('linear', [{'list': [__test_helper_position_row()]}],
                                  started=False)
    asyncio.run(plugin._ensure_broker_started())
    assert plugin._position_mode == POSITION_MODE_ONE_WAY
    assert plugin.supports_held_execution_close is True
    assert plugin.position_port is None

    flat = __test_helper_position_row(symbol='BTCUSD')
    plugin = __test_helper_plugin('inverse', [{'list': [flat]}, {'list': [flat]}],
                                  started=False)
    asyncio.run(plugin._ensure_broker_started())
    assert plugin.supports_held_execution_close is True

    plugin = __test_helper_plugin('linear', [{'list': [
        __test_helper_position_row(positionIdx=1),
        __test_helper_position_row(positionIdx=2),
    ]}], started=False)
    asyncio.run(plugin._ensure_broker_started())
    assert plugin._position_mode == POSITION_MODE_HEDGE
    assert plugin.position_port is plugin
    assert plugin.supports_held_execution_close is False

    plugin = __test_helper_plugin('spot', started=False)
    asyncio.run(plugin._ensure_broker_started())
    assert plugin.supports_held_execution_close is False
    assert plugin.calls == []


# --- one-way dispatch: sizing and the reduce-only flag -----------------------


def __test_linear_held_close_closes_the_venue_position_reduce_only__():
    """The close covers the venue position read fresh, as a reduceOnly market order.

    The venue holds 0.005 of which the journal booked 0.002 (the held entry's
    0.003 never got a fill); the engine sends the 0.005 it read.
    """
    held_long = __test_helper_position_row(size='0.005', side='Buy', avgPrice='100000')
    plugin = __test_helper_plugin('linear', [{'list': [held_long]}, {'orderId': '9001'}])
    envelope = __test_helper_envelope('BTCUSDT', 'sell', 0.005)
    order = asyncio.run(plugin.execute_close(envelope))

    assert [endpoint for endpoint, _, _ in plugin.calls] == [
        '/v5/position/list', '/v5/order/create']
    (body,) = __test_helper_order_creates(plugin)
    coid = envelope.client_order_id(KIND_CLOSE)
    assert body == {
        'category': 'linear', 'symbol': 'BTCUSDT', 'side': 'Sell', 'orderType': 'Market',
        'qty': '0.005', 'orderLinkId': coid, 'reduceOnly': True,
    }
    assert order.client_order_id == coid
    assert order.reduce_only is True
    assert order.qty == pytest.approx(0.005)
    # Labelled like any close: the engine matches its fills by this pine id.
    assert plugin._order_identity[coid] == (None, __test_helper_HELD_PINE_ID, LegType.CLOSE)


def __test_linear_held_close_never_exceeds_the_venue_position__():
    """A venue that shrank since the engine's read closes what is left, never more."""
    shrunk = __test_helper_position_row(size='0.003', side='Buy', avgPrice='100000')
    plugin = __test_helper_plugin('linear', [{'list': [shrunk]}, {'orderId': '9002'}])
    asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSDT', 'sell', 0.005)))
    (body,) = __test_helper_order_creates(plugin)
    assert body['qty'] == '0.003'
    assert body['reduceOnly'] is True

    # A venue that grew (someone added) closes the engine's quantity only.
    grown = __test_helper_position_row(size='0.008', side='Buy', avgPrice='100000')
    plugin = __test_helper_plugin('linear', [{'list': [grown]}, {'orderId': '9003'}])
    asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSDT', 'sell', 0.005)))
    (body,) = __test_helper_order_creates(plugin)
    assert body['qty'] == '0.005'


def __test_inverse_held_close_closes_the_venue_contracts_not_the_mirror__():
    """Inverse sizes the close from the venue's contracts, not the net-position mirror.

    The mirror holds the 150 contracts the journal booked; the venue holds
    250 (the held entry's 100 never got a fill). The position the engine reads
    reports 0.0015 + 100 / 100000 BTC; the close sends all 250 contracts, and
    its anchor converts them back to exactly that base. A plain close of the
    same quantity snaps onto the mirror's 150 and would leave the 100 open.
    """
    row = __test_helper_position_row(symbol='BTCUSD', size='250', side='Buy',
                                     avgPrice='100000', markPrice='100000',
                                     unrealisedPnl='0')
    plugin = __test_helper_plugin('inverse', [{'list': [row]}, {'list': [row]},
                                              {'orderId': '9101'}])
    plugin._inverse_net_contracts = 150.0
    plugin._inverse_net_base = 0.0015
    position = asyncio.run(plugin.get_position('BTCUSD'))
    assert position is not None
    assert position.size == pytest.approx(0.0025)

    envelope = __test_helper_envelope('BTCUSD', 'sell', position.size)
    order = asyncio.run(plugin.execute_close(envelope))
    (body,) = __test_helper_order_creates(plugin)
    coid = envelope.client_order_id(KIND_CLOSE)
    assert body['qty'] == '250'
    assert body['reduceOnly'] is True
    assert body['side'] == 'Sell'
    assert plugin._wire_anchor[coid] == Decimal('250') / Decimal(str(position.size))
    assert order.qty == pytest.approx(position.size)

    plugin = __test_helper_plugin('inverse', [{'orderId': '9102'}])
    plugin._inverse_net_contracts = 150.0
    plugin._inverse_net_base = 0.0015
    asyncio.run(plugin.execute_close(
        __test_helper_envelope('BTCUSD', 'sell', position.size, kind='reversal_close')))
    (body,) = __test_helper_order_creates(plugin)
    assert body['qty'] == '150'

    # The held entry was the only one: the mirror holds nothing, the venue
    # position converts at its average price, and the close takes all of it.
    alone = __test_helper_position_row(symbol='BTCUSD', size='100', side='Sell',
                                       avgPrice='80000')
    plugin = __test_helper_plugin('inverse', [{'list': [alone]}, {'orderId': '9103'}])
    asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSD', 'buy', 100 / 80000)))
    (body,) = __test_helper_order_creates(plugin)
    assert (body['qty'], body['side'], body['reduceOnly']) == ('100', 'Buy', True)


def __test_held_close_over_a_flat_or_opposite_venue_sends_nothing__():
    """A venue that is flat, or holds the other side, is declined without an order."""
    for rows in ([__test_helper_position_row()],
                 [__test_helper_position_row(size='0.005', side='Sell', avgPrice='100000')]):
        plugin = __test_helper_plugin('linear', [{'list': rows}])
        with pytest.raises(OrderSkippedByPlugin) as skipped:
            asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSDT', 'sell', 0.005)))
        assert skipped.value.reason == 'position_flat'
        assert __test_helper_order_creates(plugin) == []


def __test_held_close_read_failure_sends_nothing__(tmp_path):
    """A venue read that fails is a retryable connection fault; nothing is journaled or sent."""
    plugin = __test_helper_plugin('linear', [BybitConnectionError("HTTP 503")])
    __test_helper_open_run(plugin, tmp_path, "read-failure")
    envelope = __test_helper_envelope('BTCUSDT', 'sell', 0.005)
    with pytest.raises(ExchangeConnectionError):
        asyncio.run(plugin.execute_close(envelope))
    assert __test_helper_order_creates(plugin) == []
    assert plugin.store_ctx.get_order(envelope.client_order_id(KIND_CLOSE)) is None
    plugin.store_ctx.close()


def __test_spot_without_an_inventory_ledger_closes_nothing__():
    """Spot without persistence has no inventory ledger: nothing it owns, nothing sent.

    Its position is the ledger's, so a store-less spot run reports flat and
    the close is declined; a buy close is declined as spot holds no short.
    """
    plugin = __test_helper_plugin('spot')
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSDT', 'sell', 0.005)))
    assert skipped.value.reason == 'position_flat'
    with pytest.raises(OrderSkippedByPlugin) as skipped:
        asyncio.run(plugin.execute_close(__test_helper_envelope('BTCUSDT', 'buy', 0.005)))
    assert skipped.value.reason == 'unsupported'
    assert plugin.calls == []


def __test_spot_with_its_inventory_ledger_declares_the_close__(tmp_path, monkeypatch):
    """A spot run whose inventory manager started declares the held-execution close."""

    class __test_helper_Manager:
        async def startup(self):
            return SimpleNamespace(quarantined=False, reason='',
                                   fold=SimpleNamespace(net_base=0, fill_count=0),
                                   recovered_fills=0, adopted_fills=0)

    def manager_for(plugin, market, **_kwargs):
        return __test_helper_Manager(), object()

    monkeypatch.setattr(inventory_module, 'spot_manager_for', manager_for)
    plugin = __test_helper_plugin('spot', started=False)
    plugin._account_id = 'bybit-demo-1'
    __test_helper_open_run(plugin, tmp_path, "spot-declared")
    monkeypatch.setattr(plugin, '_recover_in_flight_submissions', __test_helper_no_recovery)
    asyncio.run(plugin._ensure_broker_started())
    assert plugin.supports_held_execution_close is True
    plugin.store_ctx.close()


async def __test_helper_no_recovery() -> None:
    """Stand-in for the startup in-flight recovery (no pending rows)."""


# --- reject and unknown outcome ----------------------------------------------


def __test_held_close_reject_is_a_definitive_reject__(tmp_path):
    """A venue reject (110017 included) surfaces as ExchangeOrderRejectedError, the row rejected."""
    held_long = __test_helper_position_row(size='0.005', side='Buy', avgPrice='100000')
    plugin = __test_helper_plugin('linear', [
        {'list': [held_long]},
        BybitAPIError("Bybit API error on /v5/order/create: retCode=110017 "
                      "retMsg='current position is zero, cannot fix reduce-only order qty'",
                      ret_code=110017),
    ])
    __test_helper_open_run(plugin, tmp_path, "reject")
    envelope = __test_helper_envelope('BTCUSDT', 'sell', 0.005)
    with pytest.raises(ExchangeOrderRejectedError):
        asyncio.run(plugin.execute_close(envelope))
    row = plugin.store_ctx.get_order(envelope.client_order_id(KIND_CLOSE))
    assert row is not None
    assert row.state == STATE_REJECTED
    assert row.closed_ts_ms is not None
    plugin.store_ctx.close()


def __test_held_close_unknown_outcome_stays_a_pending_dispatch__(tmp_path):
    """A send without a venue answer parks under the close's coid and intent key.

    The engine keeps the held close settling while its dispatch has a pending
    journal row under the close's pine id, so it never sends another blindly.
    """
    held_long = __test_helper_position_row(size='0.005', side='Buy', avgPrice='100000')
    plugin = __test_helper_plugin('linear', [{'list': [held_long]},
                                             BybitConnectionError("read timeout")])
    __test_helper_open_run(plugin, tmp_path, "unknown")
    envelope = __test_helper_envelope('BTCUSDT', 'sell', 0.005)
    coid = envelope.client_order_id(KIND_CLOSE)
    with pytest.raises(OrderDispositionUnknownError) as unknown:
        asyncio.run(plugin.execute_close(envelope))
    assert unknown.value.client_order_id == coid
    row = plugin.store_ctx.get_order(coid)
    assert row is not None and row.state == STATE_DISPOSITION_UNKNOWN
    assert [pending.intent_key for pending in find_pending_dispatch(plugin.store_ctx)] == [
        __test_helper_HELD_PINE_ID]
    plugin.store_ctx.close()


# --- fill reporting and restart ----------------------------------------------


def __test_held_close_fill_is_reported_under_its_own_client_order_id__(tmp_path):
    """The close's fill is a CLOSE fill of its own coid, labelled with the close's pine id.

    After a restart the identity and the inverse anchor come from the journal
    row, so the fill is reported the same way.
    """
    row = __test_helper_position_row(symbol='BTCUSD', size='250', side='Buy',
                                     avgPrice='100000')
    plugin = __test_helper_plugin('inverse', [{'list': [row]}, {'orderId': '9001'}])
    plugin._inverse_net_contracts = 150.0
    plugin._inverse_net_base = 0.0015
    store, identity = __test_helper_open_run(plugin, tmp_path, "fill")
    envelope = __test_helper_envelope('BTCUSD', 'sell', 0.0025)
    coid = envelope.client_order_id(KIND_CLOSE)
    asyncio.run(plugin.execute_close(envelope))
    journaled = plugin.store_ctx.get_order(coid)
    assert journaled is not None
    assert (journaled.extras or {})['kind'] == 'close'
    assert (journaled.extras or {})['close_of_entry'] == __test_helper_HELD_PINE_ID
    assert journaled.intent_key == __test_helper_HELD_PINE_ID
    plugin.store_ctx.close()

    # A fresh process on the same run: nothing in memory, everything journaled.
    restarted = __test_helper_plugin('inverse')
    restarted.store_ctx = store.open_run(identity, script_source="// held")
    restarted._inverse_net_contracts = 150.0
    restarted._inverse_net_base = 0.0015
    assert coid not in restarted._order_identity
    assert coid not in restarted._wire_anchor
    market = restarted._market
    assert market is not None
    events = restarted._translate_executions(__test_helper_execution(
        coid, side='Sell', qty='250', price='100000', category='inverse'), market)
    assert len(events) == 1
    (event,) = events
    assert event.event_type == 'filled'
    assert event.leg_type is LegType.CLOSE
    assert event.from_entry == __test_helper_HELD_PINE_ID
    assert event.order.client_order_id == coid
    assert event.fill_qty == pytest.approx(0.0025)
    assert event.journal_fill_qty == pytest.approx(250.0)
    closed = restarted.store_ctx.get_order(coid)
    assert closed is not None and closed.closed_ts_ms is not None
    restarted.store_ctx.close()


# --- hedge mode: the held order's leg is the run's ---------------------------


def __test_helper_hedge_plugin(tmp_path, name: str, legs: dict[int, str]):
    """A store-backed hedge-mode linear plugin whose venue serves ``legs`` (idx -> size)."""
    rows = [
        __test_helper_position_row(positionIdx=idx, size=size,
                                   side='Buy' if idx == 1 else 'Sell',
                                   avgPrice='100000', createdTime=f'175260000{idx}000')
        for idx, size in legs.items()
    ]
    plugin = __test_helper_plugin('linear', [{'list': rows}], mode=POSITION_MODE_HEDGE)
    __test_helper_open_run(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    # The entry the journal booked: 0.002 of the Buy leg.
    plugin.store_ctx.upsert_order(
        'coid-booked', symbol='BTCUSDT', side='buy', qty=0.002, filled_qty=0.002,
        state='confirmed', intent_key='L1', pine_entry_id='L1', exchange_order_id='701',
        extras={'kind': ENTRY_KIND_POSITION, 'order_id': '701'},
    )
    return plugin


def __test_helper_held_entry(plugin, *, side: str = 'buy', qty: float = 0.003,
                             extras: dict | None = None, order_id: str | None = '702',
                             hold_order_ids: list[str] | None = None) -> None:
    """Journal the held entry ``L2`` (ended, nothing booked) and the engine's hold of it."""
    plugin.store_ctx.upsert_order(
        'coid-held', symbol='BTCUSDT', side=side, qty=qty, filled_qty=0.0,
        state='confirmed', intent_key='L2', pine_entry_id='L2', exchange_order_id=order_id,
        extras=extras if extras is not None else {
            'kind': ENTRY_KIND_WORKING, ENTRY_REMAINDER_CANCELLED_EXTRA: True},
    )
    plugin.store_ctx.replace_keyed_events(ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND, {
        'L2': {'order_ids': ['702'] if hold_order_ids is None else hold_order_ids,
               'since_ms': 0, 'expired': True},
    })


def __test_hedge_leg_includes_the_held_entrys_unbooked_execution__(tmp_path):
    """The leg the held order opened is the run's, bounded by the physical leg.

    Without it the core reads 0.002 and its one-way emulation closes 0.002 of
    a 0.005 leg, leaving the held execution open under the quarantine.
    """
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-held", {1: '0.005', 2: '0'})
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert (leg.leg_id, leg.qty, leg.venue_qty) == ('1', 0.002, 0.005)

    __test_helper_held_entry(plugin)
    plugin.responses = [{'list': [
        __test_helper_position_row(positionIdx=1, size='0.005', side='Buy',
                                   avgPrice='100000', createdTime='1752600001000'),
        __test_helper_position_row(positionIdx=2, size='0', side=''),
    ]}]
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert (leg.leg_id, leg.side, leg.qty, leg.venue_qty) == ('1', 'buy', 0.005, None)
    # The run-owned size the flat sweep and adoption read is unchanged.
    assert plugin._run_owned_signed_size() == pytest.approx(0.002)

    # The engine's read for the close (the net of the legs) sees it too.
    plugin.responses = [{'list': [
        __test_helper_position_row(positionIdx=1, size='0.005', side='Buy',
                                   avgPrice='100000', createdTime='1752600001000'),
    ]}]
    position = asyncio.run(plugin.get_position('BTCUSDT'))
    assert position is not None
    assert (position.side, position.size) == ('long', pytest.approx(0.005))
    plugin.store_ctx.close()


def __test_hedge_held_room_never_claims_more_than_the_physical_leg__(tmp_path):
    """The room an ended order left is an upper bound: the physical leg caps it."""
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-cap", {1: '0.003'})
    __test_helper_held_entry(plugin)
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert leg.qty == pytest.approx(0.003)
    plugin.store_ctx.close()


def __test_hedge_held_known_execution_keeps_foreign_exposure_out__(tmp_path):
    """A held entry whose venue cumExecQty is known adds exactly that execution.

    The leg holds 0.006: 0.002 booked, 0.001 the held order executed, 0.003
    someone else's. The run's leg is 0.003, the rest is reported as venue size.
    """
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-known", {1: '0.006'})
    __test_helper_held_entry(plugin, qty=0.001, extras={
        'kind': ENTRY_KIND_WORKING, ENTRY_REMAINDER_CANCELLED_EXTRA: True,
        ENTRY_CANCEL_CUM_EXEC_EXTRA: 0.001,
    })
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert (leg.qty, leg.venue_qty) == (pytest.approx(0.003), 0.006)
    plugin.store_ctx.close()


def __test_hedge_held_entry_of_the_other_side_shows_its_own_leg__(tmp_path):
    """A held sell entry's execution sits on the Sell leg, beside the run's Buy leg."""
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-sell", {1: '0.002', 2: '0.004'})
    __test_helper_held_entry(plugin, side='sell', qty=0.004)
    legs = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert [(leg.leg_id, leg.side, leg.qty) for leg in legs] == [
        ('1', 'buy', pytest.approx(0.002)), ('2', 'sell', pytest.approx(0.004))]
    plugin.store_ctx.close()


def __test_hedge_entry_not_held_or_not_ended_adds_nothing__(tmp_path):
    """Only an entry the engine holds whose order the plugin saw end counts.

    A hold whose entry order is still working (its cancel parked, no end
    seen) leaves the stream to deliver its executions; an ended entry the
    engine does not hold is settled by its own evidence.
    """
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-working", {1: '0.005'})
    __test_helper_held_entry(plugin, extras={'kind': ENTRY_KIND_WORKING})
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert leg.qty == pytest.approx(0.002)
    plugin.store_ctx.close()

    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-unheld", {1: '0.005'})
    __test_helper_held_entry(plugin, hold_order_ids=['799'])
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert leg.qty == pytest.approx(0.002)
    plugin.store_ctx.close()


def __test_hedge_held_entry_is_matched_by_its_venue_order_id_only__(tmp_path):
    """The engine names the held orders by venue order id; a row without one is not matched.

    ``RunContext.held_execution_order_ids`` is the documented read: a hold
    naming no order, over a row without a venue order id, adds nothing.
    """
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-key", {1: '0.005'})
    __test_helper_held_entry(plugin, order_id=None, hold_order_ids=[])
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert leg.qty == pytest.approx(0.002)
    plugin.store_ctx.close()


def __test_hedge_order_a_held_close_took_still_counts__(tmp_path):
    """An order whose execution the engine's held-execution close took counts until it retires.

    The hold itself is gone; the stored close names the order it took the
    execution of (``held_execution_order_ids``).
    """
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-close", {1: '0.005'})
    __test_helper_held_entry(plugin)
    plugin.store_ctx.replace_keyed_events(ENTRY_FILL_EVIDENCE_HOLD_EVENT_KIND, {},
                                          removed=['L2'])
    plugin.store_ctx.replace_keyed_events(HELD_EXECUTION_CLOSE_EVENT_KIND, {
        'close': {'pine_ids': [__test_helper_HELD_PINE_ID], 'order_ids': ['702'],
                  'hold_keys': ['L2'], 'request': 'close', 'venue_signed': 0.005,
                  'qty': 0.005, 'sent_ms': 0},
    })
    (leg,) = asyncio.run(plugin.fetch_raw_positions('BTCUSDT'))
    assert leg.qty == pytest.approx(0.005)
    plugin.store_ctx.close()
