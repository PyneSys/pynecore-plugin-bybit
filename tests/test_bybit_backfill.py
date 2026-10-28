"""
@pyne
"""
import asyncio
import logging
import time
from decimal import Decimal

from pynecore.core.broker import storage
from pynecore.core.broker.disappearance import MISSING_PENDING_EXTRA, MissingResolution
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_OCA,
    CANCEL_REASON_VENUE_POSITION_CLEARED,
    CANCEL_REASON_VENUE_REDUCE_ONLY,
    CANCEL_REASON_VENUE_REJECTED,
    LegType,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION, ENTRY_KIND_WORKING

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, BybitConnectionError
from pynecore_bybit.helpers import (
    ENTRY_CANCEL_CUM_EXEC_EXTRA,
    EXECUTION_WINDOW_MS,
    venue_filled_qty,
)
from pynecore_bybit.models import InstrumentInfo


def main():
    """Dummy main to make this a valid Pyne script."""
    pass


# === Instrument builders ===================================================

def _linear_instrument(**overrides) -> InstrumentInfo:
    values = dict(
        category='linear', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='USDT', status='Trading', tick_size_str='0.10', tick_size=0.1,
        qty_step_str='0.001', qty_step=0.001, min_order_qty=0.001, min_order_amt=0.0,
        min_notional=5.0, max_limit_order_qty=1500.0, max_market_order_qty=150.0,
        contract_type='LinearPerpetual', delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


def _inverse_instrument(**overrides) -> InstrumentInfo:
    values = dict(
        category='inverse', symbol='BTCUSD', base_coin='BTC', quote_coin='USD',
        settle_coin='BTC', status='Trading', tick_size_str='0.50', tick_size=0.5,
        qty_step_str='1', qty_step=1.0, min_order_qty=1.0, min_order_amt=0.0,
        min_notional=1.0, max_limit_order_qty=1_000_000.0,
        max_market_order_qty=1_000_000.0, contract_type='InversePerpetual',
        delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


# === Fake broker (endpoint-routed, window-aware execution/list) ============

class _BackfillFake(Bybit):
    """Bybit with the REST dispatcher replaced by a window-aware fake.

    ``executions`` are ``/v5/execution/list`` rows; each request filters them
    to its ``[startTime, endTime]`` span (newest-first), its ``orderId`` and
    its ``execType`` when the request names them, and enforces the
    endpoint's real 7-day span cap — an over-wide window raises the measured
    ``retCode 10001`` so a walk that respected the cap is proven. ``fail_once``
    makes the first execution read raise a transport error (the mid-window
    failure path). Every execution request's ``(startTime, endTime, cursor)``
    is recorded in ``exec_calls``; every order-create body in ``created``.
    """

    def __init__(self, *, market: InstrumentInfo, executions=None,
                 fail_once=False):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self.executions = list(executions or [])
        self.fail_once = fail_once
        self.exec_calls: list = []
        self._exec_failures = 0
        self.orders: list[dict] = []
        self.lookups: list[str] = []
        self.created: list[dict] = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        params = dict(params or {})
        if endpoint == '/v5/order/create':
            self.created.append(dict(body or {}))
            return {'orderId': f"x-{(body or {})['orderLinkId']}"}
        if endpoint == '/v5/execution/list':
            start = int(params.get('startTime') or 0)
            end = int(params.get('endTime') or 0)
            self.exec_calls.append((start, end, params.get('cursor')))
            if self.fail_once and self._exec_failures == 0:
                self._exec_failures += 1
                raise BybitConnectionError("execution list down")
            if end - start > EXECUTION_WINDOW_MS:
                raise BybitAPIError(
                    "startTime/endTime exceeds 7 days", ret_code=10001,
                )
            order_id = params.get('orderId')
            exec_type = params.get('execType')
            rows = [
                row for row in self.executions
                if start <= int(row.get('execTime') or 0) <= end
                and (order_id is None or row.get('orderId') == order_id)
                and (exec_type is None or row.get('execType', 'Trade') == exec_type)
            ]
            rows.sort(key=lambda r: int(r.get('execTime') or 0), reverse=True)
            return {'list': rows, 'nextPageCursor': ''}
        if endpoint in ('/v5/order/realtime', '/v5/order/history'):
            live = endpoint == '/v5/order/realtime'
            link_id = params.get('orderLinkId')
            if link_id is not None and live:
                self.lookups.append(link_id)
            rows = [
                row for row in self.orders
                if (row['orderStatus'] in ('New', 'PartiallyFilled')) == live
                and (link_id is None or row['orderLinkId'] == link_id)
            ]
            return {'list': rows, 'nextPageCursor': ''}
        raise AssertionError(f"unexpected REST endpoint {endpoint} {params}")


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="bf", symbol=broker._market.symbol, timeframe="1",
        account_id="bf-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// bf")


def _exec(*, exec_id, order_id, coid, qty, price, ts_ms, symbol) -> dict:
    return {
        'execId': exec_id, 'orderId': order_id, 'orderLinkId': coid,
        'execType': 'Trade', 'execQty': str(qty), 'execPrice': str(price),
        'execFee': '0', 'execTime': str(ts_ms), 'side': 'Buy', 'symbol': symbol,
    }


def _seed(broker, coid, *, qty, filled=None, extras=None) -> None:
    fields = dict(
        symbol=broker._market.symbol, side='buy', qty=qty, state='confirmed',
        intent_key=coid, pine_entry_id='Long', exchange_order_id=f"x-{coid}",
        extras={'kind': ENTRY_KIND_POSITION, 'order_type': 'market', **(extras or {})},
    )
    broker.store_ctx.upsert_order(coid, **fields)
    broker.store_ctx.add_ref(coid, 'order_id', f"x-{coid}")
    if filled is not None:
        broker.store_ctx.set_filled(coid, filled)


def _backfill(broker) -> list:
    return asyncio.run(broker._run_deriv_fill_backfill(broker._market))


# === Missed fill recovered after a reconnect (emitted once) ================

def __test_missed_fill_recovered_once__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e1', order_id='x-c1', coid='c1', qty=0.002,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c1', qty=0.01)
    broker._deriv_exec_watermark = now - 120_000
    first = _backfill(broker)
    assert len(first) == 1
    assert first[0].event_type == 'partial'
    assert first[0].fill_id == 'e1'
    assert abs(first[0].fill_qty - 0.002) < 1e-12
    assert broker.store_ctx.get_order('c1').filled_qty == 0.002
    # A second pass re-reads the overlap window but the execId frontier
    # suppresses the already-delivered fill — no double emit.
    second = _backfill(broker)
    assert second == []


# === execId dedup: WS already delivered the fill ============================

def __test_execid_dedup_skips_ws_delivered_fill__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e2', order_id='x-c2', coid='c2', qty=0.002,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c2', qty=0.01)
    broker._deriv_exec_watermark = now - 120_000
    broker._seen_exec_ids.add('e2')  # the live PUSH path already booked it
    assert _backfill(broker) == []


# === Cursor advances only on a full drain ==================================

def __test_cursor_unmoved_on_transport_failure_then_retries__(tmp_path):
    now = int(time.time() * 1000)
    start = now - 120_000
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e3', order_id='x-c3', coid='c3', qty=0.002,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
        fail_once=True,
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c3', qty=0.01)
    broker._deriv_exec_watermark = start
    # First pass: the window read fails mid-drain -> inconclusive, so the
    # watermark must not move and no fill is booked.
    assert _backfill(broker) == []
    assert broker._deriv_exec_watermark == start
    # Second pass: the read succeeds, the same window is re-walked and the
    # fill is recovered; the watermark now advances.
    recovered = _backfill(broker)
    assert len(recovered) == 1
    assert recovered[0].fill_id == 'e3'
    assert broker._deriv_exec_watermark > start


# === 7-day window walk over a long gap =====================================

def __test_long_gap_walks_multiple_windows_without_10001__(tmp_path):
    now = int(time.time() * 1000)
    day = 86_400_000
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e4', order_id='x-c4', coid='c4', qty=0.002,
                          price=30000, ts_ms=now - 2 * day, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c4', qty=0.01)
    broker._deriv_exec_watermark = now - 20 * day
    recovered = _backfill(broker)
    # No request ever exceeded the 7-day cap (the fake would have raised
    # 10001), the gap was walked in several windows, and the fill in the
    # last window was recovered.
    assert len(recovered) == 1
    assert recovered[0].fill_id == 'e4'
    distinct_windows = {(s, e) for s, e, _c in broker.exec_calls}
    assert len(distinct_windows) >= 3
    for start, end, _cursor in broker.exec_calls:
        assert end - start <= EXECUTION_WINDOW_MS


# === Inverse anchor-correct conversion =====================================

def __test_inverse_fill_converts_at_pinned_anchor__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_inverse_instrument(),
        executions=[_exec(exec_id='e5', order_id='x-c5', coid='c5', qty=200,
                          price=31000, ts_ms=now - 30_000, symbol='BTCUSD')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c5', qty=500.0, extras={'anchor': '30000'})
    broker._deriv_exec_watermark = now - 120_000
    recovered = _backfill(broker)
    assert len(recovered) == 1
    # Wire fill is 200 contracts; the core-facing base quantity converts at
    # the PINNED dispatch anchor (30000), NOT the execution price (31000).
    assert abs(recovered[0].fill_qty - 200 / 30000) < 1e-12
    assert broker._wire_anchor['c5'] == Decimal('30000')


# === Foreign execution skipped =============================================

def __test_foreign_execution_skipped__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e6', order_id='o-foreign', coid='foreign',
                          qty=0.002, price=30000, ts_ms=now - 30_000,
                          symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c6', qty=0.01)  # a live row, but NOT the foreign coid
    broker._deriv_exec_watermark = now - 120_000
    assert _backfill(broker) == []
    # The foreign id is dedup-seeded so the overlap re-read does not re-log it.
    assert 'e6' in broker._seen_exec_ids


# === Cursor-vs-filled_qty diff no-op when the baseline already covers =======

def __test_no_op_when_filled_cursor_already_covers__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e7', order_id='x-c7', coid='c7', qty=0.01,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    # The F2 adoption baseline already raised the row's wire cursor to its
    # full size — re-booking this pre-restart slice would double-count.
    _seed(broker, 'c7', qty=0.01, filled=0.01)
    broker._deriv_exec_watermark = now - 120_000
    assert _backfill(broker) == []
    assert 'e7' in broker._seen_exec_ids
    assert broker.store_ctx.get_order('c7').filled_qty == 0.01


# === Filled MARKET park close-out ==========================================

def __test_backfilled_fill_clears_engine_park__(tmp_path):
    # A MARKET entry that parked on an unknown-disposition response and then
    # filled never re-enters get_open_orders; the recovered fill must drop
    # its lingering engine park (pending_verifications row).
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e9', order_id='x-c9', coid='c9', qty=0.01,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c9', qty=0.01)
    broker.store_ctx.record_park('c9', 'c9')
    assert 'c9' in broker.store_ctx.replay()[1]
    broker._deriv_exec_watermark = now - 120_000
    recovered = _backfill(broker)
    assert len(recovered) == 1
    assert 'c9' not in broker.store_ctx.replay()[1]


# === Adoption gate: backfill deferred until the baseline has run ============

def __test_backfill_deferred_until_adoption_baselined__(tmp_path):
    now = int(time.time() * 1000)
    broker = _BackfillFake(
        market=_linear_instrument(),
        executions=[_exec(exec_id='e8', order_id='x-c8', coid='c8', qty=0.002,
                          price=30000, ts_ms=now - 30_000, symbol='BTCUSDT')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c8', qty=0.01)
    broker._deriv_exec_watermark = now - 120_000
    # Adoption baseline not yet applied -> the backfill is a no-op (F2 before
    # F4) and touches neither the watermark nor the execution endpoint.
    assert _backfill(broker) == []
    assert broker.exec_calls == []


# === Transient read failures log one line, not a traceback ================

def _records_containing(caplog, needle: str) -> list:
    return [r for r in caplog.records if needle in r.getMessage()]


def __test_transient_window_read_failure_is_one_warning_line__(tmp_path, caplog):
    # The execution/list read drops mid-drain: the pass is inconclusive and
    # the log is ONE WARNING naming the transport error — no HTTP-stack
    # traceback (the runner counts every traceback line as an error).
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument(), fail_once=True)
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    broker._deriv_exec_watermark = now - 120_000
    with caplog.at_level(logging.WARNING, logger='pynecore_bybit'):
        assert _backfill(broker) == []
    records = _records_containing(caplog, 'execution/list read failed')
    assert len(records) == 1
    assert 'execution list down' in records[0].getMessage()
    assert not records[0].exc_info


def __test_transient_backfill_pass_failure_is_one_warning_line__(tmp_path, caplog,
                                                                  monkeypatch):
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    broker._deriv_exec_watermark = now - 120_000

    async def _drain_down(*_args):
        raise BybitConnectionError("drain down")

    monkeypatch.setattr(broker, '_drain_deriv_exec_window', _drain_down)
    with caplog.at_level(logging.WARNING, logger='pynecore_bybit'):
        assert _backfill(broker) == []
    records = _records_containing(caplog, 'fill backfill failed')
    assert len(records) == 1
    assert 'drain down' in records[0].getMessage()
    assert not records[0].exc_info


def __test_unexpected_backfill_pass_failure_keeps_the_traceback__(tmp_path, caplog,
                                                                   monkeypatch):
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    broker._deriv_exec_watermark = now - 120_000

    async def _drain_broken(*_args):
        raise RuntimeError("boom")

    monkeypatch.setattr(broker, '_drain_deriv_exec_window', _drain_broken)
    with caplog.at_level(logging.WARNING, logger='pynecore_bybit'):
        assert _backfill(broker) == []
    records = _records_containing(caplog, 'fill backfill failed')
    assert len(records) == 1
    assert records[0].exc_info


class _SpotManagerDown:
    """Spot inventory manager whose reconcile pass raises ``exc``."""

    def __init__(self, exc: Exception):
        self.exc = exc

    async def reconcile(self, _now_ms: int) -> list:
        raise self.exc


def __test_transient_spot_reconcile_failure_is_one_warning_line__(tmp_path, caplog,
                                                                   monkeypatch):
    market = _linear_instrument()
    broker = _BackfillFake(market=market)
    _open(tmp_path, broker)
    monkeypatch.setattr(broker, '_spot_manager',
                        _SpotManagerDown(BybitConnectionError("wallet down")))
    with caplog.at_level(logging.WARNING, logger='pynecore_bybit'):
        assert asyncio.run(broker._run_spot_reconcile(market)) == []
    records = _records_containing(caplog, 'spot inventory reconcile pass failed')
    assert len(records) == 1
    assert 'wallet down' in records[0].getMessage()
    assert not records[0].exc_info


def __test_unexpected_spot_reconcile_failure_keeps_the_traceback__(tmp_path, caplog,
                                                                    monkeypatch):
    market = _linear_instrument()
    broker = _BackfillFake(market=market)
    _open(tmp_path, broker)
    monkeypatch.setattr(broker, '_spot_manager', _SpotManagerDown(RuntimeError("boom")))
    with caplog.at_level(logging.WARNING, logger='pynecore_bybit'):
        assert asyncio.run(broker._run_spot_reconcile(market)) == []
    records = _records_containing(caplog, 'spot inventory reconcile pass failed')
    assert len(records) == 1
    assert records[0].exc_info


# === Late-indexed execution: recovered per order, never by the window ======

def _seed_created_at(monkeypatch, broker, coid, created_ms: int, **kwargs) -> None:
    """Seed a row whose dispatch lies ``created_ms`` in the past."""
    with monkeypatch.context() as patch:
        patch.setattr(storage, '_now_ms', lambda: created_ms)
        _seed(broker, coid, **kwargs)


def _venue_order(coid: str, *, status: str, cum_exec: str) -> dict:
    return {'orderId': f"x-{coid}", 'orderLinkId': coid, 'orderStatus': status,
            'cumExecQty': cum_exec}


def __test_fill_indexed_after_the_window_overlap_is_recovered_per_order__(
        tmp_path, monkeypatch):
    """A fill the venue indexes later than the read overlap is still booked.

    The windowed backfill re-reads only ``EXECUTION_CURSOR_OVERLAP_MS`` behind
    its watermark: an execution the stream dropped and the venue indexes later
    than that is never returned by it. The vanished order's ``cumExecQty`` is
    the proof the execution exists; the per-order read by ``orderId`` books it
    once, whatever the watermark says.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'c1', now - 600_000, qty=0.01)
    broker._deriv_exec_watermark = now - 120_000
    assert _backfill(broker) == []

    # The venue indexes the execution only now, five minutes after it ran.
    broker.executions.append(_exec(exec_id='e1', order_id='x-c1', coid='c1',
                                   qty=0.01, price=30000, ts_ms=now - 300_000,
                                   symbol='BTCUSDT'))
    assert _backfill(broker) == [], "the window cannot reach the late execution"

    broker.orders.append(_venue_order('c1', status='Filled', cum_exec='0.01'))
    row = broker.store_ctx.get_order('c1')
    verdict = asyncio.run(broker._confirm_vanished_order(broker._market, row))
    assert verdict.resolution is MissingResolution.FILLED
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['e1']
    assert events[0].event_type == 'filled'
    assert broker.store_ctx.get_order('c1').filled_qty == 0.01
    assert broker._unbooked_fill_orders == {}
    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []


def __test_vanished_order_verdict_carries_the_venue_cancel_type__(
        tmp_path, monkeypatch):
    """A dead order's ``cancelType`` tells a venue cancel from an operator's.

    The push of a venue-driven cancel can be lost like any other; the history
    read at grace expiry then names the cause, so the tracker retires the leg
    without quarantining the run. A plain user cancel stays unexplained; a
    rejected order is always the venue's verdict.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    cases = {
        'v1': ('Deactivated', 'CancelByReduceOnly', CANCEL_REASON_VENUE_REDUCE_ONLY),
        'v2': ('Deactivated', 'CancelByOCOSlCanceledByTpTriggered', CANCEL_REASON_VENUE_OCA),
        'v3': ('Deactivated', 'CancelByTpSlTsClear', CANCEL_REASON_VENUE_POSITION_CLEARED),
        'v4': ('Cancelled', 'CancelByUser', None),
        'v5': ('Rejected', 'UNKNOWN', CANCEL_REASON_VENUE_REJECTED),
        'v6': ('Cancelled', 'CancelByPrepareLiq', CANCEL_REASON_VENUE_POSITION_CLEARED),
        'v7': ('Deactivated', 'CancelByCannotAffordOrderCost', CANCEL_REASON_VENUE_REJECTED),
        'v8': ('Cancelled', 'CancelBySmp', None),
    }
    for coid, (status, cancel_type, _reason) in cases.items():
        _seed_created_at(monkeypatch, broker, coid, now - 600_000, qty=0.01)
        broker.orders.append({**_venue_order(coid, status=status, cum_exec='0'),
                              'cancelType': cancel_type})
    for coid, (_status, _cancel_type, reason) in cases.items():
        row = broker.store_ctx.get_order(coid)
        verdict = asyncio.run(broker._confirm_vanished_order(broker._market, row))
        assert verdict.resolution is MissingResolution.CANCELLED
        assert verdict.cancel_reason == reason, coid


def __test_backfill_books_same_millisecond_executions_in_seq_order__(tmp_path):
    """Executions of one millisecond are booked in the venue's ``seq`` order.

    The execution list serves them newest first. Booking the own exit before
    the entry it closes would net nothing (the entry row owns nothing yet)
    and leave the run owning a position the venue already closed.
    """
    now = int(time.time() * 1000)
    ts_ms = now - 30_000
    entry = {**_exec(exec_id='e-in', order_id='x-c1', coid='c1', qty=0.01,
                     price=30000, ts_ms=ts_ms, symbol='BTCUSDT'), 'seq': '101'}
    close = {**_exec(exec_id='e-out', order_id='x-x1', coid='x1', qty=0.01,
                     price=30100, ts_ms=ts_ms, symbol='BTCUSDT'),
             'side': 'Sell', 'closedSize': '0.01', 'seq': '102'}
    broker = _BackfillFake(market=_linear_instrument(), executions=[close, entry])
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed(broker, 'c1', qty=0.01)
    broker.store_ctx.upsert_order(
        'x1', symbol='BTCUSDT', side='sell', qty=0.01, state='confirmed',
        intent_key='X\x00Long', from_entry='Long', exchange_order_id='x-x1',
        extras={'kind': 'exit_leg', 'leg': 'tp', 'exit_id': 'X'},
    )
    broker.store_ctx.add_ref('x1', 'order_id', 'x-x1')
    broker._deriv_exec_watermark = now - 120_000
    events = _backfill(broker)
    assert [event.fill_id for event in events] == ['e-in', 'e-out']
    assert broker._run_owned_signed_size() == 0.0


def __test_backfilled_venue_exec_types_enter_the_ledger__(tmp_path):
    """A venue execution only the windowed backfill delivers proves its interval.

    The run's long is closed by the venue (ADL, takeover liquidation,
    settlement) while the stream is down; the flat position view arrives,
    the execution comes from the backfill. The ledger books the venue truth
    at once, without waiting out the give-up grace, and never reports it.
    """
    now = int(time.time() * 1000)
    for exec_type in ('AdlTrade', 'BustTrade', 'Settle'):
        venue_close = {
            **_exec(exec_id=f"v-{exec_type}", order_id=f"venue-{exec_type}", coid='',
                    qty=0.02, price=30000, ts_ms=now - 30_000, symbol='BTCUSDT'),
            'execType': exec_type, 'side': 'Sell', 'closedSize': '0.02',
            'createType': '', 'seq': '100',
        }
        broker = _BackfillFake(market=_linear_instrument(), executions=[venue_close])
        (tmp_path / exec_type).mkdir()
        _open(tmp_path / exec_type, broker)
        quarantines: list = []
        broker.quarantine_sink = lambda reason, ctx, *, kind: quarantines.append(kind)
        broker._adoption_baselined = True
        _seed(broker, 'c1', qty=0.02, filled=0.02)
        view = {'symbol': 'BTCUSDT', 'category': 'linear', 'positionIdx': 0,
                'side': 'Buy', 'size': '0.02', 'updatedTime': str(now - 60_000),
                'seq': '50'}
        broker._ingest_position_sizes([view])
        broker._ledger_advance(broker._market)
        broker._ingest_position_sizes([{**view, 'side': '', 'size': '0', 'seq': '100'}])
        broker._ledger_advance(broker._market)
        assert broker._leg_ledgers[0].open, exec_type
        broker._deriv_exec_watermark = now - 120_000
        assert _backfill(broker) == [], exec_type
        assert not broker._leg_ledgers[0].open, exec_type
        assert quarantines == [], exec_type
        clamped = [payload for _, _, _, payload in
                   broker.store_ctx.iter_events_by_kind_for_run_id('own_share_clamped')]
        assert len(clamped) == 1, exec_type
        assert broker._run_owned_signed_size() == 0.0, exec_type


def __test_filled_order_finished_below_its_row_size_is_settled__(tmp_path, monkeypatch):
    """A venue order Filled with every fill booked settles a row it left short.

    Measured live (bybit-inverse cycle 225): the venue finished a 165-contract
    close at 164; the row, judged against 165, stayed live and every pass
    re-confirmed it as filled. A close row closes; an entry row keeps its
    exposure and drops the remainder that can no longer fill; a row still
    missing a fill is left to the per-order recovery.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed_created_at(monkeypatch, broker, 'k1', now - 600_000, qty=0.0165,
                     filled=0.0164, extras={'kind': 'close'})
    _seed_created_at(monkeypatch, broker, 'k2', now - 600_000, qty=0.0165,
                     filled=0.0164)
    _seed_created_at(monkeypatch, broker, 'k3', now - 600_000, qty=0.0165,
                     filled=0.0100, extras={'kind': 'close'})
    for coid in ('k1', 'k2', 'k3'):
        broker.orders.append(_venue_order(coid, status='Filled', cum_exec='0.0164'))

    for coid in ('k1', 'k2', 'k3'):
        row = broker.store_ctx.get_order(coid)
        verdict = asyncio.run(broker._confirm_vanished_order(broker._market, row))
        assert verdict.resolution is MissingResolution.FILLED

    assert broker.store_ctx.get_order('k1').closed_ts_ms is not None
    entry = broker.store_ctx.get_order('k2')
    assert entry.closed_ts_ms is None and entry.qty == 0.0164
    short = broker.store_ctx.get_order('k3')
    assert short.closed_ts_ms is None and short.qty == 0.0165


def __test_hedge_close_leg_fill_indexed_late_is_recovered__(tmp_path, monkeypatch):
    """A close leg's fill the stream dropped and the window missed is still booked.

    Measured live (bybit cycle 335): a reversal's close leg filled while the
    private stream was down and the execution list did not show it within the
    backfill's read overlap. The leg's own close row is what the vanished-order
    confirmation and the per-order read work from; until the fill is booked the
    engine's external-flatten verdict is held.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    broker.store_ctx.upsert_order(
        'p0-c0:2', symbol='BTCUSDT.P', side='buy', qty=10.0, state='close_leg',
        intent_key='__pyne_reversal_close__L', pine_entry_id='__pyne_reversal_close__L',
    )
    with monkeypatch.context() as patch:
        patch.setattr(storage, '_now_ms', lambda: now - 600_000)
        asyncio.run(broker.close_leg('BTCUSDT', '2', 10, 'p0-c0:2'))
    assert [body['orderLinkId'] for body in broker.created] == ['p0-c0_2']
    row = broker.store_ctx.get_order('p0-c0_2')
    assert row is not None and row.closed_ts_ms is None
    assert (row.extras or {}).get('kind') == 'close'
    assert row.exchange_order_id == 'x-p0-c0_2'
    assert row.intent_key == '__pyne_reversal_close__L'
    assert not broker.fill_recovery_pending({'S'})

    broker._deriv_exec_watermark = now - 120_000
    broker.executions.append(_exec(exec_id='el', order_id='x-p0-c0_2', coid='p0-c0_2',
                                   qty=0.01, price=30000, ts_ms=now - 599_000,
                                   symbol='BTCUSDT'))
    assert _backfill(broker) == [], "the window cannot reach the late execution"

    broker.store_ctx.upsert_order(
        'p0-c0_2', extras={**(row.extras or {}), MISSING_PENDING_EXTRA: now / 1000})
    assert broker.fill_recovery_pending({'S'}), "a stamped close of the run holds"
    assert not broker.fill_recovery_pending(set())

    broker.orders.append(_venue_order('p0-c0_2', status='Filled', cum_exec='0.01'))
    verdict = asyncio.run(broker._confirm_vanished_order(
        broker._market, broker.store_ctx.get_order('p0-c0_2')))
    assert verdict.resolution is MissingResolution.FILLED
    broker.store_ctx.upsert_order('p0-c0_2', extras=row.extras or {})
    assert broker.fill_recovery_pending({'S'}), "venue proof of an unbooked fill holds"

    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [(e.fill_id, e.event_type, e.leg_type) for e in events] == \
           [('el', 'filled', LegType.CLOSE)]
    assert broker.store_ctx.get_order('p0-c0_2').closed_ts_ms is not None
    assert broker._unbooked_fill_orders == {}
    assert not broker.fill_recovery_pending({'S'})


def __test_unindexed_execution_stays_queued_until_the_venue_lists_it__(
        tmp_path, monkeypatch):
    """Proof of a fill the execution list does not show yet is kept, not dropped."""
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'c2', now - 600_000, qty=0.01)
    broker.orders.append(_venue_order('c2', status='Filled', cum_exec='0.01'))
    row = broker.store_ctx.get_order('c2')
    asyncio.run(broker._confirm_vanished_order(broker._market, row))

    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []
    assert 'c2' in broker._unbooked_fill_orders

    broker.executions.append(_exec(exec_id='e2', order_id='x-c2', coid='c2',
                                   qty=0.01, price=30000, ts_ms=now - 200_000,
                                   symbol='BTCUSDT'))
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['e2']
    assert broker._unbooked_fill_orders == {}


def __test_resting_order_partial_fill_is_recovered_from_the_open_orders_snapshot__(
        tmp_path, monkeypatch):
    """A partially filled resting order never vanishes; its snapshot is the proof."""
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'c3', now - 600_000, qty=0.01)
    broker.executions.append(_exec(exec_id='e3', order_id='x-c3', coid='c3',
                                   qty=0.004, price=30000, ts_ms=now - 300_000,
                                   symbol='BTCUSDT'))
    broker.orders.append(_venue_order('c3', status='PartiallyFilled', cum_exec='0.004'))

    present = asyncio.run(broker._disappearance_present(broker._market, []))
    assert present['orders'] == {'x-c3'}
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['e3']
    assert events[0].event_type == 'partial'
    assert broker.store_ctx.get_order('c3').filled_qty == 0.004
    assert broker._unbooked_fill_orders == {}

    # The WS already delivered the fill: nothing is queued, nothing is read.
    calls = len(broker.exec_calls)
    asyncio.run(broker._disappearance_present(broker._market, []))
    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []
    assert len(broker.exec_calls) == calls


def __test_per_order_recovery_waits_for_the_adoption_baseline__(tmp_path, monkeypatch):
    """No per-order fill escapes before the adoption baseline has latched.

    The baseline may be folding the very same execution into the adopted
    position; the queue is kept and drained once the barrier lifts.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed_created_at(monkeypatch, broker, 'c4', now - 600_000, qty=0.01)
    broker.executions.append(_exec(exec_id='e4', order_id='x-c4', coid='c4',
                                   qty=0.01, price=30000, ts_ms=now - 300_000,
                                   symbol='BTCUSDT'))
    broker.orders.append(_venue_order('c4', status='Filled', cum_exec='0.01'))
    row = broker.store_ctx.get_order('c4')
    asyncio.run(broker._confirm_vanished_order(broker._market, row))

    calls = len(broker.exec_calls)
    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []
    assert len(broker.exec_calls) == calls
    assert 'c4' in broker._unbooked_fill_orders

    broker._adoption_baselined = True
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['e4']
    assert broker._unbooked_fill_orders == {}


def __test_market_entry_fill_indexed_late_is_recovered__(tmp_path, monkeypatch):
    """A market entry never tracked by disappearance still gets its late fill.

    It leaves the open set at once, so only its terminal order truth, read
    once by ``orderLinkId`` after the grace, can prove the fill.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'm1', now - 600_000, qty=0.01)
    broker._deriv_exec_watermark = now - 120_000
    broker.executions.append(_exec(exec_id='em1', order_id='x-m1', coid='m1',
                                   qty=0.01, price=30000, ts_ms=now - 300_000,
                                   symbol='BTCUSDT'))
    assert _backfill(broker) == [], "the window cannot reach the late execution"
    broker.orders.append(_venue_order('m1', status='Filled', cum_exec='0.01'))

    asyncio.run(broker._note_unbooked_market_entries(broker._market))
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['em1']
    assert events[0].event_type == 'filled'
    assert broker.store_ctx.get_order('m1').filled_qty == 0.01
    assert broker._unbooked_fill_orders == {}


def __test_prior_instance_order_under_the_same_id_books_no_fill__(tmp_path, monkeypatch):
    """A venue order a prior instance created under the row's id is not the row's.

    Measured live (bybit-inverse cycle 225): a replay rebuilt a coid a run six
    hours earlier had spent; the order truth read by ``orderLinkId`` found that
    run's filled order and its execution booked as the new entry's fill.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'm5', now - 600_000, qty=0.01)
    broker.executions.append(_exec(exec_id='em5', order_id='x-m5', coid='m5',
                                   qty=0.01, price=30000, ts_ms=now - 25_200_000,
                                   symbol='BTCUSDT'))
    broker.orders.append({**_venue_order('m5', status='Filled', cum_exec='0.01'),
                          'createdTime': str(now - 25_200_000)})

    asyncio.run(broker._note_unbooked_market_entries(broker._market))
    assert broker._unbooked_fill_orders == {}
    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []
    assert broker.store_ctx.get_order('m5').filled_qty == 0.0


def __test_market_entry_order_truth_is_read_once__(tmp_path, monkeypatch):
    """A residual-cancelled market entry is looked up once, a fresh one not yet."""
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'm2', now - 600_000, qty=0.01, filled=0.004)
    _seed(broker, 'm3', qty=0.01)
    broker.orders.append(_venue_order('m2', status='PartiallyFilledCanceled',
                                      cum_exec='0.004'))

    asyncio.run(broker._note_unbooked_market_entries(broker._market))
    asyncio.run(broker._note_unbooked_market_entries(broker._market))
    assert broker.lookups == ['m2']
    assert broker._unbooked_fill_orders == {}


def __test_netted_working_entry_still_books_its_later_executions__(
        tmp_path, monkeypatch):
    """A reducing fill on a partially filled working entry keeps its cursor whole.

    Netting lowers the owned exposure but keeps the working remainder, so a
    later execution of the still-resting order is booked, and the venue
    cursor (owned + netted) stays comparable with its ``cumExecQty``.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_created_at(monkeypatch, broker, 'w1', now - 600_000, qty=0.01, filled=0.004,
                     extras={'kind': ENTRY_KIND_WORKING, 'order_type': 'limit'})
    broker._seen_exec_ids.add('ew1a')
    broker._reduce_entry_ownership('sell', 0.002)
    row = broker.store_ctx.get_order('w1')
    assert row.closed_ts_ms is None
    assert abs(row.qty - 0.008) < 1e-12
    assert abs(row.filled_qty - 0.002) < 1e-12
    assert abs(venue_filled_qty(row) - 0.004) < 1e-12

    broker.executions.append(_exec(exec_id='ew1a', order_id='x-w1', coid='w1',
                                   qty=0.004, price=30000, ts_ms=now - 500_000,
                                   symbol='BTCUSDT'))
    broker.executions.append(_exec(exec_id='ew1b', order_id='x-w1', coid='w1',
                                   qty=0.003, price=30000, ts_ms=now - 300_000,
                                   symbol='BTCUSDT'))
    broker.orders.append(_venue_order('w1', status='PartiallyFilled', cum_exec='0.007'))
    asyncio.run(broker._disappearance_present(broker._market, []))
    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['ew1b']
    assert events[0].event_type == 'partial'
    row = broker.store_ctx.get_order('w1')
    assert abs(row.filled_qty - 0.005) < 1e-12
    assert abs(venue_filled_qty(row) - 0.007) < 1e-12
    assert broker._unbooked_fill_orders == {}

    calls = len(broker.exec_calls)
    asyncio.run(broker._disappearance_present(broker._market, []))
    assert asyncio.run(broker._recover_unbooked_order_fills(broker._market)) == []
    assert len(broker.exec_calls) == calls


# === Remainder-cancelled entries: late fills and the cancel settlement =====

def _seed_cancelled_working_entry(monkeypatch, broker, coid, *, created_ms: int,
                                  filled: float) -> None:
    """Seed a 0.01 working LIMIT entry and apply its REST cancel ACK."""
    _seed_created_at(monkeypatch, broker, coid, created_ms, qty=0.01, filled=filled,
                     extras={'kind': ENTRY_KIND_WORKING, 'order_type': 'limit'})
    broker._retire_cancelled_row(coid)


def __test_cancel_ack_leaves_room_for_a_late_fill_in_the_windowed_backfill__(
        tmp_path, monkeypatch):
    """An execution undelivered at the cancel ACK is still booked by the backfill.

    The ACK carries no fill figure, so the row keeps its size: the backfill's
    full-row barrier must not treat the booked cursor as complete and swallow
    the missed execution.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_cancelled_working_entry(monkeypatch, broker, 'k1',
                                  created_ms=now - 600_000, filled=0.004)
    broker._seen_exec_ids.add('ek1a')
    broker._deriv_exec_watermark = now - 120_000
    broker.executions.append(_exec(exec_id='ek1a', order_id='x-k1', coid='k1',
                                   qty=0.004, price=30000, ts_ms=now - 90_000,
                                   symbol='BTCUSDT'))
    broker.executions.append(_exec(exec_id='ek1b', order_id='x-k1', coid='k1',
                                   qty=0.001, price=30000, ts_ms=now - 60_000,
                                   symbol='BTCUSDT'))
    events = _backfill(broker)
    assert [e.fill_id for e in events] == ['ek1b']
    row = broker.store_ctx.get_order('k1')
    assert row.closed_ts_ms is None
    assert abs(row.filled_qty - 0.005) < 1e-12


def __test_cancel_settlement_reads_the_order_and_queues_its_missing_fill__(
        tmp_path, monkeypatch):
    """A dropped terminal push: the reconcile cadence settles the cancelled entry.

    The order is read by ``orderLinkId`` once; its ``cumExecQty`` sizes the
    row and queues the per-order recovery for the execution the window never
    returns, after which the row is a settled position.
    """
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._adoption_baselined = True
    _seed_cancelled_working_entry(monkeypatch, broker, 'k2',
                                  created_ms=now - 600_000, filled=0.004)
    assert broker._tracked_refs(broker.store_ctx.get_order('k2')) == set()
    broker._seen_exec_ids.add('ek2a')
    broker.executions.append(_exec(exec_id='ek2a', order_id='x-k2', coid='k2',
                                   qty=0.004, price=30000, ts_ms=now - 500_000,
                                   symbol='BTCUSDT'))
    broker.executions.append(_exec(exec_id='ek2b', order_id='x-k2', coid='k2',
                                   qty=0.002, price=30000, ts_ms=now - 400_000,
                                   symbol='BTCUSDT'))
    broker.orders.append(_venue_order('k2', status='PartiallyFilledCanceled',
                                      cum_exec='0.006'))

    asyncio.run(broker._settle_cancelled_entries(broker._market))
    row = broker.store_ctx.get_order('k2')
    assert row.closed_ts_ms is None
    assert abs(row.qty - 0.006) < 1e-12
    assert row.extras[ENTRY_CANCEL_CUM_EXEC_EXTRA] == 0.006
    assert broker._unbooked_fill_orders['k2'] == ('x-k2', Decimal('0.006'))

    events = asyncio.run(broker._recover_unbooked_order_fills(broker._market))
    assert [e.fill_id for e in events] == ['ek2b']
    row = broker.store_ctx.get_order('k2')
    assert abs(row.filled_qty - 0.006) < 1e-12
    assert broker._tracked_refs(row) == {('positions', 'BTCUSDT')}

    lookups = len(broker.lookups)
    asyncio.run(broker._settle_cancelled_entries(broker._market))
    assert len(broker.lookups) == lookups


def __test_cancel_settlement_retires_an_entry_that_never_executed__(
        tmp_path, monkeypatch):
    """A zero-fill cancelled entry is closed once the venue confirms no fill."""
    now = int(time.time() * 1000)
    broker = _BackfillFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed_cancelled_working_entry(monkeypatch, broker, 'k3',
                                  created_ms=now - 600_000, filled=0.0)
    assert broker.store_ctx.get_order('k3').closed_ts_ms is None

    broker.orders.append(_venue_order('k3', status='New', cum_exec='0'))
    asyncio.run(broker._settle_cancelled_entries(broker._market))
    assert broker.store_ctx.get_order('k3').closed_ts_ms is None, \
        "a still-live order is re-checked, never concluded"

    broker.orders[0]['orderStatus'] = 'Cancelled'
    asyncio.run(broker._settle_cancelled_entries(broker._market))
    assert broker.store_ctx.get_order('k3').closed_ts_ms is not None
