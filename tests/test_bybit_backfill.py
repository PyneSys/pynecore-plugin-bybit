"""
@pyne
"""
import asyncio
import time
from decimal import Decimal

from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, BybitConnectionError
from pynecore_bybit.helpers import EXECUTION_WINDOW_MS
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
    to its ``[startTime, endTime]`` span (newest-first) and enforces the
    endpoint's real 7-day span cap — an over-wide window raises the measured
    ``retCode 10001`` so a walk that respected the cap is proven. ``fail_once``
    makes the first execution read raise a transport error (the mid-window
    failure path). Every execution request's ``(startTime, endTime, cursor)``
    is recorded in ``exec_calls``.
    """

    def __init__(self, *, market: InstrumentInfo, executions=None,
                 fail_once=False):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self.executions = list(executions or [])
        self.fail_once = fail_once
        self.exec_calls: list = []
        self._exec_failures = 0

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        params = dict(params or {})
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
            rows = [
                row for row in self.executions
                if start <= int(row.get('execTime') or 0) <= end
            ]
            rows.sort(key=lambda r: int(r.get('execTime') or 0), reverse=True)
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
