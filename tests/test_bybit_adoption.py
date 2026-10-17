"""
@pyne
"""
import asyncio
from collections.abc import AsyncGenerator
from typing import cast

import pytest

from pynecore.core.broker.models import QuarantineKind
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAdoptionBaselineError, BybitConnectionError
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_HEDGE


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


# === Fake broker (endpoint-routed REST) ====================================

#: Venue server clock the fake serves on ``/v5/market/time`` (epoch-ms).
_VENUE_TIME_MS = 1_710_000_000_123


class _AdoptionFake(Bybit):
    """Bybit with the REST dispatcher replaced by an endpoint-routed fake.

    ``position_snapshots`` is a list of ``/v5/position/list`` snapshots
    served IN ORDER (the last repeats forever) — a multi-element list
    simulates a fill racing the baseline's stability re-read. ``execs``
    maps a broker ``orderId`` to its ``/v5/execution/list`` rows.
    ``fail_exec_reads`` makes every execution read raise (transport
    failure). Every call is recorded in ``calls``.
    """

    def __init__(self, *, market: InstrumentInfo, positions=None,
                 position_snapshots=None, execs=None, order_lookups=None):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self._position_mode = 'one_way'
        if position_snapshots is None:
            position_snapshots = [list(positions or [])]
        self.position_snapshots = [list(rows) for rows in position_snapshots]
        self.execs = dict(execs or {})
        self.order_lookups = dict(order_lookups or {})
        self.fail_exec_reads = False
        self.calls: list = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        params = dict(params or {})
        self.calls.append((endpoint, params))
        if endpoint == '/v5/position/list':
            if len(self.position_snapshots) > 1:
                return {'list': self.position_snapshots.pop(0)}
            return {'list': list(self.position_snapshots[0])}
        if endpoint == '/v5/market/time':
            return {'timeSecond': str(_VENUE_TIME_MS // 1000),
                    'timeNano': str(_VENUE_TIME_MS * 1_000_000)}
        if endpoint == '/v5/execution/list':
            if self.fail_exec_reads:
                raise BybitConnectionError("injected execution read failure")
            rows = self.execs.get(str(params.get('orderId') or ''), [])
            return {'list': list(rows), 'nextPageCursor': ''}
        if endpoint in ('/v5/order/realtime', '/v5/order/history'):
            entry = self.order_lookups.get(str(params.get('orderLinkId') or ''))
            return {'list': [dict(entry)] if entry else []}
        raise AssertionError(f"unexpected REST endpoint {endpoint} {params}")


def _pos(*, side, size, avg='30000', updated='1700000000000') -> dict:
    return {'symbol': 'BTCUSDT', 'positionIdx': 0, 'size': size,
            'side': side, 'avgPrice': avg, 'updatedTime': updated}


def _inv_pos(*, side, size, avg='30000', updated='1700000000000') -> dict:
    return {'symbol': 'BTCUSD', 'positionIdx': 0, 'size': size,
            'side': side, 'avgPrice': avg, 'updatedTime': updated}


def _exec(exec_id, qty) -> dict:
    return {'execId': exec_id, 'execQty': qty, 'execType': 'Trade'}


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="adopt", symbol=broker._market.symbol, timeframe="1",
        account_id="adopt-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// adopt")


def _seed(broker, coid, *, qty, state='confirmed', filled_qty=0.0, side='buy',
          exchange_order_id='o1', kind=ENTRY_KIND_POSITION, extras=None) -> None:
    fields = dict(
        symbol=broker._market.symbol, side=side, qty=qty, state=state,
        filled_qty=filled_qty, intent_key=coid, pine_entry_id='Long',
        exchange_order_id=exchange_order_id,
        extras={'kind': kind, 'order_type': 'market', **(extras or {})},
    )
    broker.store_ctx.upsert_order(coid, **fields)


def _adopt(broker):
    """Drive the first engine-facing position snapshot (the adoption call)."""
    market = broker._market
    return asyncio.run(broker._fetch_deriv_position(market))


def _exec_list_order_ids(broker) -> list:
    return [params.get('orderId')
            for endpoint, params in broker.calls
            if endpoint == '/v5/execution/list']


# === Baseline seeds cursor + de-dup from per-order venue truth =============

def __test_baseline_seeds_cursor_from_own_executions__(tmp_path):
    # A confirmed market entry fully filled into the adopted net, but its
    # filled_qty cursor never persisted before the crash: the baseline seeds
    # the cursor from the order's OWN execution rows, silently, and seeds
    # the execIds into the de-dup frontier.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.004'), _exec('e2', '0.006')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01, filled_qty=0.0)
    pos = _adopt(broker)
    row = broker.store_ctx.get_order('c1')
    assert row.filled_qty == 0.01           # sum of the order's own execQty
    assert {'e1', 'e2'} <= broker._seen_exec_ids
    assert pos is not None and pos.size == 0.01


def __test_baseline_partial_cursor_is_order_truth_not_qty__(tmp_path):
    # A resting LIMIT entry only partially filled: the cursor lands on the
    # order's own executed quantity, never on the full row qty — the
    # unfilled residual must still be fillable later.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.004')],
        execs={'o1': [_exec('e1', '0.004')]},
        order_lookups={'c2': {'orderId': 'o1',
                              'orderStatus': 'PartiallyFilled',
                              'cumExecQty': '0.004'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c2', qty=0.01, filled_qty=0.0)
    _adopt(broker)
    row = broker.store_ctx.get_order('c2')
    assert row.filled_qty == 0.004          # order truth, residual preserved
    assert row.qty == 0.01                  # a LIVE order is never normalized


def __test_baseline_attributes_per_order_not_fifo__(tmp_path):
    # F5 regression: TWO working orders on the same side; the venue matched
    # the NEWER one (price decides, not creation order). The older row must
    # stay at zero — an aggregate FIFO split would hand it the newer
    # order's fill.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o-new': [_exec('e-new', '0.01')]},
        order_lookups={'old': {'orderId': 'o-old', 'orderStatus': 'New',
                               'cumExecQty': '0'},
                       'new': {'orderId': 'o-new', 'orderStatus': 'Filled',
                               'cumExecQty': '0.01'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'old', qty=0.01, filled_qty=0.0, exchange_order_id='o-old')
    _seed(broker, 'new', qty=0.01, filled_qty=0.0, exchange_order_id='o-new')
    _adopt(broker)
    assert broker.store_ctx.get_order('old').filled_qty == 0.0
    assert broker.store_ctx.get_order('new').filled_qty == 0.01


def __test_baseline_runs_exactly_once__(tmp_path):
    # The guard latches on the FIRST committed snapshot: a later snapshot
    # with new fills (genuine live trading, not restart lag) must NOT
    # re-clamp the cursor or re-read executions.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.004')],
        execs={'o1': [_exec('e1', '0.004')]},
        order_lookups={'c3': {'orderId': 'o1',
                              'orderStatus': 'PartiallyFilled',
                              'cumExecQty': '0.004'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c3', qty=0.02, filled_qty=0.0)
    _adopt(broker)
    assert broker.store_ctx.get_order('c3').filled_qty == 0.004
    assert broker._adoption_baselined is True
    exec_reads = len(_exec_list_order_ids(broker))
    broker.execs['o1'].append(_exec('e2', '0.016'))
    _adopt(broker)
    assert broker.store_ctx.get_order('c3').filled_qty == 0.004
    assert len(_exec_list_order_ids(broker)) == exec_reads


def __test_baseline_exempts_pending_dispatch_row__(tmp_path):
    # A row F1 recovery left parked (still in a pending-dispatch state):
    # recovery owns its resolution, so the baseline must not walk or touch
    # it.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'op1': [_exec('e1', '0.01')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c4', qty=0.01, filled_qty=0.0, state='submitted',
          exchange_order_id='op1')
    _adopt(broker)
    row = broker.store_ctx.get_order('c4')
    assert row.state == 'submitted'         # untouched
    assert row.filled_qty == 0.0            # cursor NOT advanced
    assert 'op1' not in _exec_list_order_ids(broker)


def __test_baseline_covers_exit_rows_too__(tmp_path):
    # A reduce-only exit leg whose pre-restart fill already reduced the
    # adopted net: its cursor must equally reflect venue truth, or a later
    # backfill would re-book (double-reduce) the slice.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'oe1': [_exec('e-exit', '0.005')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'exit1', qty=0.005, filled_qty=0.0, side='sell',
          kind='full_close', exchange_order_id='oe1')
    _adopt(broker)
    row = broker.store_ctx.get_order('exit1')
    assert row.filled_qty == 0.005
    assert 'e-exit' in broker._seen_exec_ids


def __test_baseline_nets_seeded_reducing_fills_out_of_the_entry_rows__(tmp_path):
    # The run held 0.02 long; its 0.01 TP leg filled while the bot was down,
    # and someone else holds 0.01 on the same one-way position, so the venue
    # still shows 0.02. The venue clamp sees no excess there: the seeded TP
    # fill itself must net the entry rows, or the run would own 0.02 of a
    # leg that holds only 0.01 of its own.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.02')],
        execs={'o1': [_exec('e1', '0.02')], 'otp': [_exec('e-tp', '0.01')]},
        order_lookups={'c1': {'orderId': 'o1', 'orderStatus': 'Filled',
                              'cumExecQty': '0.02'},
                       'tp1': {'orderId': 'otp', 'orderStatus': 'Filled',
                               'cumExecQty': '0.01'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.02, filled_qty=0.02)
    _seed(broker, 'tp1', qty=0.01, side='sell', kind='exit_leg',
          exchange_order_id='otp')
    _adopt(broker)
    assert broker._run_owned_signed_size() == pytest.approx(0.01)
    assert broker.store_ctx.get_order('tp1').closed_ts_ms is not None
    assert _clamp_events(broker) == []


def __test_helper_hedge_rows(buy: str, *, seq: int, sell: str = '0') -> list[dict]:
    """Both hedge legs of the symbol, sequenced."""
    return [
        {'symbol': 'BTCUSDT', 'positionIdx': 1, 'size': buy,
         'side': 'Buy' if float(buy) > 0 else '', 'avgPrice': '30000',
         'updatedTime': '1700000000000', 'seq': seq},
        {'symbol': 'BTCUSDT', 'positionIdx': 2, 'size': sell,
         'side': 'Sell' if float(sell) > 0 else '', 'avgPrice': '30000',
         'updatedTime': '1700000000000', 'seq': seq},
    ]


def __test_helper_give_up_before_restart(tmp_path, monkeypatch, *, view: str) -> _AdoptionFake:
    """A hedge run long 0.02 whose leg falls to ``view`` with no execution known.

    The position ledger gives the interval up past the grace: the journal
    books the venue truth and a credit is journalled for the reduce.
    """
    clock = [1_752_600_000.0]
    monkeypatch.setattr('pynecore_bybit.protection.epoch_time', lambda: clock[0])
    first = _AdoptionFake(market=_linear_instrument())
    first._position_mode = POSITION_MODE_HEDGE
    _open(tmp_path, first)
    _seed(first, 'c1', qty=0.02, filled_qty=0.02)
    _seed(first, 'tp1', qty=0.01, side='sell', kind='exit_leg', exchange_order_id='otp')
    first._adoption_baselined = True
    first._ingest_position_sizes(__test_helper_hedge_rows('0.02', seq=50))
    first._ledger_advance(first._market)
    first._ingest_position_sizes(__test_helper_hedge_rows(view, seq=60))
    first._ledger_advance(first._market)
    clock[0] += first.fill_report_grace_s + 1
    first._ledger_advance(first._market)
    assert first._run_owned_signed_size() == pytest.approx(float(view))
    return first


def __test_restart_after_a_give_up_does_not_net_the_reduce_twice__(tmp_path, monkeypatch):
    # The run's own TP of 0.01 filled at seq 60; the earlier process never
    # saw its execution and booked the leg's fall as venue truth.
    first = __test_helper_give_up_before_restart(tmp_path, monkeypatch, view='0.01')
    fresh = _AdoptionFake(
        market=_linear_instrument(),
        positions=__test_helper_hedge_rows('0.01', seq=60),
        execs={'o1': [{**_exec('e1', '0.02'), 'seq': '40'}],
               'otp': [{**_exec('e-tp', '0.01'), 'seq': '60'}]},
        order_lookups={'c1': {'orderId': 'o1', 'orderStatus': 'Filled', 'cumExecQty': '0.02'},
                       'tp1': {'orderId': 'otp', 'orderStatus': 'Filled', 'cumExecQty': '0.01'}},
    )
    fresh._position_mode = POSITION_MODE_HEDGE
    fresh.store_ctx = first.store_ctx
    _adopt(fresh)
    assert fresh._run_owned_signed_size() == pytest.approx(0.01)
    legs = asyncio.run(fresh.fetch_raw_positions('BTCUSDT'))
    assert [(leg.side, leg.qty) for leg in legs] == [('buy', pytest.approx(0.01))]
    # The credit is spent: nothing an earlier process left is taken again.
    assert fresh._load_persisted_ledger_credits() == {1: []}


def __test_restart_credit_never_covers_a_later_own_reduce__(tmp_path, monkeypatch):
    # The leg fell 0.01 at seq 60 with no execution known (a lost venue
    # close); someone else then added 0.01, and the run's own TP of 0.01
    # filled at seq 80 while the bot was down.
    first = __test_helper_give_up_before_restart(tmp_path, monkeypatch, view='0.01')
    fresh = _AdoptionFake(
        market=_linear_instrument(),
        positions=__test_helper_hedge_rows('0.01', seq=80),
        execs={'o1': [{**_exec('e1', '0.02'), 'seq': '40'}],
               'otp': [{**_exec('e-tp', '0.01'), 'seq': '80'}]},
        order_lookups={'c1': {'orderId': 'o1', 'orderStatus': 'Filled', 'cumExecQty': '0.02'},
                       'tp1': {'orderId': 'otp', 'orderStatus': 'Filled', 'cumExecQty': '0.01'}},
    )
    fresh._position_mode = POSITION_MODE_HEDGE
    fresh.store_ctx = first.store_ctx
    _adopt(fresh)
    # The TP took the run's last 0.01: the 0.01 the leg holds is foreign.
    assert fresh._run_owned_signed_size() == 0.0


def __test_baseline_flat_adoption_still_seeds_live_rows__(tmp_path):
    # A flat venue at adoption: the pre-restart fills netted out (a close
    # consumed them), so the ownership clamp nets the seeded cursor back to
    # zero — the run owns no exposure on a flat venue — while the execIds
    # still seed the de-dup frontier so nothing re-emits them, the resting
    # unfilled residual keeps the row alive, and the guard latches so later
    # live-trading fills are never clamped.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[],
        execs={'o1': [_exec('e1', '0.004')]},
        order_lookups={'c6': {'orderId': 'o1',
                              'orderStatus': 'PartiallyFilled',
                              'cumExecQty': '0.004'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c6', qty=0.01, filled_qty=0.0)
    _adopt(broker)
    assert broker._adoption_baselined is True
    row = broker.store_ctx.get_order('c6')
    assert row.filled_qty == 0.0            # owned exposure on a flat venue
    assert row.qty == pytest.approx(0.006)  # the resting residual survives
    assert 'e1' in broker._seen_exec_ids


def __test_baseline_stamps_floor_from_venue_clock__(tmp_path):
    # The backfill floor is the venue server clock read BEFORE the adoption
    # snapshot — never the local post-read wall clock, which could discard
    # a fill racing the snapshot.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.01')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c7', qty=0.01, filled_qty=0.0)
    _adopt(broker)
    assert broker._deriv_exec_floor_ms == _VENUE_TIME_MS


# === Race / failure handling ================================================

def __test_baseline_retries_on_racing_fill__(tmp_path):
    # A fill lands between the adoption snapshot and the verify re-read:
    # the snapshots differ, so the baseline re-runs against the fresh
    # snapshot and commits the cursor of the SECOND walk.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        position_snapshots=[
            [_pos(side='Buy', size='0.004', updated='1700000000000')],
            [_pos(side='Buy', size='0.01', updated='1700000000500')],
        ],
        execs={'o1': [_exec('e1', '0.004')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c8', qty=0.01, filled_qty=0.0)

    market = broker._market
    rows = asyncio.run(broker._fetch_position_rows(market))

    # Simulate the racing fill: the second walk sees the extra execution.
    class _RacingExecs(dict):
        def get(self, key, default=None):
            value = super().get(key, default)
            if key == 'o1' and broker.position_snapshots \
                    and len(broker.position_snapshots) == 1:
                return list(value or []) + [_exec('e2', '0.006')]
            return value

    broker.execs = _RacingExecs(broker.execs)
    final = asyncio.run(broker._apply_adoption_baseline(market, rows))
    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c8').filled_qty == 0.01
    assert {'e1', 'e2'} <= broker._seen_exec_ids
    # The caller must report from the verified (fresh) snapshot.
    assert final and final[0]['size'] == '0.01'


def __test_baseline_not_latched_on_failed_seed_read__(tmp_path):
    # A transport failure during the execution seed walk: nothing is
    # committed, the guard stays unlatched, and the read RAISES (retryable)
    # so the engine's one-shot startup adoption never consumes an
    # unbaselined snapshot; the retried read succeeds once the venue heals.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.01')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c9', qty=0.01, filled_qty=0.0)
    broker.fail_exec_reads = True
    with pytest.raises(BybitAdoptionBaselineError):
        _adopt(broker)
    assert broker._adoption_baselined is False
    assert broker.store_ctx.get_order('c9').filled_qty == 0.0
    broker.fail_exec_reads = False
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c9').filled_qty == 0.01


def __test_baseline_retires_unattributable_non_entry_row__(tmp_path):
    # A handle-less exit row the venue conclusively does not know: not live
    # (a live order always answers the realtime lookup), so it can never
    # fill again — the baseline retires the row AND its intent envelope at
    # commit; leaving it live would strand both forever (a row with no
    # exchange handle is exempt from the disappearance pass).
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'x1', qty=0.005, filled_qty=0.0, side='sell',
          kind='exit_leg', exchange_order_id='')
    broker.store_ctx.record_envelope('x1', 1_700_000_000_000, 0)
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert list(broker.store_ctx.iter_live_orders(symbol='BTCUSDT')) == []
    envelopes, _pending = broker.store_ctx.replay()
    assert 'x1' not in envelopes


def __test_baseline_keeps_unattributable_entry_row_with_fills__(tmp_path):
    # A handle-less ENTRY row carrying fills is NOT retired: its fills are
    # real components of the adopted position, and the position lifecycle
    # (positions-namespace tracking + flat sweep) owns its retirement.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'e1', qty=0.01, filled_qty=0.01, exchange_order_id='')
    _adopt(broker)
    assert broker._adoption_baselined is True
    live = [r.client_order_id
            for r in broker.store_ctx.iter_live_orders(symbol='BTCUSDT')]
    assert live == ['e1']


def __test_baseline_retires_dead_partial_exit_with_stale_handle__(tmp_path):
    # A conclusively not-found exit leg that still carries a STALE exchange
    # handle: the handle is walked first (its historical execution seeds
    # the de-dup frontier and the cursor), but the residual can never fill
    # again — the row and its envelope are retired at commit. Leaving it
    # live would strand it: the disappearance confirmation maps the same
    # not-found to INCONCLUSIVE forever.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'oe1': [_exec('e-exit', '0.002')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'x2', qty=0.005, filled_qty=0.0, side='sell',
          kind='exit_leg', exchange_order_id='oe1')
    broker.store_ctx.record_envelope('x2', 1_700_000_000_000, 0)
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert 'e-exit' in broker._seen_exec_ids
    assert list(broker.store_ctx.iter_live_orders(symbol='BTCUSDT')) == []
    envelopes, _pending = broker.store_ctx.replay()
    assert 'x2' not in envelopes


def __test_baseline_normalizes_partially_filled_dead_entry__(tmp_path):
    # A conclusively not-found ENTRY row with PARTIAL fills stays live (its
    # fills are a real component of the adopted position) but its qty is
    # normalized down to the terminal filled amount: both position-lifecycle
    # owners (positions-namespace tracking + flat sweep) predicate on a
    # FULL fill, and the dead residual can never fill again — an
    # un-normalized row would be owned by neither.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.004')],
        execs={'o1': [_exec('e1', '0.004')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'e2', qty=0.01, filled_qty=0.0)
    _adopt(broker)
    row = broker.store_ctx.get_order('e2')
    assert row is not None
    assert row.filled_qty == 0.004
    assert row.qty == 0.004         # normalized: the full-fill owners take it
    live = [r.client_order_id
            for r in broker.store_ctx.iter_live_orders(symbol='BTCUSDT')]
    assert live == ['e2']


def __test_baseline_full_seed_completes_exit_intent__(tmp_path):
    # A fully seeded exit row's terminal closure must ALSO retire the
    # persisted intent envelope: the baseline suppresses the fill event,
    # so the engine's own envelope drop never runs — without the
    # completion the envelope would replay and hand a re-emitted intent
    # the same spent client order id.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'oe1': [_exec('e-exit', '0.005')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'exit1', qty=0.005, filled_qty=0.0, side='sell',
          kind='exit_leg', exchange_order_id='oe1')
    broker.store_ctx.record_envelope('exit1', 1_700_000_000_000, 0)
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert list(broker.store_ctx.iter_live_orders(symbol='BTCUSDT')) == []
    envelopes, _pending = broker.store_ctx.replay()
    assert 'exit1' not in envelopes


def __test_baseline_uses_parked_private_execution_while_rest_index_lags__(tmp_path):
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': []},
        order_lookups={'c11': {'orderId': 'o1', 'orderStatus': 'Filled',
                               'cumExecQty': '0.01'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c11', qty=0.01, filled_qty=0.0)
    broker._pre_adoption_frames = [_exec_frame('e-private', '0.01', coid='c11')]

    _adopt(broker)

    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c11').filled_qty == 0.01
    assert 'e-private' in broker._seen_exec_ids
    assert broker._replay_pre_adoption_frames(broker._market) == []


def __test_baseline_counts_fill_seen_by_rest_and_parked_push_once__(tmp_path):
    # One real 0.01 fill of a 0.02 entry reaches the startup window through
    # BOTH the REST walk and a parked push, on a shared 0.03 position (0.02
    # foreign). The fill's quantity must enter the cursor once: the venue
    # clamp cannot catch a doubled cursor that the foreign exposure covers.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.03')],
        execs={'o1': [_exec('e-both', '0.01')]},
        order_lookups={'c11': {'orderId': 'o1', 'orderStatus': 'PartiallyFilled',
                               'cumExecQty': '0.01'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c11', qty=0.02, filled_qty=0.0)
    broker._pre_adoption_frames = [_exec_frame('e-both', '0.01', coid='c11')]

    _adopt(broker)

    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c11').filled_qty == 0.01
    assert 'e-both' in broker._seen_exec_ids


def __test_baseline_retries_execution_index_lag_in_same_adoption__(tmp_path):
    # The order endpoint can expose cumExecQty before the execution index
    # publishes the corresponding row. The bounded adoption pass re-reads the
    # complete evidence and commits once the execution row becomes visible.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        order_lookups={'c11': {'orderId': 'o1', 'orderStatus': 'Filled',
                               'cumExecQty': '0.01'}},
    )

    class _CatchingUpExecutions(dict):
        reads = 0

        def get(self, key, default=None):
            if key == 'o1':
                self.reads += 1
                if self.reads < 3:
                    return []
            return super().get(key, default)

    executions = _CatchingUpExecutions({'o1': [_exec('e1', '0.01')]})
    broker.execs = executions
    _open(tmp_path, broker)
    _seed(broker, 'c11', qty=0.01, filled_qty=0.0)

    _adopt(broker)

    assert executions.reads == 3
    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c11').filled_qty == 0.01
    assert 'e1' in broker._seen_exec_ids


def __test_baseline_raises_on_execution_index_lag__(tmp_path):
    # The order truth gate: both stable position snapshots already reflect
    # a fill whose execution row is not indexed yet — the walked execQty
    # sum falls short of the order's authoritative cumExecQty, so the
    # baseline must raise (retryable) instead of committing an understated
    # cursor; once the index catches up the retried baseline commits.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.004')]},
        order_lookups={'c11': {'orderId': 'o1', 'orderStatus': 'Filled',
                               'cumExecQty': '0.01'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c11', qty=0.01, filled_qty=0.0)
    with pytest.raises(BybitAdoptionBaselineError):
        _adopt(broker)
    assert broker._adoption_baselined is False
    assert broker.store_ctx.get_order('c11').filled_qty == 0.0
    broker.execs['o1'].append(_exec('e2', '0.006'))
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert broker.store_ctx.get_order('c11').filled_qty == 0.01
    assert {'e1', 'e2'} <= broker._seen_exec_ids


# === Inverse domain correctness ============================================

def __test_baseline_inverse_uses_wire_contract_domain__(tmp_path):
    # Inverse: execQty from /v5/execution/list is whole USD contracts — the
    # SAME wire domain as the row cursor — so the seed is
    # contract-for-contract, never converted through the base anchor.
    broker = _AdoptionFake(
        market=_inverse_instrument(),
        positions=[_inv_pos(side='Buy', size='200', avg='40000')],
        execs={'o1': [_exec('e1', '200')]},
        order_lookups={'c10': {'orderId': 'o1',
                               'orderStatus': 'PartiallyFilled',
                               'cumExecQty': '200'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c10', qty=500.0, filled_qty=0.0, side='buy',
          extras={'anchor': '40000'})
    pos = _adopt(broker)
    row = broker.store_ctx.get_order('c10')
    assert row.filled_qty == 200.0          # contracts, NOT base-converted
    # The returned position size IS base-converted (core-facing), proving the
    # cursor stayed in the wire domain while the report did not.
    assert pos is not None and pos.size != 200.0


# === Ownership clamp: entry cursors vs the venue position ===================

def _clamp_events(broker) -> list:
    import json as _json
    cur = broker.store_ctx._store._conn.execute(
        "SELECT client_order_id, payload FROM events "
        "WHERE kind = 'adoption_baseline_clamped' ORDER BY id",
    )
    return [(coid, _json.loads(payload) if payload else {})
            for coid, payload in cur.fetchall()]


def __test_baseline_clamps_inherited_ownership_to_venue__(tmp_path):
    # The 2026-08-21 inverse incident: rows inherited from a prior run own
    # closes the venue already executed (the closes filled before the
    # fill-time netting existed / while the bot was down), so the adopted
    # book vastly overstates the venue (305 contracts vs 1). The clamp nets
    # the per-side excess FIFO: the oldest row is consumed whole, the newer
    # row keeps exactly the venue's live size.
    broker = _AdoptionFake(
        market=_inverse_instrument(),
        positions=[_inv_pos(side='Buy', size='1', avg='76455')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'a', qty=300.0, filled_qty=300.0, side='buy',
          exchange_order_id='', extras={'anchor': '76455'})
    _seed(broker, 'b', qty=5.0, filled_qty=5.0, side='buy',
          exchange_order_id='', extras={'anchor': '76455'})
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (1_700_000_000_000, 'a'),
    )
    broker.store_ctx._store._conn.execute(
        "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
        (1_700_000_001_000, 'b'),
    )
    _adopt(broker)
    live = [r.client_order_id
            for r in broker.store_ctx.iter_live_orders(symbol='BTCUSD')]
    assert live == ['b']                    # 'a' consumed whole (FIFO)
    row = broker.store_ctx.get_order('b')
    assert row.filled_qty == 1.0            # exactly the venue's live size
    assert row.qty == 1.0
    assert [coid for coid, _ in _clamp_events(broker)] == ['a', 'b']


def __test_baseline_hedge_clamp_is_per_side_and_noop_when_covered__(tmp_path):
    # Hedge legs are separate positions, so the clamp compares per side: a
    # buy entry fully covered by the venue's Buy leg is untouched (and emits
    # no event), while a sell entry whose Sell leg is gone from the venue is
    # netted away.
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=__test_helper_hedge_rows('0.02', seq=60),
    )
    broker._position_mode = POSITION_MODE_HEDGE
    _open(tmp_path, broker)
    _seed(broker, 'long1', qty=0.02, filled_qty=0.02, side='buy', exchange_order_id='')
    _seed(broker, 'short1', qty=0.03, filled_qty=0.03, side='sell', exchange_order_id='')
    _adopt(broker)
    live = [r.client_order_id
            for r in broker.store_ctx.iter_live_orders(symbol='BTCUSDT')]
    assert live == ['long1']
    assert broker.store_ctx.get_order('long1').filled_qty == 0.02
    assert [coid for coid, _ in _clamp_events(broker)] == ['short1']


def __test_helper_fold_events(broker) -> list:
    cur = broker.store_ctx._store._conn.execute(
        "SELECT client_order_id FROM events "
        "WHERE kind = 'adoption_baseline_folded' ORDER BY id",
    )
    return [coid for (coid,) in cur.fetchall()]


def __test_helper_age_rows(broker, *coids: str) -> None:
    """Give the rows strictly increasing creation times, in argument order."""
    for offset, coid in enumerate(coids):
        broker.store_ctx._store._conn.execute(
            "UPDATE orders SET created_ts_ms = ? WHERE client_order_id = ?",
            (1_700_000_000_000 + offset * 1000, coid),
        )


def __test_baseline_oneway_clamp_folds_the_sides_first__(tmp_path):
    # One-way: a long 2 entry, then a folded reversal (a short entry of 3)
    # that closed it and opened 1 short. The venue now holds 2 long: the
    # run's short 1 is gone, and the long is not the run's. Clamped side by
    # side, the old long row would keep owning it.
    broker = _AdoptionFake(
        market=_inverse_instrument(),
        positions=[_inv_pos(side='Buy', size='2', avg='76455')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'long1', qty=2.0, filled_qty=2.0, side='buy',
          exchange_order_id='', extras={'anchor': '76455'})
    _seed(broker, 'short1', qty=3.0, filled_qty=3.0, side='sell',
          exchange_order_id='', extras={'anchor': '76455'})
    __test_helper_age_rows(broker, 'long1', 'short1')
    _adopt(broker)
    assert broker._run_owned_signed_size() == 0.0
    assert [r.client_order_id
            for r in broker.store_ctx.iter_live_orders(symbol='BTCUSD')] == []
    assert __test_helper_fold_events(broker) == ['long1', 'short1']
    assert [coid for coid, _ in _clamp_events(broker)] == ['short1']


def __test_helper_reversal_restart(tmp_path, *, venue: str) -> _AdoptionFake:
    """A one-way run that went long 0.02, then reversed short with a 0.04
    sell (net short 0.02), restarting on a venue position of ``venue`` short
    (``seq`` 60)."""
    broker = _AdoptionFake(
        market=_linear_instrument(),
        positions=[{**_pos(side='Sell', size=venue), 'seq': 60}],
        execs={'o1': [{**_exec('e1', '0.02'), 'seq': '40'}],
               'orev': [{**_exec('e-rev', '0.04'), 'seq': '50'}]},
        order_lookups={'c1': {'orderId': 'o1', 'orderStatus': 'Filled', 'cumExecQty': '0.02'},
                       'rev': {'orderId': 'orev', 'orderStatus': 'Filled',
                               'cumExecQty': '0.04'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.02, filled_qty=0.02, exchange_order_id='o1')
    _seed(broker, 'rev', qty=0.04, filled_qty=0.04, side='sell', exchange_order_id='orev')
    __test_helper_age_rows(broker, 'c1', 'rev')
    broker.quarantines = []
    broker.quarantine_sink = (
        lambda reason, ctx, *, kind: broker.quarantines.append((kind, ctx)))
    _adopt(broker)
    return broker


def __test_baseline_oneway_reversal_keeps_the_run_share_under_a_foreign_add__(tmp_path):
    # Someone else sold 0.01 on the same one-way position while the bot was
    # down: the venue holds 0.03 short, of which 0.02 is the run's.
    broker = __test_helper_reversal_restart(tmp_path, venue='0.03')
    assert broker._run_owned_signed_size() == pytest.approx(-0.02)
    assert broker._leg_ledgers[0].share == pytest.approx(-0.02)
    assert _clamp_events(broker) == []
    # The other party buys its own 0.01 back: the run's share is intact.
    broker._translate_private_frame(__test_helper_oneway_exec(
        'f-70', side='Buy', qty='0.01', seq=70), broker._market)
    broker._translate_private_frame(__test_helper_oneway_view(
        side='Sell', size='0.02', seq=70), broker._market)
    assert broker.quarantines == []
    assert broker._run_owned_signed_size() == pytest.approx(-0.02)
    # A buy that then takes the leg below the run's 0.02 is an intervention.
    broker._translate_private_frame(__test_helper_oneway_exec(
        'f-80', side='Buy', qty='0.01', seq=80), broker._market)
    broker._translate_private_frame(__test_helper_oneway_view(
        side='Sell', size='0.01', seq=80), broker._market)
    assert [kind for kind, _ in broker.quarantines] == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert broker.quarantines[0][1]['consumed'] == pytest.approx(0.01)


def __test_baseline_oneway_reversal_clamps_the_net_to_the_venue__(tmp_path):
    # Someone else bought 0.01 back from the run's short while the bot was
    # down: the run keeps 0.01, never more and never less.
    broker = __test_helper_reversal_restart(tmp_path, venue='0.01')
    assert broker._run_owned_signed_size() == pytest.approx(-0.01)
    assert [coid for coid, _ in _clamp_events(broker)] == ['rev']


def __test_helper_oneway_exec(exec_id: str, *, side: str, qty: str, seq: int) -> dict:
    """A foreign user execution on the one-way position, as a push frame."""
    return {'topic': 'execution', 'data': [{
        'symbol': 'BTCUSDT', 'category': 'linear', 'execType': 'Trade',
        'execId': exec_id, 'orderId': f"order-{exec_id}", 'orderLinkId': '',
        'side': side, 'execQty': qty, 'execPrice': '30000', 'execFee': '0',
        'execTime': str(_VENUE_TIME_MS + 1000), 'closedSize': qty,
        'stopOrderType': '', 'createType': 'CreateByUser', 'seq': str(seq),
    }]}


def __test_helper_oneway_view(*, side: str, size: str, seq: int) -> dict:
    """A one-way position push without trading stops."""
    return {'topic': 'position', 'data': [{
        'symbol': 'BTCUSDT', 'category': 'linear', 'positionIdx': 0,
        'side': side, 'size': size, 'takeProfit': '0', 'stopLoss': '0',
        'trailingStop': '0', 'updatedTime': '1752600000000', 'seq': str(seq),
    }]}


# === Startup gate: fills racing the adoption baseline (F9) =================

class _GateFake(_AdoptionFake):
    """``_AdoptionFake`` with the broker-startup latch stubbed to a no-op."""

    async def _ensure_broker_started(self) -> None:
        return None


class _SignalQueue(asyncio.Queue):
    """Queue that signals when the consumer blocks on an empty read."""

    def __init__(self):
        super().__init__()
        self.drained = asyncio.Event()

    async def get(self):
        if self.empty():
            self.drained.set()
        return await super().get()


def _exec_frame(exec_id, qty, *, coid='c1', order_id='o1') -> dict:
    return {'topic': 'execution', 'data': [{
        'symbol': 'BTCUSDT', 'category': 'linear', 'execType': 'Trade',
        'execId': exec_id, 'orderId': order_id, 'orderLinkId': coid,
        'execQty': qty, 'execPrice': '30000', 'execFee': '0',
        'execTime': str(_VENUE_TIME_MS - 1000), 'side': 'Buy',
    }]}


def __test_startup_gate_parks_frames_until_baseline__(tmp_path):
    # F9 regression, park half: a fill pushed while the adoption baseline
    # has not committed must NOT be translated — no OrderEvent, no execId
    # booking, no cursor movement. It is parked for the post-baseline
    # replay; emitting it here would double-apply once the engine adopts
    # the position snapshot that already contains it.
    broker = _GateFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.004'), _exec('e-race', '0.006')]},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01, filled_qty=0.0)

    async def _drive():
        queue = _SignalQueue()
        queue.put_nowait(_exec_frame('e-race', '0.006'))
        broker._private_events = queue
        broker._private_ws = type('_StubWS', (), {'is_open': True})()
        agen = cast(AsyncGenerator, broker.watch_orders())
        task = asyncio.ensure_future(agen.__anext__())
        # ``drained`` fires when the loop consumed the frame and blocked
        # on the next (empty) read — the deterministic "frame processed"
        # signal, no timing involved.
        await queue.drained.wait()
        assert not task.done()
        assert len(broker._pre_adoption_frames) == 1
        assert 'e-race' not in broker._seen_exec_ids
        assert broker.store_ctx.get_order('c1').filled_qty == 0.0
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await agen.aclose()

    asyncio.run(_drive())


def __test_startup_gate_replay_owns_each_fill_once__(tmp_path):
    # F9 regression, replay half: after the baseline commits, a parked
    # fill the adopted snapshot already owns (its execId was seeded from
    # the per-order venue walk) is dropped, while a genuinely
    # post-adoption fill parked alongside is emitted exactly once.
    broker = _GateFake(
        market=_linear_instrument(),
        positions=[_pos(side='Buy', size='0.01')],
        execs={'o1': [_exec('e1', '0.004'), _exec('e-race', '0.006')]},
        order_lookups={'c1': {'orderId': 'o1', 'orderStatus': 'Filled',
                              'cumExecQty': '0.01'},
                       'c2': {'orderId': 'o2', 'orderStatus': 'New',
                              'cumExecQty': '0'}},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01, filled_qty=0.0)
    _seed(broker, 'c2', qty=0.002, filled_qty=0.0, exchange_order_id='o2')
    broker._pre_adoption_frames = [
        _exec_frame('e-race', '0.006'),
        {
            **_exec_frame('e-post', '0.002', coid='c2', order_id='o2'),
            'data': [{
                **_exec_frame('e-post', '0.002', coid='c2', order_id='o2')['data'][0],
                'execTime': str(_VENUE_TIME_MS + 1000),
            }],
        },
    ]
    market = broker._market
    # While the gate is active the replay is a no-op — nothing may be
    # emitted ahead of the baseline.
    assert broker._replay_pre_adoption_frames(market) == []
    assert len(broker._pre_adoption_frames) == 2
    _adopt(broker)
    assert broker._adoption_baselined is True
    assert 'e-race' in broker._seen_exec_ids
    events = broker._replay_pre_adoption_frames(market)
    assert [event.fill_id for event in events] == ['e-post']
    assert broker._pre_adoption_frames == []
    # The adopted cursor was seeded once by the baseline and the replayed
    # duplicate did not move it again.
    assert broker.store_ctx.get_order('c1').filled_qty == 0.01
    assert broker.store_ctx.get_order('c2').filled_qty == 0.002
