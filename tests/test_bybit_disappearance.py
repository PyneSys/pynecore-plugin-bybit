"""
@pyne
"""
import asyncio
from time import time as epoch_time

import pytest

from pynecore.core.broker.exceptions import UnexpectedCancelError
from pynecore.core.broker.models import LegType
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
)

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.models import InstrumentInfo


def main():
    """Dummy main to make this a valid Pyne script."""
    pass


# === Instrument builder ====================================================

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


# === Fake broker (endpoint-routed REST) ====================================

class _DisappearanceFake(Bybit):
    """Bybit with the REST dispatcher replaced by an endpoint-routed fake.

    ``realtime_by_coid`` / ``history_by_coid`` map an ``orderLinkId`` to the
    order object served by the id-filtered realtime / history lookup;
    ``open_orders`` is the resting-order snapshot; ``positions`` the
    ``/v5/position/list`` rows. ``raise_realtime`` makes every realtime read
    fail (a transport-down probe). Cancels succeed unless routed otherwise.
    """

    def __init__(self, *, market: InstrumentInfo,
                 realtime_by_coid=None, history_by_coid=None,
                 open_orders=None, positions=None, raise_realtime=False):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self.realtime_by_coid = dict(realtime_by_coid or {})
        self.history_by_coid = dict(history_by_coid or {})
        self.open_orders = list(open_orders or [])
        self.positions = list(positions or [])
        self.raise_realtime = raise_realtime
        self.calls: list = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        params = dict(params or {})
        self.calls.append((endpoint, params))
        if endpoint == '/v5/order/realtime':
            if self.raise_realtime:
                raise BybitConnectionError("realtime down")
            coid = params.get('orderLinkId')
            if coid is not None:
                order = self.realtime_by_coid.get(coid)
                return {'list': [order] if order else []}
            return {'list': list(self.open_orders), 'nextPageCursor': ''}
        if endpoint == '/v5/order/history':
            order = self.history_by_coid.get(params.get('orderLinkId'))
            return {'list': [order] if order else []}
        if endpoint == '/v5/position/list':
            return {'list': list(self.positions)}
        if endpoint == '/v5/order/cancel':
            return {}
        raise AssertionError(f"unexpected REST endpoint {endpoint} {params}")


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="disap", symbol=broker._market.symbol, timeframe="1",
        account_id="disap-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// disap")


def _order(*, order_id, coid, status, cum='0', qty='0.01') -> dict:
    return {
        'orderId': order_id, 'orderLinkId': coid, 'orderStatus': status,
        'cumExecQty': cum, 'qty': qty, 'avgPrice': '30000',
    }


def _seed(broker, coid, *, qty=0.01, filled=0.0, state='confirmed',
          kind=ENTRY_KIND_WORKING, exchange_order_id=None, extras=None) -> None:
    fields = dict(
        symbol=broker._market.symbol, side='buy', qty=qty, filled_qty=filled,
        state=state, intent_key=coid, pine_entry_id='Long',
        extras={'kind': kind, 'order_type': 'limit', **(extras or {})},
    )
    if exchange_order_id is not None:
        fields['exchange_order_id'] = exchange_order_id
    broker.store_ctx.upsert_order(coid, **fields)
    if exchange_order_id is not None:
        broker.store_ctx.add_ref(coid, 'order_id', exchange_order_id)


def _stamp(broker, coid, *, age_s=100.0) -> None:
    """Pre-stamp a row's missing-pending breadcrumb aged past the grace window."""
    row = broker.store_ctx.get_order(coid)
    extras = dict(row.extras or {})
    extras['missing_pending_since'] = epoch_time() - age_s
    broker.store_ctx.upsert_order(coid, extras=extras)


def _run_reconcile(broker, position_rows) -> list:
    return asyncio.run(
        broker._reconcile_disappearance(broker._market, position_rows),
    )


def _live_coids(broker) -> set:
    return {r.client_order_id for r in broker.store_ctx.iter_live_orders()}


def _stamp_of(broker, coid):
    row = broker.store_ctx.get_order(coid)
    return (row.extras or {}).get('missing_pending_since')


# === Verdict: CANCELLED (dual signal + policy) =============================

def __test_grace_expired_cancel_emits_event_and_quarantines__(tmp_path):
    # A resting working entry gone from the open set, found dead (zero fills)
    # in history: retire it with a synthetic cancelled event AND latch the
    # engine quarantine through the ``stop`` policy's sink.
    calls: list = []
    broker = _DisappearanceFake(
        market=_linear_instrument(),
        history_by_coid={'c1': _order(order_id='o1', coid='c1', status='Cancelled')},
    )
    broker.on_unexpected_cancel = 'stop'
    broker.quarantine_sink = lambda reason, ctx: calls.append((reason, ctx))
    _open(tmp_path, broker)
    _seed(broker, 'c1', exchange_order_id='o1')
    _stamp(broker, 'c1')
    events = _run_reconcile(broker, [])
    assert [e.event_type for e in events] == ['cancelled']
    assert 'c1' not in _live_coids(broker)          # terminally retired
    assert calls                                    # quarantine latched


# === Verdict: FILLED (false premise cleared) ===============================

def __test_grace_expired_fill_clears_stamp_no_event__(tmp_path):
    # The order vanished because it FILLED: the stamp premise is false — clear
    # it, keep the row live for the PUSH/catch-up path, book nothing here.
    broker = _DisappearanceFake(
        market=_linear_instrument(),
        history_by_coid={'c2': _order(order_id='o2', coid='c2',
                                      status='Filled', cum='0.01')},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c2', exchange_order_id='o2')
    _stamp(broker, 'c2')
    events = _run_reconcile(broker, [])
    assert events == []
    assert 'c2' in _live_coids(broker)
    assert _stamp_of(broker, 'c2') is None          # stamp cleared


# === Verdict: INCONCLUSIVE (transport failure) =============================

def __test_grace_expired_transport_failure_keeps_stamp__(tmp_path):
    # The confirm lookup transport is down: never conclude a cancel from a
    # truncated read — keep the stamp, emit nothing, leave the row live.
    broker = _DisappearanceFake(market=_linear_instrument(), raise_realtime=True)
    _open(tmp_path, broker)
    _seed(broker, 'c3', exchange_order_id='o3')
    _stamp(broker, 'c3')
    events = _run_reconcile(broker, [])
    assert events == []
    assert 'c3' in _live_coids(broker)
    assert _stamp_of(broker, 'c3') is not None       # stamp preserved


# === Exemption: natural_close_at ===========================================

def __test_natural_close_flagged_row_is_exempt__(tmp_path):
    # A row flagged as a known/expected close is skipped by the tracker even
    # though its history lookup would otherwise classify it as an external
    # cancel — no false unexpected-cancel for an expected disappearance.
    broker = _DisappearanceFake(
        market=_linear_instrument(),
        history_by_coid={'c4': _order(order_id='o4', coid='c4', status='Cancelled')},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c4', exchange_order_id='o4',
          extras={'natural_close_at': epoch_time()})
    _stamp(broker, 'c4')
    events = _run_reconcile(broker, [])
    assert events == []
    assert 'c4' in _live_coids(broker)


# === Presence: clear on reappearance =======================================

def __test_reappearance_clears_stamp__(tmp_path):
    # The order is back in the open set: phase-1 presence clears the stale
    # stamp and no grace confirmation runs.
    broker = _DisappearanceFake(
        market=_linear_instrument(),
        open_orders=[_order(order_id='o5', coid='c5', status='New')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c5', exchange_order_id='o5')
    _stamp(broker, 'c5')
    events = _run_reconcile(broker, [])
    assert events == []
    assert 'c5' in _live_coids(broker)
    assert _stamp_of(broker, 'c5') is None           # cleared on reappearance


# === Sibling sweep: skip filled exposure ===================================

def __test_sibling_sweep_skips_filled_exposure__(tmp_path):
    # The zero-fill sibling sweep cancels resting siblings but LEAVES a
    # settled position row (live exposure) for the operator.
    broker = _DisappearanceFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed(broker, 'orig', exchange_order_id='oo')
    _seed(broker, 'sf', filled=0.01, kind=ENTRY_KIND_POSITION,
          exchange_order_id='osf')                    # settled: real exposure
    _seed(broker, 'sr', kind=ENTRY_KIND_WORKING, exchange_order_id='osr')  # resting
    origin = broker.store_ctx.get_order('orig')
    asyncio.run(broker._cancel_sibling_working_orders(origin))
    live = _live_coids(broker)
    assert 'sf' in live                               # filled sibling kept
    assert 'sr' not in live                           # zero-fill sibling swept


# === Halt propagation ======================================================

def __test_halting_policy_propagates_through_reconcile__(tmp_path):
    # ``stop`` policy with NO quarantine sink wired falls back to the
    # process-exiting halt: the reconcile pass must RE-RAISE it (a graceful
    # engine stop), not swallow it as a transient failure.
    broker = _DisappearanceFake(
        market=_linear_instrument(),
        history_by_coid={'c7': _order(order_id='o7', coid='c7', status='Cancelled')},
    )
    broker.on_unexpected_cancel = 'stop'              # quarantine_sink stays None
    _open(tmp_path, broker)
    _seed(broker, 'c7', exchange_order_id='o7')
    _stamp(broker, 'c7')
    with pytest.raises(UnexpectedCancelError):
        _run_reconcile(broker, [])


# === Entry-row flat sweep: causal freshness + envelope teardown ============
#
# The sweep retires a fully-filled entry row once the venue position is flat.
# Two coupled hazards it must handle: (1) the bot's own opening ``execution``
# push can be processed BEFORE the ``position`` push refreshes the size cache,
# so a stale-flat reading must NOT retire the still-open entry row; (2) once
# the position IS genuinely flat, retiring the row must also clear the intent
# envelope, so a re-entry of the same Pine id mints a fresh ``orderLinkId``.


def _own_fill(broker, coid, *, exec_id, exec_time, qty='0.002') -> list:
    return broker._translate_executions({
        'topic': 'execution',
        'data': [{
            'category': 'linear', 'symbol': 'BTCUSDT', 'execType': 'Trade',
            'execId': exec_id, 'orderId': 'o1', 'orderLinkId': coid,
            'side': 'Buy', 'execQty': qty, 'execPrice': '30000',
            'execTime': exec_time,
        }],
    }, broker._market)


def _position_push(broker, *, size, side, updated) -> None:
    broker._ingest_position_frame({
        'topic': 'position',
        'data': [{'category': 'linear', 'symbol': 'BTCUSDT', 'positionIdx': 0,
                  'size': size, 'side': side, 'updatedTime': updated}],
    }, broker._market)


def __test_flat_sweep_stale_position_keeps_entry_live__(tmp_path):
    # Bug #2: the own opening fill (execTime 2000) is handled before the
    # position push. The size cache still shows the adopted stale-flat
    # (updatedTime 1000) — older than the fill — so the sweep must be
    # suppressed and the freshly filled entry row stays live.
    broker = _DisappearanceFake(market=_linear_instrument())
    _open(tmp_path, broker)
    broker._ingest_position_sizes(
        [{'positionIdx': 0, 'size': '0', 'side': '', 'updatedTime': '1000'}],
    )
    _seed(broker, 'e1', qty=0.002, filled=0.002, state='confirmed',
          kind=ENTRY_KIND_POSITION, exchange_order_id='o1')
    broker._record_identity('e1', pine_id='Long', from_entry=None,
                            leg_type=LegType.ENTRY, qty=0.002)
    events = _own_fill(broker, 'e1', exec_id='x1', exec_time='2000')
    assert len(events) == 1
    assert 'e1' in _live_coids(broker)                # stale-flat -> not swept

    # The size push finally lands (size present, fresh time): still not flat.
    _position_push(broker, size='0.002', side='Buy', updated='2500')
    assert 'e1' in _live_coids(broker)


def __test_flat_sweep_genuine_flat_clears_envelope__(tmp_path):
    # A genuine flat snapshot that POST-dates the last own fill retires the
    # entry row AND clears its intent envelope, so a re-entry mints a fresh
    # coid instead of reusing the spent one (Bug #1 root fix).
    broker = _DisappearanceFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed(broker, 'e1', qty=0.002, filled=0.002, state='confirmed',
          kind=ENTRY_KIND_POSITION, exchange_order_id='o1')
    broker._record_identity('e1', pine_id='Long', from_entry=None,
                            leg_type=LegType.ENTRY, qty=0.002)
    broker.store_ctx.record_envelope('e1', 1_784_292_600_000, 0)

    _own_fill(broker, 'e1', exec_id='x1', exec_time='2000')
    assert 'e1' in _live_coids(broker)                # no fresh snapshot yet
    assert 'e1' in broker.store_ctx.replay()[0]       # envelope still anchored

    _position_push(broker, size='0', side='', updated='3000')
    assert 'e1' not in _live_coids(broker)            # swept on genuine flat
    assert 'e1' not in broker.store_ctx.replay()[0]   # envelope cleared
