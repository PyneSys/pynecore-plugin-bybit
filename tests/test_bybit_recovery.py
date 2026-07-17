"""
@pyne
"""
import asyncio
from decimal import Decimal

from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    EXTRAS_KEY_CANCEL_TENTATIVE_SINCE_TS_MS,
    LEG_KIND_SL_PARTIAL,
    LEG_STATE_ARMED,
    LEG_STATE_CANCEL_TENTATIVE,
    create_engine_trigger_partial_leg_row,
    iter_active_engine_trigger_partial_legs,
    update_engine_trigger_partial_leg_state,
)

from pynecore_bybit import Bybit, BybitConfig
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


# === Fake broker (endpoint-routed REST) ====================================

class _RecoveryFake(Bybit):
    """Bybit with the REST dispatcher replaced by an endpoint-routed fake.

    ``realtime_by_coid`` / ``history_by_coid`` map an ``orderLinkId`` to the
    order object served by the id-filtered realtime / history lookup;
    ``open_orders`` is the resting-order snapshot; ``positions`` the
    ``/v5/position/list`` rows; ``executions`` the ``/v5/execution/list``
    rows. Every call is recorded in ``calls``.
    """

    def __init__(self, *, market: InstrumentInfo,
                 realtime_by_coid=None, history_by_coid=None,
                 open_orders=None, positions=None, executions=None):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self.realtime_by_coid = dict(realtime_by_coid or {})
        self.history_by_coid = dict(history_by_coid or {})
        self.open_orders = list(open_orders or [])
        self.positions = list(positions or [])
        self.executions = list(executions or [])
        self.calls: list = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        params = dict(params or {})
        self.calls.append((endpoint, params))
        if endpoint == '/v5/order/realtime':
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
        if endpoint == '/v5/execution/list':
            return {'list': list(self.executions), 'nextPageCursor': ''}
        raise AssertionError(f"unexpected REST endpoint {endpoint} {params}")


def _open(tmp_path, broker) -> None:
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="recov", symbol=broker._market.symbol, timeframe="1",
        account_id="recov-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// recov")


def _order(*, order_id, coid, status, cum='0', qty='0.01', avg='30000') -> dict:
    return {
        'orderId': order_id, 'orderLinkId': coid, 'orderStatus': status,
        'cumExecQty': cum, 'qty': qty, 'avgPrice': avg,
    }


def _exec(*, exec_id, order_id) -> dict:
    return {
        'execId': exec_id, 'orderId': order_id, 'execType': 'Trade',
        'execQty': '0.002', 'execPrice': '30000', 'execFee': '0',
        'execTime': '1700000000000', 'side': 'Buy', 'symbol': 'BTCUSDT',
    }


def _seed(broker, coid, *, qty, state='submitted', exchange_order_id=None,
          extras=None) -> None:
    fields = dict(
        symbol=broker._market.symbol, side='buy', qty=qty, state=state,
        intent_key=coid, pine_entry_id='Long',
        extras={'kind': ENTRY_KIND_POSITION, 'order_type': 'market', **(extras or {})},
    )
    if exchange_order_id is not None:
        fields['exchange_order_id'] = exchange_order_id
    broker.store_ctx.upsert_order(coid, **fields)


def _recover(broker) -> None:
    asyncio.run(broker._recover_in_flight_submissions())


def _live_coids(broker) -> set:
    return {r.client_order_id for r in broker.store_ctx.iter_live_orders()}


def _parked_coids(store_ctx) -> set:
    """The engine parks persisted in ``pending_verifications`` (replay view)."""
    return set(store_ctx.replay()[1])


# === Verdict: confirmed ====================================================

def __test_recover_confirmed_resting_order__(tmp_path):
    # A LIMIT/working entry that DID land and rests in the open set: the
    # orderLinkId match confirms it, records the order_id alias, keeps it live.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        realtime_by_coid={'c1': _order(order_id='o1', coid='c1', status='New')},
        open_orders=[_order(order_id='o1', coid='c1', status='New')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c1')
    assert row.state == 'confirmed'
    assert row.exchange_order_id == 'o1'
    assert row.filled_qty == 0.0
    assert 'c1' in _live_coids(broker)
    assert broker.store_ctx.find_by_ref('order_id', 'o1').client_order_id == 'c1'


def __test_recover_confirmed_partial_fill_seeds_cursor_and_dedup__(tmp_path):
    # A partially filled entry: the fill cursor is adopted (wire domain) and
    # the execId de-dup is seeded so a reconnect replay is not double-counted.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        realtime_by_coid={
            'c4': _order(order_id='o4', coid='c4', status='PartiallyFilled',
                         cum='0.004'),
        },
        executions=[_exec(exec_id='e1', order_id='o4'),
                    _exec(exec_id='e2', order_id='o4')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c4', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c4')
    assert row.state == 'confirmed'
    assert row.filled_qty == 0.004
    assert 'c4' in _live_coids(broker)
    assert {'e1', 'e2'} <= broker._seen_exec_ids


def __test_recover_confirmed_fill_left_pending_when_exec_read_fails__(tmp_path):
    # cumExecQty > 0 but the execution read fails: confirming would advance
    # the cursor with no de-dup anchor, so the row stays PARKED instead.
    from pynecore_bybit.exceptions import BybitConnectionError

    class _NoExecFake(_RecoveryFake):
        def __call__(self, endpoint, params=None, *, method='get', body=None,
                     auth=False):
            if endpoint == '/v5/execution/list':
                raise BybitConnectionError("execution list down")
            return super().__call__(endpoint, params, method=method, body=body,
                                    auth=auth)

    broker = _NoExecFake(
        market=_linear_instrument(),
        realtime_by_coid={
            'c9': _order(order_id='o9', coid='c9', status='PartiallyFilled',
                         cum='0.004'),
        },
    )
    _open(tmp_path, broker)
    _seed(broker, 'c9', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c9')
    assert row.state == 'submitted'          # left parked
    assert row.filled_qty == 0.0             # cursor NOT advanced
    assert 'c9' in _live_coids(broker)


# === Verdict: rejected =====================================================

def __test_recover_rejected_terminal_retires_row__(tmp_path):
    # A clean reject with zero fills found only in history: retire the row.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        history_by_coid={'c2': _order(order_id='o2', coid='c2', status='Rejected')},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c2', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c2')
    assert row.state == 'rejected'
    assert 'c2' not in _live_coids(broker)   # closed


def __test_recover_cancelled_with_fill_confirms_not_rejects__(tmp_path):
    # PartiallyFilledCanceled is a DEAD status but carries a real fill — the
    # residual is gone, the fill is live, so it confirms rather than rejects.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        history_by_coid={
            'c5': _order(order_id='o5', coid='c5',
                         status='PartiallyFilledCanceled', cum='0.006'),
        },
        executions=[_exec(exec_id='e5', order_id='o5')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c5', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c5')
    assert row.state == 'confirmed'
    assert row.filled_qty == 0.006
    assert 'c5' in _live_coids(broker)


# === Verdict: still-unknown ================================================

def __test_recover_not_found_leaves_row_parked__(tmp_path):
    # No order under this orderLinkId in realtime OR history (never landed, or
    # aged out of retention, or a transport miss) — leave parked, never retire.
    broker = _RecoveryFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed(broker, 'c3', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('c3')
    assert row.state == 'submitted'          # unchanged
    assert 'c3' in _live_coids(broker)
    assert broker.store_ctx.find_by_ref('order_id', 'anything') is None


# === Orphan retirement =====================================================

def __test_orphan_retired_when_gone_and_flat__(tmp_path):
    # A confirmed row whose order is not resting and the symbol is flat: the
    # counterpart is gone (operator-closed offline) -> retire + clear anchor.
    broker = _RecoveryFake(market=_linear_instrument())  # open_orders empty, flat
    _open(tmp_path, broker)
    _seed(broker, 'g1', qty=0.01, state='confirmed', exchange_order_id='og1')
    _recover(broker)
    assert 'g1' not in _live_coids(broker)


def __test_orphan_skipped_for_promoted_coid__(tmp_path):
    # A pending row confirmed to a filled (non-resting) order in this same
    # pass: even though its id is absent from the open set and the book is
    # flat, the promoted-coid skip keeps recovery from retiring it.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        realtime_by_coid={
            'g2': _order(order_id='og2', coid='g2', status='Filled', cum='0.01'),
        },
        executions=[_exec(exec_id='eg2', order_id='og2')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'g2', qty=0.01)
    _recover(broker)
    row = broker.store_ctx.get_order('g2')
    assert row.state == 'confirmed'
    assert 'g2' in _live_coids(broker)       # NOT retired


def __test_orphan_skipped_without_broker_handle__(tmp_path):
    # A confirmed row that never recorded any broker handle cannot be proven
    # an orphan — leave it for the runtime reconcile.
    broker = _RecoveryFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed(broker, 'g3', qty=0.01, state='confirmed')  # no exchange_order_id
    _recover(broker)
    assert 'g3' in _live_coids(broker)


def __test_orphan_skipped_while_exposure_present__(tmp_path):
    # Any live position blocks the whole orphan pass: a Bybit position carries
    # no per-order handle, so a shed (filled) entry cannot be told apart from a
    # cancel by the id alone while exposure exists.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        positions=[{'symbol': 'BTCUSDT', 'positionIdx': 0, 'size': '0.01',
                    'side': 'Buy', 'avgPrice': '30000'}],
    )
    _open(tmp_path, broker)
    _seed(broker, 'g4', qty=0.01, state='confirmed', exchange_order_id='og4')
    _recover(broker)
    assert 'g4' in _live_coids(broker)       # exposure present -> pass skipped


# === Inverse anchor-correct recovery =======================================

def __test_recover_inverse_keeps_wire_cursor_and_anchor__(tmp_path):
    # Inverse: the row qty / cumExecQty / filled cursor are all in the WIRE
    # (contract) domain — no base conversion — and the persisted anchor is
    # preserved and re-seeded into the in-memory map so future fills convert
    # at the same rate.
    broker = _RecoveryFake(
        market=_inverse_instrument(),
        realtime_by_coid={
            'c8': _order(order_id='o8', coid='c8', status='PartiallyFilled',
                         cum='200', qty='500'),
        },
        executions=[{'execId': 'e8', 'orderId': 'o8', 'execType': 'Trade',
                     'symbol': 'BTCUSD'}],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c8', qty=500.0, extras={'anchor': '30000'})
    _recover(broker)
    row = broker.store_ctx.get_order('c8')
    assert row.state == 'confirmed'
    assert row.filled_qty == 200.0           # contracts, NOT converted to base
    assert row.extras.get('anchor') == '30000'   # anchor preserved
    assert broker._wire_anchor['c8'] == Decimal('30000')
    assert 'e8' in broker._seen_exec_ids


# === Disposition-unknown park close-out (engine park lifecycle) =============
#
# A dispatch the plugin parked as OrderDispositionUnknownError leaves TWO
# persisted artefacts: the order row (state ``disposition_unknown``, written by
# the execution mix-in) AND the engine's ``pending_verifications`` park row
# (written by ``OrderSyncEngine._park_pending`` -> ``record_park``). Recovery
# resolves the order row; these tests assert it also closes out the ENGINE park
# in every verdict path, so a resolved dispatch is not replayed forever by the
# post-restart ``_verify_pending_dispatches`` and an unresolved one survives for
# the next restart's verdict.


def __test_recover_confirm_clears_engine_park__(tmp_path):
    # A resting order confirms the parked row: the confirm path must drop the
    # engine park (record_unpark) so _verify_pending_dispatches stops matching
    # an already-resolved dispatch.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        realtime_by_coid={'c1': _order(order_id='o1', coid='c1', status='New')},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01, state='disposition_unknown')
    broker.store_ctx.record_park('c1', 'c1')   # engine park (key == intent_key)
    _recover(broker)
    assert broker.store_ctx.get_order('c1').state == 'confirmed'
    assert 'c1' not in _parked_coids(broker.store_ctx)


def __test_recover_reject_clears_engine_park__(tmp_path):
    # A found dead order retires the parked row: apply_reconcile_outcome closes
    # it and record_complete(intent_key) deletes the engine park in the same
    # step. Nothing must be left parked.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        history_by_coid={'c2': _order(order_id='o2', coid='c2', status='Rejected')},
    )
    _open(tmp_path, broker)
    _seed(broker, 'c2', qty=0.01, state='disposition_unknown')
    broker.store_ctx.record_park('c2', 'c2')
    _recover(broker)
    assert broker.store_ctx.get_order('c2').state == 'rejected'
    assert 'c2' not in _live_coids(broker)
    assert 'c2' not in _parked_coids(broker.store_ctx)


def __test_recover_still_unknown_keeps_engine_park__(tmp_path):
    # Not found in realtime OR history: the row stays parked, and the engine
    # park is PRESERVED so the post-restart _verify_pending_dispatches (and the
    # next restart's recovery) re-examine it — an unresolved dispatch must never
    # be silently un-parked.
    broker = _RecoveryFake(market=_linear_instrument())
    _open(tmp_path, broker)
    _seed(broker, 'c3', qty=0.01, state='disposition_unknown')
    broker.store_ctx.record_park('c3', 'c3')
    _recover(broker)
    assert broker.store_ctx.get_order('c3').state == 'disposition_unknown'
    assert 'c3' in _parked_coids(broker.store_ctx)


def __test_recover_exec_read_failure_keeps_park_for_next_restart__(tmp_path):
    # cumExecQty > 0 but the execution seed read fails: the row is left parked
    # (no fill cursor advanced without a de-dup anchor) AND the engine park is
    # preserved, so the NEXT restart re-runs the verdict — the row never wedges.
    # In-session the WS execution push applies the fills once (recovery touched
    # nothing) and the fill-path drop clears the park (see F5 report).
    from pynecore_bybit.exceptions import BybitConnectionError

    class _NoExecFake(_RecoveryFake):
        def __call__(self, endpoint, params=None, *, method='get', body=None,
                     auth=False):
            if endpoint == '/v5/execution/list':
                raise BybitConnectionError("execution list down")
            return super().__call__(endpoint, params, method=method, body=body,
                                    auth=auth)

    broker = _NoExecFake(
        market=_linear_instrument(),
        realtime_by_coid={
            'c9': _order(order_id='o9', coid='c9', status='PartiallyFilled',
                         cum='0.004'),
        },
    )
    _open(tmp_path, broker)
    _seed(broker, 'c9', qty=0.01, state='disposition_unknown')
    broker.store_ctx.record_park('c9', 'c9')
    _recover(broker)
    row = broker.store_ctx.get_order('c9')
    assert row.state == 'disposition_unknown'   # left parked
    assert row.filled_qty == 0.0                # cursor NOT advanced
    assert 'c9' in _parked_coids(broker.store_ctx)   # park survives for restart


# === Cancel-tentative / partial-leg rehydrate contract ======================


def __test_exit_leg_persist_is_orthogonal_to_engine_partial_leg_replay__(tmp_path):
    # The core cancel-tentative / native-failsafe rehydrate
    # (_rehydrate_*_from_replayed_legs) reads ENGINE-owned partial-bracket leg
    # rows (state 'partial_bracket_leg', written by SoftwarePartialBracketEngine
    # and replayed via iter_active_engine_trigger_partial_legs). The Bybit
    # plugin's own broker exit-leg rows (_persist_leg_row -> _confirm_row, state
    # 'submitted'/'confirmed') are a DIFFERENT subsystem. This test proves they
    # are orthogonal across a restart: the Bybit exit-leg row round-trips with
    # every field its own readers (_resolve_identity, recovery, residual
    # enumeration) need, while the engine's partial-leg replay reader sees ONLY
    # the engine legs and never misclassifies the broker exit leg.
    broker = _RecoveryFake(market=_linear_instrument())
    market = broker._market
    store = BrokerStore(tmp_path / "broker.sqlite", plugin_name=broker.plugin_name)
    identity = RunIdentity(
        strategy_id="recov", symbol=market.symbol, timeframe="1",
        account_id="recov-account",
    )
    broker.store_ctx = store.open_run(identity, script_source="// recov")

    # (a) A Bybit broker exit-leg row, persisted-first then confirmed exactly
    #     as the live _place_exit_leg path does.
    leg_extras = {'kind': 'exit_leg', 'leg': 'sl', 'exit_id': 'X1'}
    broker._persist_leg_row(
        'e_sl', market=market, side='sell', qty=Decimal('0.01'),
        intent_key='exit:X1:Long', from_entry='Long', sl_level=29000.0,
        extras=leg_extras,
    )
    broker._confirm_row('e_sl', 'oexit', leg_extras)

    # (b) Two ENGINE-owned partial-bracket legs — the rows the core rehydrate
    #     actually consumes: one armed SL (native-failsafe input) and one
    #     cancel-tentative (cancel-tentative rehydrate input, carrying the
    #     persisted stale-grace anchor).
    create_engine_trigger_partial_leg_row(
        broker.store_ctx, coid='eng_sl', symbol=market.symbol, side='sell',
        qty=0.01, intent_key='exit:X1:Long', pine_entry_id='X1',
        from_entry='Long', leg_kind=LEG_KIND_SL_PARTIAL,
        leg_state=LEG_STATE_ARMED, parent_pine_entry_id='Long',
        parent_entry_dispatch_ref='c_entry', intent_partial_qty=0.01,
        trigger_level=29000.0,
    )
    create_engine_trigger_partial_leg_row(
        broker.store_ctx, coid='eng_ct', symbol=market.symbol, side='sell',
        qty=0.01, intent_key='exit:X2:Long', pine_entry_id='X2',
        from_entry='Long', leg_kind=LEG_KIND_SL_PARTIAL,
        leg_state=LEG_STATE_ARMED, parent_pine_entry_id='Long',
        parent_entry_dispatch_ref='c_entry2', intent_partial_qty=0.01,
        trigger_level=28000.0,
    )
    # The cancel-tentative state is reached through a transition (the live
    # path's mark), not at creation; anchor the persisted stale-grace deadline.
    update_engine_trigger_partial_leg_state(
        broker.store_ctx, coid='eng_ct',
        new_leg_state=LEG_STATE_CANCEL_TENTATIVE,
        extras_patch={EXTRAS_KEY_CANCEL_TENTATIVE_SINCE_TS_MS: 1_700_000_000_000},
    )

    # Simulate restart: end this run instance and re-open on the same logical
    # identity — adoption re-points the orphan live rows to the fresh instance.
    broker.store_ctx.close()
    ctx2 = store.open_run(identity, script_source="// recov")

    # The Bybit exit-leg row survives with everything its readers rely on.
    leg = ctx2.get_order('e_sl')
    assert leg is not None
    assert leg.state == 'confirmed'
    assert leg.exchange_order_id == 'oexit'
    assert leg.from_entry == 'Long'
    assert (leg.extras or {}).get('kind') == 'exit_leg'
    assert (leg.extras or {}).get('leg') == 'sl'
    assert (leg.extras or {}).get('exit_id') == 'X1'

    # The engine partial-leg replay reader — the source that feeds the core
    # rehydrate's iter_legs() — sees ONLY the two engine legs. The Bybit broker
    # exit-leg row (state 'confirmed') is filtered out by the state guard, so it
    # can never corrupt the partial-bracket / cancel-tentative ledger.
    replayed = {r.client_order_id for r in iter_active_engine_trigger_partial_legs(ctx2)}
    assert replayed == {'eng_sl', 'eng_ct'}
    ct = ctx2.get_order('eng_ct')
    assert (ct.extras or {}).get(EXTRAS_KEY_CANCEL_TENTATIVE_SINCE_TS_MS) \
        == 1_700_000_000_000


# === Startup self-heal: orphaned intent envelopes ==========================
#
# A prior instance's premature flat sweep closed a fully-filled entry row via
# close_order WITHOUT the matching record_complete, orphaning its envelope.
# The closed row is NOT adopted into the new instance (only open rows carry
# over), so the startup orphan pass — which walks iter_live_orders — never
# sees it, and the stale envelope would let a re-entry rebuild the SAME spent
# orderLinkId. Startup clears such envelopes on a conclusively flat symbol.


def __test_startup_clears_orphaned_envelope_when_flat__(tmp_path):
    # Envelope with no surviving order row, symbol flat -> cleared, so the
    # next dispatch of the same Pine id mints a fresh coid.
    broker = _RecoveryFake(market=_linear_instrument())          # positions=[] -> flat
    _open(tmp_path, broker)
    broker.store_ctx.record_envelope('Long', 1_784_292_600_000, 0)
    assert 'Long' in broker.store_ctx.replay()[0]
    _recover(broker)
    assert 'Long' not in broker.store_ctx.replay()[0]            # self-healed


def __test_startup_keeps_envelope_with_live_row__(tmp_path):
    # An envelope that still owns a live (adopted) order row is preserved —
    # only genuinely orphaned envelopes are cleared.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        realtime_by_coid={'c1': _order(order_id='o1', coid='c1', status='New')},
        open_orders=[_order(order_id='o1', coid='c1', status='New')],
    )
    _open(tmp_path, broker)
    _seed(broker, 'c1', qty=0.01)                                # intent_key == 'c1'
    broker.store_ctx.record_envelope('c1', 1_784_292_600_000, 0)
    _recover(broker)
    assert 'c1' in broker.store_ctx.replay()[0]                  # preserved
    assert 'c1' in _live_coids(broker)


def __test_startup_keeps_orphaned_envelope_while_exposure__(tmp_path):
    # Live venue exposure blocks the self-heal (conservative, like the orphan
    # pass): a stale-looking envelope is NOT cleared while a position is open.
    broker = _RecoveryFake(
        market=_linear_instrument(),
        positions=[{'symbol': 'BTCUSDT', 'positionIdx': 0, 'size': '0.01',
                    'side': 'Buy', 'avgPrice': '30000'}],
    )
    _open(tmp_path, broker)
    broker.store_ctx.record_envelope('Long', 1_784_292_600_000, 0)
    _recover(broker)
    assert 'Long' in broker.store_ctx.replay()[0]                # exposure -> kept
