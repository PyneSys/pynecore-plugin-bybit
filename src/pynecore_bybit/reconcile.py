"""Runtime disappearance detection for the Bybit plugin (deriv + spot legs).

The private PUSH stream (``events.py``) is the primary order-event source;
:meth:`_RecoveryMixin._recover_in_flight_submissions` resolves the rows a
crash left pending at STARTUP. This mix-in is the RUNTIME counterpart of
that startup orphan pass: it drives the venue-agnostic
:class:`~pynecore.core.broker.disappearance.DisappearanceTracker` from the
reconcile cadence piggybacked on ``watch_orders``, catching bot-owned
orders (and settled positions) that vanish behind the engine's back — a
manual cancel on the Bybit UI, a broker-side liquidation, a silent
external flatten.

Two presence namespaces are tracked, each keyed so an order id can never
collide with a position ref:

* ``orders`` — the resting / working order rows (entry limits + STOP
  entries, the software TP / SL exit legs, market close orders) keyed by
  their exchange ``orderId``. The present set is the live id set from
  ``GET /v5/order/realtime``; a row whose id has left the open set is
  stamped, and the grace-expiry confirmation reads the deterministic
  ``orderLinkId`` lookup (``/v5/order/realtime`` then ``/v5/order/history``)
  to classify it — FILLED (the false stamp is cleared, the fill stays with
  the PUSH / catch-up path), CANCELLED (a found dead order with zero fills,
  retired with the dual signal + policy) or INCONCLUSIVE (transport failure
  or a not-found row, from which a cancel is never concluded).
* ``positions`` — the settled position rows (a fully-filled entry) keyed by
  the chart symbol, on the derivative categories only (spot settled
  exposure is owned by the ``SpotInventoryManager`` balance invariant, not
  duplicated here). The present set is ``{symbol}`` while
  ``GET /v5/position/list`` carries exposure; a vanished position is
  re-verified against a fresh position read and retired as a natural CLOSE.
  The eager ``_close_entry_rows_when_flat`` sweep is the fast teardown for
  the normal case; this tracker is the grace-gated backstop for a stale
  last-known-size cache (a missed ``position`` push).

Bybit's ``orderLinkId`` echo makes the confirmation deterministic (no
heuristic, unlike the cTrader deal-history bridge). The reconcile pass
swallows transient errors so a failing snapshot never tears down the PUSH
stream, but RE-RAISES :class:`~pynecore.core.broker.exceptions.BrokerManualInterventionError`
(the tracker's unexpected-cancel signal of a ``stop`` policy whose quarantine
hook was missing or failed) so the engine latches the external-intervention
quarantine it carries; the process keeps running.
"""
import asyncio
import logging
from abc import ABC
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.disappearance import (
    MISSING_PENDING_EXTRA,
    DisappearanceTracker,
    MissingConfirmation,
    MissingResolution,
)
from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_REJECTED,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.parked_dispatch import ParkedOutcome, ParkedVerdict
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    PENDING_DISPATCH_STATES,
)
from pynecore.core.plugin import override

from ._base import _BybitBase
from .events import _CANCEL_TYPE_REASONS
from .exceptions import BybitError, traceback_wanted
from .execution import _DEAD_ORDER_STATUSES
from .helpers import (
    CATEGORY_SPOT,
    ENTRY_CANCEL_CUM_EXEC_EXTRA,
    ENTRY_EXECUTION_HOLD_EXTRA,
    ENTRY_REMAINDER_CANCELLED_EXTRA,
    ENTRY_TERMINAL_EVENT_OWED_EXTRA,
    EXECUTION_CURSOR_OVERLAP_MS,
    FILL_RECOVERY_DONE_EVENT,
    FILL_RECOVERY_TARGET_EVENT,
    OWED_TERMINAL_EVENT_READY_EVENT,
    venue_filled_qty,
)
from .recovery import open_orders_by_id
from .state import _ended_cum_exec_qty

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: Disappearance grace window (seconds). A bot-owned row stamped
#: ``missing_pending_since`` (gone from its tracked namespace) is only acted
#: on once this window elapses without the row reappearing. Deliberately
#: DECOUPLED from the reconcile cadence (:data:`RECONCILE_CADENCE_S`): the
#: cadence is how often the snapshot is read, the grace is how long to wait
#: before concluding a cancel — wide enough to absorb the snapshot-vs-PUSH
#: skew (a fill in flight can flicker out of every snapshot for one pass).
_MISSING_PENDING_GRACE_S = 25.0

#: Small float slack when comparing a row's cumulative fill against its
#: dispatched size (both exact decimal strings; the slack only absorbs the
#: float round-trip). Matches ``events.py``'s ``_FILL_EPS``.
_FILL_EPS = 1e-9

#: ``orderStatus`` values that mean the order is still live on the venue: a
#: row whose ``orderLinkId`` lookup returns one of these has reappeared, so
#: the stamp is a snapshot artifact to clear, not a disappearance.
_LIVE_ORDER_STATUSES = frozenset({
    'New', 'PartiallyFilled', 'Untriggered', 'Triggered',
})

#: Age (seconds since the persist-first row was written) a
#: ``disposition_unknown`` dispatch must reach before the parked-dispatch
#: resolver starts looking it up by ``orderLinkId``. Covers the venue's own
#: indexing lag of a POST that DID land moments before its reply was lost.
_PARKED_DISPATCH_GRACE_S = 30.0


def _decimal_or_none(value: object) -> Decimal | None:
    """A finite, non-negative journaled quantity, or ``None`` when unusable."""
    if not isinstance(value, (str, int, float)) or isinstance(value, bool):
        return None
    try:
        figure = Decimal(str(value))
    except InvalidOperation:
        return None
    if not figure.is_finite() or figure < 0:
        return None
    return figure

#: Age (milliseconds since the persist-first row was written) a confirmed
#: derivative MARKET entry with an unbooked remainder must reach before its
#: order truth is looked up (:meth:`_ReconcileMixin._note_unbooked_market_entries`).
#: By then the private stream and the windowed backfill's overlap re-read
#: have both had their chance; only a fill neither delivered costs a lookup.
_MARKET_ENTRY_LOOKUP_GRACE_MS = EXECUTION_CURSOR_OVERLAP_MS

#: Consecutive conclusive not-found lookups (one per reconcile pass) after
#: which a parked dispatch is judged never to have reached the venue.
_PARKED_DISPATCH_MISSES_TO_REJECT = 3


class _ReconcileMixin(_BybitBase, ABC):
    """Runtime disappearance detection over the core tracker."""

    def _disappearance_tracker(self) -> DisappearanceTracker:
        """The lazily-built core disappearance tracker for this instance.

        Built on first use because its inputs — ``store_ctx``,
        ``on_unexpected_cancel``, ``quarantine_sink`` — are injected by the
        runner / CLI after ``__init__``. Venue wiring:

        * ``tracked_refs`` maps resting / working order rows to the
          ``orders`` namespace (by exchange ``orderId``) and settled
          derivative position rows to the ``positions`` namespace (by
          symbol); every other row is left untracked.
        * ``confirm_missing`` re-verifies a grace-expired stamp against a
          fresh venue read — the deterministic ``orderLinkId`` lookup for
          an order, a fresh position read for a settled position — and
          never concludes a cancel from a transport failure or a
          not-found row.
        * ``is_exempt`` skips rows flagged ``natural_close_at`` (a known
          expected close), so the grace window never raises a false
          unexpected-cancel for them.
        * ``cancelled_event_factory`` builds the plugin's leg-typed
          cancelled event; ``sibling_coids`` retires the settled siblings
          sharing a CLOSED netting position.
        * ``retire_cancelled_row`` retires a confirmed cancel the way the
          live ``order`` push does, so a partially filled entry keeps
          owning the exposure it left behind.
        """
        tracker = self._disappearance
        if tracker is None:
            assert self.store_ctx is not None
            tracker = DisappearanceTracker(
                self.store_ctx,
                grace_s=_MISSING_PENDING_GRACE_S,
                policy=self.on_unexpected_cancel,
                tracked_refs=self._tracked_refs,
                confirm_missing=self._confirm_missing,
                is_exempt=self._is_exempt,
                request_quarantine=self.quarantine_sink,
                sibling_coids=self._closed_position_siblings,
                cancelled_event_factory=self._cancelled_event,
                retire_cancelled_row=self._retire_vanished_cancelled_row,
            )
            self._disappearance = tracker
        return tracker

    # --- presence tracking ------------------------------------------------------

    def _tracked_refs(self, row: 'OrderRow') -> set[tuple[str, str]]:
        """Map a live row to its ``{(namespace, ref)}`` presence set.

        A resting / working order is tracked in ``orders`` by its exchange
        ``orderId``; a settled derivative position row (a fully-filled
        entry) in ``positions`` by symbol. A fleeting unfilled MARKET entry
        and any row with no broker handle are left untracked (an empty set
        exempts the row).

        An order whose execution the engine still owes its book
        (:meth:`~pynecore.core.broker.storage.RunContext.held_execution_order_ids`)
        is never tracked: the engine reads it back from the venue, and its
        absence from the venue is no intervention.
        """
        extras = row.extras or {}
        kind = extras.get('kind')
        ref = row.exchange_order_id or extras.get('order_id')
        if (ref and self.store_ctx is not None
                and str(ref) in self.store_ctx.held_execution_order_ids()):
            return set()
        if kind in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
            if row.filled_qty >= row.qty - _FILL_EPS:
                # Settled position — tracked by symbol on the derivatives
                # only; spot settled exposure is the inventory ledger's.
                market = self._market
                if market is not None and market.category != CATEGORY_SPOT:
                    return {('positions', row.symbol)}
                return set()
            if extras.get(ENTRY_EXECUTION_HOLD_EXTRA):
                # The order ended on the venue; the engine books what it
                # executed from the venue's history
                # (:meth:`~_RecoveryMixin._hand_over_unreported_execution`).
                return set()
            if extras.get(ENTRY_REMAINDER_CANCELLED_EXTRA):
                # Our own cancel retired the working remainder: nothing is
                # left to vanish. The cancel settlement
                # (:meth:`_settle_cancelled_entries`) and the per-order fill
                # recovery own the row until its cursor covers the executed
                # quantity, when it is tracked as a settled position.
                return set()
            if kind == ENTRY_KIND_WORKING:
                return {('orders', str(ref))} if ref else set()
            # Unfilled MARKET entry — fills near-instantly and leaves the
            # open set; stamping it would only churn until the confirm
            # bridge cleared it, so it is left to the PUSH / recovery path.
            return set()
        if kind in ('exit_leg', 'close'):
            return {('orders', str(ref))} if ref else set()
        return set()

    @staticmethod
    def _is_exempt(row: 'OrderRow') -> bool:
        """Exempt rows flagged as a known / expected close from the tracker."""
        return (row.extras or {}).get('natural_close_at') is not None

    @staticmethod
    def _is_settled_position(row: 'OrderRow') -> bool:
        """Whether ``row`` is a fully-filled entry (a settled position)."""
        return (
            (row.extras or {}).get('kind') in (ENTRY_KIND_POSITION,
                                               ENTRY_KIND_WORKING)
            and row.filled_qty >= row.qty - _FILL_EPS
        )

    # --- reconcile-cadence entry point ------------------------------------------

    async def _reconcile_disappearance(
            self, market: 'InstrumentInfo', position_rows: list[dict] | None,
    ) -> list[OrderEvent]:
        """Run one disappearance observation pass, returning recovered events.

        Reads the presence snapshot (open-orders id set + derivative
        position exposure), then drives the tracker's stamp / clear / grace
        protocol. A transient failure while reading or confirming is
        swallowed so the PUSH stream survives — the next pass retries — but
        the tracker's :class:`BrokerManualInterventionError` signal
        RE-RAISES so the engine latches the quarantine it carries. A no-op
        without persistence.

        :param market: The chart instrument.
        :param position_rows: The derivative position snapshot already read
            by the caller (reused for the ``positions`` present set),
            ``None`` when that read failed this pass.
        """
        if self.store_ctx is None:
            return []
        present = await self._disappearance_present(market, position_rows)
        tracker = self._disappearance_tracker()
        events: list[OrderEvent] = []
        try:
            async for event in tracker.observe(present, epoch_time()):
                events.append(event)
        except BrokerManualInterventionError:
            raise
        except Exception as exc:  # noqa: BLE001 - the reconcile pass must not kill the stream
            logger.warning(
                "Bybit disappearance reconcile pass failed (transient): %s",
                exc, exc_info=traceback_wanted(exc),
            )
        return events

    async def _resolve_parked_dispatches(self, market: 'InstrumentInfo') -> None:
        """Resolve the dispatches parked with an unknown disposition in-run.

        A POST whose reply was lost (timeout, gateway 5xx) leaves its
        persist-first row in ``disposition_unknown`` and the engine's
        dispatch parked. The core rule
        (:class:`~pynecore.core.broker.parked_dispatch.ParkedDispatchResolver`)
        releases the ones the venue never accepted; this pass runs it at the
        reconcile cadence and answers its question by ``orderLinkId``
        (:meth:`_confirm_lookup`):

        * found live — landed; the engine verification / the fill path
          unpark it.
        * found ended with fills (``Filled``, or dead with a positive
          executed quantity) — landed, and settled here
          (:meth:`~_ReconcileMixin._settle_landed_ended_dispatch`): the order is in no open
          set, and its push may be the one that was lost.
        * found dead with zero fills — the venue refused it; dead without a
          usable executed quantity (:func:`~pynecore_bybit.state._ended_cum_exec_qty`)
          — inconclusive, never a refusal.
        * not found with both endpoints read — absent.
        * transport failure — inconclusive.

        A rejected intent goes out again under the same ``orderLinkId``,
        which is safe: the venue never saw it and the persist-first write
        reopens the closed row. A row an earlier process left pending, which
        the startup recovery could not find, is looked up the same way; the
        engine does not hold its intent back, because Bybit refuses a second
        send under the same ``orderLinkId`` and the duplicate path adopts a
        first send that did land. A no-op without persistence.
        """
        async def lookup(row: 'OrderRow') -> ParkedVerdict:
            existing, conclusive = await self._confirm_lookup(market, row.client_order_id)
            if not conclusive:
                return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
            if existing is None:
                return ParkedVerdict(ParkedOutcome.ABSENT)
            status = str(existing.get('orderStatus') or '')
            order_id = str(existing.get('orderId') or '') or None
            if status in _DEAD_ORDER_STATUSES or status == 'Filled':
                cum_exec = _ended_cum_exec_qty(existing)
                if cum_exec is None:
                    # An ended order without a usable executed quantity
                    # proves no refusal: a rejected row would read as proof
                    # that the order never executed.
                    return ParkedVerdict(ParkedOutcome.INCONCLUSIVE)
                if status in _DEAD_ORDER_STATUSES and cum_exec <= _FILL_EPS:
                    return ParkedVerdict(ParkedOutcome.REFUSED, order_id=order_id,
                                         status=status)
                if order_id:
                    self._settle_landed_ended_dispatch(market, row, existing, cum_exec)
            return ParkedVerdict(ParkedOutcome.LANDED, order_id=order_id, status=status)

        try:
            await self._parked_resolver.resolve(self.store_ctx, lookup)
        except Exception as exc:  # noqa: BLE001 - the reconcile pass must not kill the stream
            logger.warning(
                "Bybit parked dispatch resolution pass failed (transient): %s",
                exc, exc_info=traceback_wanted(exc),
            )

    def _settle_landed_ended_dispatch(
            self, market: 'InstrumentInfo', row: 'OrderRow', existing: dict,
            cum_exec: float,
    ) -> None:
        """Settle a parked dispatch the venue shows ended with executions.

        The create reply was lost, and the order already ended (``Filled``,
        or dead with a positive ``cumExecQty``): it is in no open set, so
        neither the engine verification nor the disappearance pass ever looks
        at it, and its terminal push may have been lost too. The venue's
        order object is the proof it landed, so in one store transaction the
        row is confirmed under the venue ``orderId`` (the alias the fill
        paths resolve executions by), the engine park is dropped, the
        executions the cursor lacks are queued for the per-order fill
        recovery (:meth:`~_ReconcileMixin._note_unbooked_fills`, which books them through the
        event stream's gates, never from this figure), and a dead order is
        retired like its terminal push would have: a close row journals how
        it ended (:meth:`~_EventStreamMixin._journal_close_ending`), an entry
        row is normalized to the executed quantity
        (:meth:`~_EventStreamMixin._retire_cancelled_row`).

        :param market: The chart instrument.
        :param row: The parked row.
        :param existing: The venue order object found by ``orderLinkId``.
        :param cum_exec: Its validated ``cumExecQty``
            (:func:`~pynecore_bybit.state._ended_cum_exec_qty`), positive.
        """
        if self.store_ctx is None:
            return
        coid = row.client_order_id
        order_id = str(existing.get('orderId') or '')
        status = str(existing.get('orderStatus') or '')
        with self._journal_transaction():
            self.store_ctx.upsert_order(coid, state='confirmed', exchange_order_id=order_id)
            self.store_ctx.add_ref(coid, 'order_id', order_id)
            self.store_ctx.record_unpark(coid)
            confirmed = self.store_ctx.get_order(coid)
            if confirmed is not None and market.category != CATEGORY_SPOT:
                self._note_unbooked_fills(confirmed, order_id, Decimal(str(cum_exec)))
            if status in _DEAD_ORDER_STATUSES:
                self._retire_cancelled_row(coid, cum_exec, order_id, venue_status=status)

    async def _disappearance_present(
            self, market: 'InstrumentInfo', position_rows: list[dict] | None,
    ) -> dict[str, set[str] | None]:
        """Build the per-namespace present-ref sets for one pass.

        ``orders`` carries the live exchange order ids (``None`` when the
        open-orders read failed — an incomplete snapshot must never look
        like a complete absence); ``positions`` carries ``{symbol}`` while
        the derivative position snapshot shows exposure (``None`` on a
        failed read), and is omitted entirely on spot.
        """
        read_started_at = epoch_time()
        order_rows, orders_ok = await self._open_order_rows(market)
        open_orders = open_orders_by_id(order_rows)
        if orders_ok:
            # The private push is the fast path of the guard; this read
            # judges the guarded orders whose edit push was lost or skipped.
            self._observe_open_order_levels(order_rows, market,
                                            read_started_at=read_started_at)
        present: dict[str, set[str] | None] = {
            'orders': set(open_orders) if orders_ok else None,
        }
        if market.category != CATEGORY_SPOT:
            self._note_open_order_fill_progress(open_orders)
            if position_rows is None:
                present['positions'] = None
            else:
                try:
                    present['positions'] = (
                        {market.symbol} if self._rows_have_exposure(position_rows)
                        else set()
                    )
                except BybitError:
                    present['positions'] = None
        return present

    def _note_open_order_fill_progress(self, open_orders: dict[str, Decimal]) -> None:
        """Note the live rows whose resting order carries unbooked fills.

        A partially filled resting order stays in the open set, so no
        disappearance confirmation ever looks at it; its ``cumExecQty`` in
        the open-orders snapshot is the evidence instead.

        :param open_orders: The open-orders snapshot, ``orderId`` →
            ``cumExecQty`` (wire domain).
        """
        if self.store_ctx is None or not open_orders:
            return
        for row in list(self.store_ctx.iter_live_orders()):
            order_id = row.exchange_order_id or ''
            cum_exec = open_orders.get(order_id)
            if cum_exec is not None:
                self._note_unbooked_fills(row, order_id, cum_exec)

    def _note_unbooked_fills(
            self, row: 'OrderRow', order_id: str, cum_exec: Decimal,
    ) -> None:
        """Queue a derivative row for the per-order fill recovery.

        The venue's ``cumExecQty`` is durable evidence that executions exist;
        a row whose ``filled_qty`` cursor stays below it has a fill the
        stream dropped and the time-windowed backfill did not reach. The
        reconcile cadence reads that order's executions by ``orderId`` until
        the cursor covers the figure (:meth:`_recover_unbooked_order_fills`).
        Spot stays out: its fills recover through the inventory ledger.

        The target is journaled (:data:`FILL_RECOVERY_TARGET_EVENT`) before
        it is queued, so a caller that retires the row in the same store
        transaction never closes it without the durable obligation, and a
        failed write leaves both the queue and the row as they were. A target
        already queued unchanged is not written again.
        """
        if not order_id or venue_filled_qty(row) >= float(cum_exec) - _FILL_EPS:
            return
        coid = row.client_order_id
        target = (order_id, cum_exec)
        if self._fill_recovery_target(coid) == target:
            return
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                FILL_RECOVERY_TARGET_EVENT, client_order_id=coid,
                exchange_order_id=order_id, payload={'cum_exec_qty': str(cum_exec)},
            )
        self._publish_fill_recovery(coid, target)

    def _clear_fill_recovery(self, coid: str) -> None:
        """Drop a per-order fill recovery target once its executions are accounted.

        The done record (:data:`FILL_RECOVERY_DONE_EVENT`) is written first,
        so a failed write keeps the target queued for the next pass.

        :param coid: The row's client order id.
        """
        target = self._fill_recovery_target(coid)
        if target is not None:
            self._journal_fill_recovery_done(coid, target[0], target[1])
        self._publish_fill_recovery(coid, None)

    @contextmanager
    def _journal_transaction(self) -> Iterator[None]:
        """Open (or join) a store transaction that also covers the recovery queue.

        Every plugin write span goes through here. The per-order fill recovery
        queue (:attr:`~_BybitBase._unbooked_fill_orders`) mirrors journal
        records (:data:`FILL_RECOVERY_TARGET_EVENT`,
        :data:`FILL_RECOVERY_DONE_EVENT`); a queue change made inside the span
        is staged and applied only when the outermost span commits, so a
        rolled back span never leaves a queued target without its durable
        record (a retry would skip the record as already queued) or drops a
        target whose done record was discarded.
        """
        assert self.store_ctx is not None
        outermost = self._fill_recovery_staged is None
        if outermost:
            self._fill_recovery_staged = {}
        try:
            with self.store_ctx.transaction():
                yield
        except BaseException:
            if outermost:
                self._fill_recovery_staged = None
            raise
        if outermost:
            staged = self._fill_recovery_staged or {}
            self._fill_recovery_staged = None
            for coid, target in staged.items():
                if target is None:
                    self._unbooked_fill_orders.pop(coid, None)
                else:
                    self._unbooked_fill_orders[coid] = target

    def _fill_recovery_target(self, coid: str) -> tuple[str, Decimal] | None:
        """The recovery target queued for ``coid``, a change staged in the open span included."""
        staged = self._fill_recovery_staged
        if staged is not None and coid in staged:
            return staged[coid]
        return self._unbooked_fill_orders.get(coid)

    def _publish_fill_recovery(self, coid: str, target: tuple[str, Decimal] | None) -> None:
        """Queue (``target``) or drop (``None``) a recovery target, on commit inside a span.

        Outside a :meth:`_journal_transaction` span the journal write before
        this call has committed already, so the change applies at once.
        """
        staged = self._fill_recovery_staged
        if staged is not None:
            staged[coid] = target
        elif target is None:
            self._unbooked_fill_orders.pop(coid, None)
        else:
            self._unbooked_fill_orders[coid] = target

    def _journal_fill_recovery_done(
            self, coid: str, order_id: str, cum_exec: Decimal, **payload: object,
    ) -> None:
        """Write the done record of one per-order fill recovery target."""
        if self.store_ctx is None:
            return
        self.store_ctx.log_event(
            FILL_RECOVERY_DONE_EVENT, client_order_id=coid, exchange_order_id=order_id,
            payload={'cum_exec_qty': str(cum_exec), **payload},
        )

    def _durable_fill_recovery_targets(self) -> dict[str, tuple[str, Decimal]]:
        """The per-order fill recovery targets earlier processes of the run left open.

        The newest target of each client order id counts; it is open unless
        a done record of the same order covers its ``cum_exec_qty``. A record
        without a client order id, an order id or a readable figure is
        skipped.

        :return: ``client_order_id`` -> ``(orderId, cumExecQty)``, wire domain.
        """
        if self.store_ctx is None:
            return {}
        targets: dict[str, tuple[str, Decimal]] = {}
        for _ik, coid, order_id, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(FILL_RECOVERY_TARGET_EVENT):
            figure = _decimal_or_none(payload.get('cum_exec_qty'))
            if coid and order_id and figure is not None:
                targets[coid] = (order_id, figure)
        if not targets:
            return {}
        for _ik, coid, order_id, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(FILL_RECOVERY_DONE_EVENT):
            target = targets.get(coid or '')
            figure = _decimal_or_none(payload.get('cum_exec_qty'))
            if (target is not None and figure is not None and order_id == target[0]
                    and figure >= target[1]):
                del targets[coid or '']
        return targets

    @override
    def fill_recovery_pending(self, entry_ids: Collection[str]) -> bool:
        """Whether a fill of the run that may explain a venue change is still owed.

        Pending while the disappearance tracker holds a stamp on a row of the
        entries (:meth:`BrokerPlugin.fill_recovery_pending`), while a close of
        the run (a one-way close or a hedge close leg, whose rows name no
        entry) is stamped missing and its venue order not confirmed yet, and
        while the per-order recovery holds venue proof of executions it has
        not booked (:attr:`_unbooked_fill_orders`): the run's own reduce is
        the likeliest cause of a position the venue shows smaller.
        """
        if super().fill_recovery_pending(entry_ids):
            return True
        store = self.store_ctx
        if store is None or not entry_ids:
            return False
        if self._unbooked_fill_orders:
            return True
        return any(
            (row.extras or {}).get('kind') == 'close'
            and MISSING_PENDING_EXTRA in (row.extras or {})
            for row in store.iter_live_orders())

    async def _note_unbooked_market_entries(self, market: 'InstrumentInfo') -> None:
        """Queue the MARKET entries whose terminal order shows unbooked fills.

        A market entry leaves the open set at once and the disappearance
        pass never tracks it (:meth:`_tracked_refs`), so neither
        :meth:`_note_open_order_fill_progress` nor
        :meth:`_confirm_vanished_order` ever sees its ``cumExecQty``. A
        confirmed derivative market entry whose cursor still lags its size
        once :data:`_MARKET_ENTRY_LOOKUP_GRACE_MS` has passed (the stream and
        the windowed backfill had their chance) is looked up ONCE by
        ``orderLinkId``: a terminal order queues its unbooked fills through
        :meth:`_note_unbooked_fills`, a still-live one is re-checked next
        pass, a transport failure retries. A conclusive not-found ends the
        checks too — nothing proves a fill. Spot stays out (the inventory
        ledger owns its fills).
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return
        checked = self._market_entries_checked
        now_ms = int(epoch_time() * 1000)
        candidates: set[str] = set()
        for row in list(self.store_ctx.iter_live_orders()):
            if ((row.extras or {}).get('kind') != ENTRY_KIND_POSITION
                    or (row.extras or {}).get(ENTRY_REMAINDER_CANCELLED_EXTRA)
                    or row.state in PENDING_DISPATCH_STATES
                    or row.filled_qty >= row.qty - _FILL_EPS):
                continue
            coid = row.client_order_id
            candidates.add(coid)
            if (coid in checked or coid in self._unbooked_fill_orders
                    or now_ms - row.created_ts_ms < _MARKET_ENTRY_LOOKUP_GRACE_MS):
                continue
            existing, conclusive = await self._confirm_lookup(market, coid)
            if not conclusive:
                continue
            if existing is None:
                checked.add(coid)
                continue
            if str(existing.get('orderStatus') or '') in _LIVE_ORDER_STATUSES:
                continue
            ended = _ended_cum_exec_qty(existing)
            if ended is None:
                # The terminal order carries no usable executed quantity:
                # a zero would end the checks with its fills unproven.
                continue
            cum_exec = Decimal(str(ended))
            order_id = str(existing.get('orderId') or '') or row.exchange_order_id or ''
            self._note_unbooked_fills(row, order_id, cum_exec)
            checked.add(coid)
        checked.intersection_update(candidates)

    @override
    async def _settle_cancelled_entries(
            self, market: 'InstrumentInfo', events: list[OrderEvent],
    ) -> None:
        """Settle the remainder-cancelled entries whose executed quantity is unknown.

        A REST cancel ACK carries no fill figure, so the entry row stays
        live and unnormalized (:meth:`_retire_cancelled_row`) until the
        venue's terminal ``cumExecQty`` arrives. The private ``order`` push
        normally delivers it; this pass is the backstop for a dropped push
        and for a push without a usable figure: each such row is read once
        per reconcile pass by ``orderLinkId`` (:meth:`_confirm_lookup`), and
        a terminal order with a usable ``cumExecQty``
        (:func:`~pynecore_bybit.state._ended_cum_exec_qty`) settles it —
        closed on zero fills, otherwise normalized to the executed quantity
        with a lagging cursor queued for the per-order fill recovery. A
        still-live order (the cancel is not processed yet), a not-found
        order, an ended order without a usable figure and a transport
        failure are retried on the next pass.

        A row whose terminal event went to the core without the figure
        (:data:`ENTRY_TERMINAL_EVENT_OWED_EXTRA`) gets that event again, now
        with the venue's executed quantity. The settlement's transaction
        clears the mark and journals the event as ready
        (:data:`OWED_TERMINAL_EVENT_READY_EVENT`, with the venue's order
        object); the event is appended to ``events`` as soon as it commits,
        before the next lookup, so a later row's failure leaves it with the
        caller. The engine stores a delivered event before it queues it and
        replays it itself after a restart, but an event the earlier process
        never yielded (it stopped with the event still in the outbox) is
        still owed, and nothing else reads the settled row again: the first
        pass of the next process re-delivers the ready events of the entries
        the engine still holds (:meth:`_owed_terminal_events_still_held`).

        :param events: The pass's event list; the owed terminal events are
            appended to it.
        """
        if self.store_ctx is None:
            return
        if not self._owed_terminal_restored:
            for ready in self._owed_terminal_events_still_held():
                event = self._owed_terminal_event(market, ready)
                if event is not None:
                    events.append(event)
            self._owed_terminal_restored = True
        for row in list(self.store_ctx.iter_live_orders()):
            extras = row.extras or {}
            owed_mark = ENTRY_TERMINAL_EVENT_OWED_EXTRA in extras
            if (not extras.get(ENTRY_REMAINDER_CANCELLED_EXTRA)
                    or ENTRY_CANCEL_CUM_EXEC_EXTRA in extras
                    or (not owed_mark and row.filled_qty >= row.qty - _FILL_EPS)):
                continue
            existing, conclusive = await self._confirm_lookup(market, row.client_order_id)
            if not conclusive or existing is None:
                continue
            status = str(existing.get('orderStatus') or '')
            if status != 'Filled' and status not in _DEAD_ORDER_STATUSES:
                continue
            cum_exec = _ended_cum_exec_qty(existing)
            if cum_exec is None:
                continue
            with self._journal_transaction():
                payload = self._deliver_owed_terminal_event(
                    row.client_order_id, existing, None)
                self._retire_cancelled_row(
                    row.client_order_id, cum_exec,
                    str(existing.get('orderId') or '') or None,
                )
            if payload is None:
                continue
            event = self._owed_terminal_event(market, payload)
            if event is not None:
                events.append(event)

    def _owed_terminal_events_still_held(self) -> list[dict]:
        """The ready owed terminal events whose entry the engine still holds.

        The engine keeps an entry whose terminal event carried no executed
        quantity in a durable fill-evidence hold
        (:meth:`~pynecore.core.broker.storage.RunContext.fill_evidence_holds`:
        intent key -> the venue order ids it names). A restart re-arms it,
        and the hold is released only after the event that settles it was
        applied. A hold that names the order of a ready record (its venue
        order id, or the intent key of its journal row) is therefore an owed
        terminal event the engine has not applied. When the earlier process
        yielded it, the engine stored it on receipt and replays it itself,
        and the copy re-delivered here is only logged; when it never yielded
        it (or the engine's store write of the received event failed), this
        copy is the one that settles the hold. A released hold means the
        engine applied it, so the event is not re-delivered.

        :return: One ready record payload per order, in journal order.
        """
        if self.store_ctx is None:
            return []
        holds = self.store_ctx.fill_evidence_holds()
        if not holds:
            return []
        held_order_ids = {order_id for order_ids in holds.values() for order_id in order_ids}
        ready: dict[str, dict] = {}
        for _ik, _coid, _oid, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(OWED_TERMINAL_EVENT_READY_EVENT):
            coid = payload.get('coid')
            readback = payload.get('readback')
            if not isinstance(coid, str) or not coid or not isinstance(readback, dict):
                continue
            row = self.store_ctx.get_order_for_run_id(coid)
            if (str(readback.get('orderId') or '') in held_order_ids
                    or (row is not None and row.intent_key in holds)):
                ready[coid] = payload
        return list(ready.values())

    def _owed_terminal_event(self, market: 'InstrumentInfo', payload: dict) -> OrderEvent | None:
        """Build the owed terminal event of one ready record.

        :param payload: The :data:`OWED_TERMINAL_EVENT_READY_EVENT` payload.
        :return: The event, ``None`` when the record names no order.
        """
        readback = payload['readback']
        coid = str(payload.get('coid') or '')
        if not coid:
            return None
        identity = self._resolve_identity(coid, str(readback.get('orderId') or '') or None)
        if identity[2] is None:
            # A restart does not re-home the closed row: the identity the
            # settlement recorded names the order.
            stored = payload.get('identity') or [None, None, None]
            identity = (stored[0], stored[1],
                        LegType(stored[2]) if stored[2] is not None else None)
        return self._order_lifecycle_event(
            readback, str(payload.get('event_type') or 'cancelled'), coid, identity, market,
        )

    def _rows_have_exposure(self, position_rows: list[dict]) -> bool:
        """Whether any position row carries a non-zero size."""
        for row in position_rows:
            if self._position_row_size(row) > 0.0:
                return True
        return False

    # --- grace-expiry confirmation ----------------------------------------------

    async def _confirm_missing(self, row: 'OrderRow') -> MissingConfirmation:
        """Re-verify a grace-expired stamp against a fresh venue read.

        A settled derivative position row is confirmed against a fresh
        ``/v5/position/list`` read; every other tracked row against the
        deterministic ``orderLinkId`` lookup. Never concludes a cancel from
        a transport failure or a not-found row — those stay INCONCLUSIVE and
        the tracker keeps the stamp for a later pass.
        """
        market = await asyncio.to_thread(self._broker_market)
        if (market.category != CATEGORY_SPOT
                and self._is_settled_position(row)):
            return await self._confirm_settled_position(market)
        return await self._confirm_vanished_order(market, row)

    async def _confirm_settled_position(
            self, market: 'InstrumentInfo',
    ) -> MissingConfirmation:
        """Confirm a vanished settled position from a fresh position read.

        Exposure back → STILL_PRESENT (a snapshot artifact); a conclusive
        flat → CLOSED (a natural close — the position is gone, retire the
        row without a synthetic cancel or a fill event, the engine's
        position adoption owns the size side); a failed read → INCONCLUSIVE.
        """
        try:
            rows = await self._fetch_position_rows(market)
        except BybitError:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        try:
            if self._rows_have_exposure(rows):
                return MissingConfirmation(MissingResolution.STILL_PRESENT)
        except BybitError:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        return MissingConfirmation(
            MissingResolution.CLOSED, position_ref=market.symbol,
        )

    async def _confirm_vanished_order(
            self, market: 'InstrumentInfo', row: 'OrderRow',
    ) -> MissingConfirmation:
        """Confirm a vanished working / resting order from its ``orderLinkId``.

        * A found live order → STILL_PRESENT (the stamp was a snapshot
          artifact).
        * A found ``Filled`` order (or a not-yet-dead status carrying
          fills) → FILLED with NO fill slice: the false stamp is cleared and
          the row stays live for the PUSH / catch-up path to book the fill
          (this pass never books it, so a stream-gap fill is never
          double-applied). A derivative row whose cursor lags the venue's
          ``cumExecQty`` is queued for the per-order recovery, which books
          the missing executions by ``execId``.
        * A found dead order carrying fills (``PartiallyFilledCanceled``:
          the fill is real, the residual was cancelled) → CANCELLED, but
          only once the row's persisted cursor already covers the reported
          fills — the per-order recovery books the slice on the same cadence, and
          concluding earlier would retire the row with a stale cursor.
          Until then INCONCLUSIVE, so the stamp is kept and re-checked
          instead of looping a FILLED verdict forever (which would clear
          the stamp, never surface the residual cancel, and bypass the
          unexpected-cancel policy).
        * A found dead order with zero fills → CANCELLED (retired with the
          dual signal + policy).
        * Before either CANCELLED verdict a close row journals how its order
          ended (:meth:`~_EventStreamMixin._journal_close_ending`), which is
          what settles a close the core holds.
        * Either CANCELLED verdict carries the venue-driven reason its
          ``cancelType`` names (a reduce-only squeeze, an OCA sibling
          trigger, a cleared position): the push of a venue cancel can be
          lost too, and the history then tells it apart from an operator's
          cancel — the tracker retires the row without the policy. A
          ``Rejected`` order carries the venue-rejected reason: only the
          venue can reject.
        * A transport failure, a not-found row (never landed, or a dead
          order aged out of the < 24h history retention), or an ended order
          whose ``cumExecQty`` is missing, unparsable or a zero its
          status contradicts → INCONCLUSIVE: a
          cancel is only ever concluded from a FOUND dead order with its
          executed quantity.
        """
        existing, conclusive = await self._confirm_lookup(market, row.client_order_id)
        if not conclusive:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        if existing is None:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        status = str(existing.get('orderStatus') or '')
        if status in _LIVE_ORDER_STATUSES:
            return MissingConfirmation(MissingResolution.STILL_PRESENT)
        venue_cum_exec = _ended_cum_exec_qty(existing)
        if venue_cum_exec is None:
            # An ended order without a readable executed quantity, or with
            # a zero one its status contradicts (``Filled``,
            # ``PartiallyFilledCanceled``), proves neither a fill nor its
            # absence: the retirement would take the
            # row's cursor as the terminal figure and journal a zero as proof.
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        cum_exec = Decimal(str(venue_cum_exec))
        if market.category != CATEGORY_SPOT:
            self._note_unbooked_fills(row, str(existing.get('orderId') or ''), cum_exec)
        if status in _DEAD_ORDER_STATUSES:
            if cum_exec > 0 and venue_filled_qty(row) < float(cum_exec) - _FILL_EPS:
                # The fills are real but the row's cursor has not caught up
                # (the per-order recovery books them on this same cadence):
                # keep the stamp and re-check, never retire with a stale
                # cursor.
                return MissingConfirmation(MissingResolution.INCONCLUSIVE)
            # Zero-fill dead order, or the fills are fully booked: the
            # residual is a genuine cancellation — dual signal, plus the
            # policy unless the venue itself names the cause. A rejected
            # order is always the venue's verdict (the push path books it
            # as a reject, never as an intervention). A close row journals
            # the ending first: the push that would have carried it is the
            # one that went missing.
            if (row.extras or {}).get('kind') == 'close':
                self._journal_close_ending(
                    row.client_order_id, status, venue_cum_exec,
                )
            if status == 'Rejected':
                cancel_reason: str | None = CANCEL_REASON_VENUE_REJECTED
            else:
                cancel_reason = _CANCEL_TYPE_REASONS.get(
                    str(existing.get('cancelType') or ''))
            return MissingConfirmation(
                MissingResolution.CANCELLED, cancel_reason=cancel_reason,
            )
        if status == 'Filled' or cum_exec > 0:
            # The order filled: the disappearance premise is false. Clear
            # the stamp and leave the booking to the fill path.
            if status == 'Filled':
                self._settle_filled_row(row, float(cum_exec))
            return MissingConfirmation(MissingResolution.FILLED)
        return MissingConfirmation(MissingResolution.INCONCLUSIVE)

    def _settle_filled_row(self, row: 'OrderRow', cum_exec: float) -> None:
        """Settle a row whose venue order finished with every fill booked.

        The venue can finish an order below the dispatched size (a
        reduce-only order trimmed to the position left to reduce). The fill
        path settles that from the execution's ``orderQty``; a row whose
        fills were booked without it — adopted across a restart, say — would
        otherwise stay live forever, re-stamped and re-confirmed as filled
        on every pass (measured live: bybit-inverse cycle 225, a 165-contract
        close the venue finished at 164). An exit / close row is closed; an
        entry row keeps owning its exposure and drops only the remainder
        that can no longer fill.
        """
        if self.store_ctx is None or venue_filled_qty(row) < cum_exec - _FILL_EPS:
            return
        if (row.extras or {}).get('kind') in ('exit_leg', 'close'):
            self.store_ctx.close_order(row.client_order_id)
        elif row.qty > row.filled_qty + _FILL_EPS:
            self.store_ctx.upsert_order(row.client_order_id, qty=row.filled_qty)

    async def _confirm_lookup(
            self, market: 'InstrumentInfo', coid: str,
    ) -> tuple[dict | None, bool]:
        """Read one order by ``orderLinkId``, reporting read completeness.

        Unlike :meth:`_lookup_order_by_coid` (which conflates a transport
        failure with a clean not-found), this reports ``conclusive=False``
        the moment either endpoint read raises, so the confirmation never
        concludes a cancel from a truncated read.

        :return: ``(order, conclusive)`` — the found order object (open set
            first, then history) or ``None``; ``conclusive`` is ``False`` on
            a transport failure and ``True`` when both endpoints were read.
        """
        for endpoint in ('/v5/order/realtime', '/v5/order/history'):
            try:
                result = await self._call(endpoint, {
                    'category': market.category,
                    'symbol': market.symbol,
                    'orderLinkId': coid,
                }, auth=True)
            except BybitError:
                return None, False
            entries = result.get('list') or []
            if entries:
                if self._duplicate_original_is_spent(coid, entries[0]):
                    # A prior instance's order under the same id is not
                    # this row's order: nothing of ours exists under it.
                    return None, True
                return entries[0], True
        return None, True

    # --- dual-signal artefacts --------------------------------------------------

    def _cancelled_event(self, row: 'OrderRow', now_ts: float) -> OrderEvent:
        """Build the synthetic cancelled event for a vanished bot-owned row.

        Carries the row's Pine identity (leg type / entry ids resolved
        exactly as the live ``order`` PUSH path does), so the sync engine
        books the disappearance against the originating entry / bracket leg
        and re-syncs the strategy position. On inverse the wire-domain row
        quantities convert back to base through the dispatch anchor
        (:meth:`_inverse_order_to_base`), matching the PUSH path.
        """
        pine_id, from_entry, leg_type = self._resolve_identity(
            row.client_order_id, row.exchange_order_id,
        )
        lt = leg_type if leg_type is not None else LegType.ENTRY
        order = ExchangeOrder(
            id=row.exchange_order_id or '', symbol=row.symbol, side=row.side,
            order_type=(OrderType.LIMIT if lt is LegType.TAKE_PROFIT
                        else OrderType.STOP if lt is LegType.STOP_LOSS
                        else OrderType.MARKET),
            # The order's executed quantity, with what reducing fills netted
            # out of the row: the core keeps an entry's protection on it.
            qty=row.qty + venue_filled_qty(row) - row.filled_qty,
            filled_qty=venue_filled_qty(row),
            remaining_qty=max(0.0, row.qty - row.filled_qty),
            price=None, stop_price=None, average_fill_price=None,
            status=OrderStatus.CANCELLED, timestamp=now_ts, fee=0.0,
            fee_currency='', reduce_only=lt is not LegType.ENTRY,
            client_order_id=row.client_order_id,
        )
        market = self._market
        if market is not None and market.is_inverse:
            order = self._inverse_order_to_base(order)
        # The inverse anchor is keyed by the wire id; the core reads its own.
        order = replace(order, client_order_id=self._core_client_order_id(row.client_order_id))
        return OrderEvent(
            order=order, event_type='cancelled',
            fill_price=None, fill_qty=None, timestamp=now_ts,
            pine_id=pine_id, from_entry=from_entry, leg_type=lt,
        )

    def _retire_vanished_cancelled_row(self, row: 'OrderRow') -> None:
        """Retire a row the disappearance verdict confirmed cancelled.

        The same retirement the live ``order`` push applies
        (:meth:`_retire_cancelled_row`): exit / close legs and entries that
        never executed are closed, a partially filled entry is normalized to
        its executed quantity and stays live. A CANCELLED verdict is only
        given once the row's fill cursor covers the venue's ``cumExecQty``,
        so the cursor is the terminal executed figure.
        """
        self._retire_cancelled_row(
            row.client_order_id, venue_filled_qty(row), row.exchange_order_id,
        )

    def _closed_position_siblings(
            self, row: 'OrderRow', _confirmation: MissingConfirmation,
    ) -> list[str]:
        """Live settled siblings sharing a CLOSED netting position.

        A Bybit one-way account merges pyramid entries onto one net
        position per symbol, so a proven close retires every other settled
        entry row of the symbol in the same transaction — closing only the
        observed row would leave siblings live against a flat venue.
        """
        if self.store_ctx is None:
            return []
        return [
            other.client_order_id
            for other in self.store_ctx.iter_live_orders()
            if other.client_order_id != row.client_order_id
            and self._is_settled_position(other)
        ]
