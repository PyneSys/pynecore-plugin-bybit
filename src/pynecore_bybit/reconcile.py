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
(the tracker's halting-policy escalation) so the engine performs its
graceful stop.
"""
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.disappearance import (
    DisappearanceTracker,
    MissingConfirmation,
    MissingResolution,
)
from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
)

from ._base import _BybitBase
from .exceptions import BybitError
from .execution import _DEAD_ORDER_STATUSES
from .helpers import CATEGORY_SPOT

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


class _ReconcileMixin(_BybitBase):
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
        * ``cancel_siblings`` powers the ``stop_and_cancel`` policy's
          zero-fill sweep; ``cancelled_event_factory`` builds the plugin's
          leg-typed cancelled event; ``sibling_coids`` retires the settled
          siblings sharing a CLOSED netting position.
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
                cancel_siblings=self._cancel_sibling_working_orders,
                request_quarantine=self.quarantine_sink,
                sibling_coids=self._closed_position_siblings,
                cancelled_event_factory=self._cancelled_event,
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
        """
        extras = row.extras or {}
        kind = extras.get('kind')
        if kind in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
            if row.filled_qty >= row.qty - _FILL_EPS:
                # Settled position — tracked by symbol on the derivatives
                # only; spot settled exposure is the inventory ledger's.
                market = self._market
                if market is not None and market.category != CATEGORY_SPOT:
                    return {('positions', row.symbol)}
                return set()
            if kind == ENTRY_KIND_WORKING:
                ref = row.exchange_order_id or extras.get('order_id')
                return {('orders', str(ref))} if ref else set()
            # Unfilled MARKET entry — fills near-instantly and leaves the
            # open set; stamping it would only churn until the confirm
            # bridge cleared it, so it is left to the PUSH / recovery path.
            return set()
        if kind in ('exit_leg', 'close'):
            ref = row.exchange_order_id or extras.get('order_id')
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
        a :class:`BrokerManualInterventionError` from a halting policy
        RE-RAISES so the engine performs its graceful stop. A no-op without
        persistence.

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
                exc, exc_info=True,
            )
        return events

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
        order_ids, orders_ok = await self._recovery_open_order_ids(market)
        present: dict[str, set[str] | None] = {
            'orders': order_ids if orders_ok else None,
        }
        if market.category != CATEGORY_SPOT:
            if position_rows is None:
                present['positions'] = None
            else:
                present['positions'] = (
                    {market.symbol} if self._rows_have_exposure(position_rows)
                    else set()
                )
        return present

    @staticmethod
    def _rows_have_exposure(position_rows: list[dict]) -> bool:
        """Whether any position row carries a non-zero size."""
        for row in position_rows:
            try:
                if float(row.get('size') or 0.0) > 0.0:
                    return True
            except (TypeError, ValueError):
                continue
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
        if self._rows_have_exposure(rows):
            return MissingConfirmation(MissingResolution.STILL_PRESENT)
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
          double-applied).
        * A found dead order carrying fills (``PartiallyFilledCanceled``:
          the fill is real, the residual was cancelled) → CANCELLED, but
          only once the row's persisted cursor already covers the reported
          fills — the F4 backfill books the slice on the same cadence, and
          concluding earlier would retire the row with a stale cursor.
          Until then INCONCLUSIVE, so the stamp is kept and re-checked
          instead of looping a FILLED verdict forever (which would clear
          the stamp, never surface the residual cancel, and bypass the
          unexpected-cancel policy).
        * A found dead order with zero fills → CANCELLED (retired with the
          dual signal + policy).
        * A transport failure, or a not-found row (never landed, or a dead
          order aged out of the < 24h history retention) → INCONCLUSIVE: a
          cancel is only ever concluded from a FOUND dead order.
        """
        existing, conclusive = await self._confirm_lookup(market, row.client_order_id)
        if not conclusive:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        if existing is None:
            return MissingConfirmation(MissingResolution.INCONCLUSIVE)
        status = str(existing.get('orderStatus') or '')
        if status in _LIVE_ORDER_STATUSES:
            return MissingConfirmation(MissingResolution.STILL_PRESENT)
        try:
            cum_exec = Decimal(str(existing.get('cumExecQty') or '0') or '0')
        except (InvalidOperation, TypeError, ValueError):
            cum_exec = Decimal(0)
        if status in _DEAD_ORDER_STATUSES:
            if cum_exec > 0 and row.filled_qty < float(cum_exec) - _FILL_EPS:
                # The fills are real but the row's cursor has not caught up
                # (the backfill books them on this same cadence): keep the
                # stamp and re-check, never retire with a stale cursor.
                return MissingConfirmation(MissingResolution.INCONCLUSIVE)
            # Zero-fill dead order, or the fills are fully booked: the
            # residual is a genuine cancellation — dual signal + policy.
            return MissingConfirmation(MissingResolution.CANCELLED)
        if status == 'Filled' or cum_exec > 0:
            # The order filled: the disappearance premise is false. Clear
            # the stamp and leave the booking to the fill path.
            return MissingConfirmation(MissingResolution.FILLED)
        return MissingConfirmation(MissingResolution.INCONCLUSIVE)

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
            qty=row.qty, filled_qty=row.filled_qty,
            remaining_qty=max(0.0, row.qty - row.filled_qty),
            price=None, stop_price=None, average_fill_price=None,
            status=OrderStatus.CANCELLED, timestamp=now_ts, fee=0.0,
            fee_currency='', reduce_only=lt is not LegType.ENTRY,
            client_order_id=row.client_order_id,
        )
        market = self._market
        if market is not None and market.is_inverse:
            order = self._inverse_order_to_base(order)
        return OrderEvent(
            order=order, event_type='cancelled',
            fill_price=None, fill_qty=None, timestamp=now_ts,
            pine_id=pine_id, from_entry=from_entry, leg_type=lt,
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
            for other in self.store_ctx.iter_live_orders(symbol=row.symbol)
            if other.client_order_id != row.client_order_id
            and self._is_settled_position(other)
        ]

    async def _cancel_sibling_working_orders(self, row: 'OrderRow') -> None:
        """Best-effort zero-fill sibling cancel sweep (``stop_and_cancel``).

        Cancels every OTHER bot-owned resting order in the origin row's
        symbol and retires its store row, so the halt does not strand the
        remaining working orders. Rows with live filled exposure (a settled
        position, or a partial fill whose residual is still working) are
        left for the operator — retiring their tracking row would strand
        real broker exposure. A sibling whose cancel disposition is
        ambiguous (already-filled / unknown) is kept live so a surfaced fill
        can still book against it. Per-order failures are swallowed — this
        runs while the quarantine / halt is already armed.
        """
        if self.store_ctx is None:
            return
        market = await asyncio.to_thread(self._broker_market)
        for other in list(self.store_ctx.iter_live_orders(symbol=row.symbol)):
            if other.client_order_id == row.client_order_id:
                continue
            extras = other.extras or {}
            if not (other.exchange_order_id or extras.get('order_id')):
                continue
            if (extras.get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                    and other.filled_qty > _FILL_EPS):
                # Real broker exposure — never cancel-and-retire it.
                continue
            try:
                outcome = await self._cancel_outcome_for(
                    market, other.client_order_id,
                )
            except BybitError:
                # Ambiguous disposition (mapped reject / transport) — leave
                # the row live for the reconcile to resolve.
                continue
            if outcome is not CancelDispositionOutcome.CANCEL_CONFIRMED:
                # ALREADY_FILLED / UNKNOWN: the order may have filled or its
                # cancel is unconfirmed — keep the row live.
                continue
            # ``_cancel_outcome_for`` already closed the row on a confirmed
            # cancel; record the cascade for the forensic audit.
            self.store_ctx.log_event(
                'unexpected_cancel_cascade',
                client_order_id=other.client_order_id,
                exchange_order_id=other.exchange_order_id,
                payload={'origin_coid': row.client_order_id},
            )
