"""Persist-first crash recovery + startup-orphan retirement for Bybit.

Runs once inside :meth:`_ensure_broker_started`, after the category startup
(spot inventory / derivative position-mode probe) and BEFORE the engine's
startup reconcile (whose first ``get_position`` adopts the broker net
position). Resolves every persist-first dispatch row a crash left pending
between the wire-send and its post-ack confirm — the rows the execution
mix-in writes BEFORE the wire call, in states ``submitted`` /
``disposition_unknown`` / ``server_ref_seen``.

Deterministic reconcile-recovery, NOT the fuzzy ``DispatchJournal``
``recover_pending`` resume. Bybit echoes ``orderLinkId`` verbatim on every
order object and rejects a duplicate id, so a pending row's outcome is read
deterministically from two authoritative REST sources (the same
:meth:`_lookup_order_by_coid` the forward dispatch path already uses):

* ``GET /v5/order/realtime?orderLinkId=`` — the open set: an order still
  resting (or freshly filled but not yet shed) carries the row's
  ``orderLinkId`` → confirmed;
* ``GET /v5/order/history?orderLinkId=`` — the terminal set for an order the
  realtime endpoint has shed (filled, cancelled, rejected). The
  ``orderLinkId`` lookup bypasses the history time window entirely (measured
  on the global demo), so no per-order since-anchor is needed for the match.

The cTrader ``recovery.py`` is the STRUCTURAL pattern — the deterministic
coid-echo match (Bybit ``orderLinkId`` ⇔ cTrader ``clientOrderId``), the
"a live position alone NEVER confirms a row" rule, the ``execId`` de-dup
seed (⇔ cTrader ``dealId``), and the ``promoted_coids`` orphan skip. It is
NOT the Capital.com ``ResumeOutcome`` journal path: with a direct per-coid
lookup there is no ambiguity to resolve through a hook protocol, so the
verdicts are written straight onto the store (terminal retirements route
through :meth:`DispatchJournal.apply_reconcile_outcome` for the canonical
``recovered_in_flight_terminal`` audit event + row close, matching cTrader).

Resolution rules:

* **Live-position rule.** A Bybit position object carries no order handle,
  so a live position alone NEVER confirms a pending row — only the
  ``orderLinkId`` match on the order object does. The position snapshot is
  used ONLY by the orphan pass (to prove a confirmed row's counterpart is
  gone), never to confirm a pending row.
* **Never re-issue.** A row whose ``orderLinkId`` is not found in either
  endpoint stays pending (``still_unknown``); recovery never dispatches a
  fresh order. An empty lookup is a retention-safe non-verdict: a
  Cancelled / Rejected order can age out of the history window after 24h
  (documented retention), so "not found" is treated as indeterminate, never
  as a definitive reject — only a *found* dead order (with zero fills)
  retires the row.
* **Fill de-dup seed.** A confirmed row that already carries fills seeds the
  shared :attr:`_seen_exec_ids` from ``/v5/execution/list`` BEFORE advancing
  the fill cursor, so a private-stream reconnect that replays the same
  ``execId`` is not double-applied once ``watch_orders`` opens. When that
  seed cannot be read (transport failure), the row is left pending rather
  than confirmed — advancing ``filled_qty`` with no de-dup anchor would let
  the reconnect replay double-count the fill.
* **Wire domain.** The BrokerStore row ``qty`` / ``filled_qty`` and the
  order object's ``cumExecQty`` are all in the WIRE domain (base units on
  spot/linear, whole USD contracts on inverse), so the fill cursor is
  adopted contract-for-contract with NO conversion. The persisted
  ``extras['anchor']`` (written before the wire send) is preserved so the
  event stream keeps converting future fills back to base at the SAME rate;
  it is also re-seeded into the in-memory anchor map for fast resolution.
* **No fill event.** Recovery emits NO OrderEvent — it runs before the
  engine's startup adoption, which folds the broker net position. The
  ``execId`` de-dup seed keeps the post-open reconnect replay from
  double-counting.
"""
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from time import time as epoch_time

from pynecore.core.broker.journal import DispatchJournal, ReconcileOutcome
from pynecore.core.broker.store_helpers import find_pending_dispatch

from ._base import _BybitBase
from .exceptions import BybitError
from .execution import _DEAD_ORDER_STATUSES
from .helpers import (
    CATEGORY_SPOT,
    EXECUTION_PAGE_LIMIT,
    EXECUTION_SINCE_SKEW_MS,
    EXECUTION_WINDOW_MS,
    OPEN_ORDERS_PAGE_LIMIT,
)
from .models import InstrumentInfo

logger = logging.getLogger(__name__)


class _RecoveryMixin(_BybitBase):
    """Persist-first crash recovery + startup-orphan retirement."""

    async def _recover_in_flight_submissions(self) -> None:
        """Resolve every pending persist-first dispatch row at startup.

        Walks the BrokerStore's
        :data:`~pynecore.core.broker.store_helpers.PENDING_DISPATCH_STATES`
        rows, resolves each against the deterministic ``orderLinkId``
        lookup, then runs the orphan retirement against the venue's
        open-orders + position snapshots. Category-agnostic: spot and
        derivative pending rows go through the same path (the spot
        inventory ledger's own startup stays independent). A no-op when
        persistence is off (the test / one-shot paths) or there is nothing
        to recover.

        Never raises out of this pass for a recoverable condition — a
        transport failure while reading a snapshot leaves the affected rows
        parked and the orphan pass skipped, so an ambiguous restart keeps
        running rather than halting the bot; genuine auth failures
        propagate as usual through the underlying REST reads.
        """
        if self.store_ctx is None:
            return
        pending = list(find_pending_dispatch(self.store_ctx))
        has_live = next(iter(self.store_ctx.iter_live_orders()), None) is not None
        # A replayed envelope with no live/pending row is a self-heal target
        # for the crashed-sweep legacy case (:meth:`_retire_orphaned_envelopes`),
        # so it too must keep the recovery pass alive.
        has_envelopes = bool(self.store_ctx.replay()[0])
        if not pending and not has_live and not has_envelopes:
            return
        market = await asyncio.to_thread(self._broker_market)
        promoted_coids: set[str] = set()
        for row in pending:
            if await self._recover_one_pending_row(row, market):
                promoted_coids.add(row.client_order_id)
        # Orphan retirement needs both venue snapshots to be conclusive: a
        # truncated read cannot prove a row's counterpart is gone, so a
        # failed snapshot skips the pass (the runtime reconcile retries).
        open_order_ids, orders_ok = await self._recovery_open_order_ids(market)
        has_exposure, exposure_ok = await self._recovery_has_exposure(market)
        if orders_ok and exposure_ok:
            self._retire_startup_orphans(open_order_ids, has_exposure, promoted_coids)
            self._retire_orphaned_envelopes(has_exposure)

    async def _recover_one_pending_row(
            self, row, market: InstrumentInfo,
    ) -> bool:
        """Resolve one pending row from the deterministic ``orderLinkId`` lookup.

        :return: ``True`` when the row was promoted to ``confirmed`` (so the
            orphan pass skips it); ``False`` for a terminally-retired or
            still-unknown row.
        """
        if self.store_ctx is None:
            return False
        coid = row.client_order_id
        existing = await self._lookup_order_by_coid(coid)
        if existing is None:
            # Not found in realtime OR history — either the create never
            # reached the venue (crash after the persist-first write, before
            # the wire send) or the lookup transport failed, or a terminal
            # order aged out of the history window. All three are
            # indeterminate: leave the row parked (still_unknown). Recovery
            # never re-dispatches, and never retires a row on an empty
            # lookup — a definitive reject requires FINDING a dead order.
            return False
        status = str(existing.get('orderStatus') or '')
        try:
            cum_exec = Decimal(str(existing.get('cumExecQty') or '0') or '0')
        except (InvalidOperation, TypeError, ValueError):
            cum_exec = Decimal(0)
        if status in _DEAD_ORDER_STATUSES and cum_exec <= 0:
            # A clean reject / cancel / deactivation with zero fills: the
            # order died without becoming exposure. Retire it terminally.
            self._retire_recovered_rejected(row, existing)
            return False
        # The order landed (New / Untriggered / Triggered / PartiallyFilled /
        # Filled), or a terminal status that carries fills
        # (PartiallyFilledCanceled: the fill is real, the residual is gone).
        # Confirm — adopt the broker order id, seed the fill cursor + de-dup,
        # and unpark the parked verification. Emits no event.
        return await self._confirm_recovered_row(row, market, existing, cum_exec)

    async def _confirm_recovered_row(
            self, row, market: InstrumentInfo, existing: dict, cum_exec: Decimal,
    ) -> bool:
        """Promote a pending row to ``confirmed`` from the broker order truth.

        Records the broker ``orderId`` alias, advances ``filled_qty`` (wire
        domain, monotone-clamped into the row's own size), preserves the
        persisted conversion anchor, and drops the parked verification. When
        the order already carries fills, the shared ``execId`` de-dup is
        seeded from ``/v5/execution/list`` FIRST so a private-stream
        reconnect replay of the same executions is not double-applied; if
        that seed cannot be read the row stays pending instead (no fill
        cursor is advanced without its de-dup anchor).
        """
        if self.store_ctx is None:
            return False
        coid = row.client_order_id
        order_id = str(existing.get('orderId') or '')
        if cum_exec > 0:
            from_ms = row.created_ts_ms - EXECUTION_SINCE_SKEW_MS
            exec_ids, _exec_qty, seeded = await self._recovery_fill_ids(
                market, order_id, from_ms,
            )
            if not seeded or not exec_ids:
                # The order reports fills but the execution read failed —
                # or drained clean while returning NO executions (an
                # inconsistent venue read: fills exist, their ids do not).
                # Advancing ``filled_qty`` on either would let a replay of
                # these same executions pass the de-dup gate and
                # double-count. Leave the row parked: the private stream
                # re-applies the fills once (recovery touched nothing), and
                # a later restart / verification resolves the dispatch.
                return False
            self._seen_exec_ids.update(exec_ids)
        extras = dict(row.extras or {})
        # The wire-domain fill cursor is the order's cumulative executed
        # quantity, clamped into the row's own size (never above ``qty``).
        filled = min(row.qty, max(row.filled_qty, float(cum_exec)))
        self.store_ctx.upsert_order(
            coid, state='confirmed', exchange_order_id=order_id,
            filled_qty=filled, extras=extras,
        )
        if order_id:
            self.store_ctx.add_ref(coid, 'order_id', order_id)
        self.store_ctx.record_unpark(coid)
        # Keep the inverse base<->contract anchor resolvable in-memory so the
        # event stream converts future fills at the SAME rate the dispatch
        # pinned; ``_inverse_anchor_for`` also reads it from the row extras,
        # so this only saves the store round-trip.
        anchor_raw = extras.get('anchor')
        if anchor_raw:
            try:
                self._wire_anchor[coid] = Decimal(str(anchor_raw))
            except (InvalidOperation, TypeError, ValueError):
                pass
        self.store_ctx.log_event(
            'recovered_in_flight_confirmed', client_order_id=coid,
            exchange_order_id=order_id, intent_key=row.intent_key,
            payload={'filled_qty': filled,
                     'order_status': str(existing.get('orderStatus') or ''),
                     'kind': extras.get('kind')},
        )
        return True

    def _retire_recovered_rejected(self, row, existing: dict) -> None:
        """Land a rejected / cancelled recovered row in ``rejected`` + retire it.

        Routes through :meth:`DispatchJournal.apply_reconcile_outcome` (the
        cTrader terminal-close pattern) so the row is closed from
        ``iter_live_orders`` and the canonical ``recovered_in_flight_terminal``
        audit event is written in one step, then deletes the envelope anchor
        so the next dispatch of the same Pine intent mints a fresh
        ``orderLinkId`` instead of reusing a spent one.
        """
        if self.store_ctx is None:
            return
        order_id = str(existing.get('orderId') or '')
        DispatchJournal(self.store_ctx).apply_reconcile_outcome(
            row.client_order_id,
            ReconcileOutcome(
                kind='terminal_close',
                reason='recovered_in_flight_terminal',
                new_state='rejected',
                audit_event='recovered_in_flight_rejected',
                close_row=True,
                audit_payload={'order_id': order_id,
                               'order_status': str(existing.get('orderStatus') or '')},
                exchange_order_id=order_id or None,
            ),
        )
        self._clear_intent_anchor(row)

    def _clear_intent_anchor(self, row) -> None:
        """Delete the envelope + parked verifications of a terminally-retired row.

        ``close_order`` leaves the ``envelopes`` anchor and any parked
        ``pending_verifications`` for ``row.intent_key`` behind. Without this
        delete a restart's ``replay()`` re-surfaces the stale anchor and the
        next dispatch of the same Pine intent rebuilds the SAME
        ``orderLinkId`` (same ``bar_ts_ms``) onto the just-closed row;
        ``upsert_order`` then updates the closed row without clearing
        ``closed_ts_ms``, hiding the fresh entry from ``iter_live_orders``.
        ``record_complete`` deletes both in one transaction.
        """
        if self.store_ctx is None or not row.intent_key:
            return
        self.store_ctx.record_complete(row.intent_key)

    # --- venue snapshots (read directly, never via the re-entrant reads) -------

    async def _recovery_open_order_ids(
            self, market: InstrumentInfo,
    ) -> tuple[set[str], bool]:
        """Page ``/v5/order/realtime`` for the symbol's live order ids.

        Read directly (not through :meth:`get_open_orders`) so recovery does
        not re-enter :meth:`_ensure_broker_started` mid-startup. ``conclusive``
        is ``False`` on any transport failure — a truncated read must never
        let the orphan pass conclude a row's counterpart is absent.
        """
        ids: set[str] = set()
        cursor: str | None = None
        try:
            while True:
                result = await self._call('/v5/order/realtime', {
                    'category': market.category,
                    'symbol': market.symbol,
                    'limit': OPEN_ORDERS_PAGE_LIMIT,
                    'cursor': cursor,
                }, auth=True)
                for entry in result.get('list') or []:
                    oid = str(entry.get('orderId') or '')
                    if oid:
                        ids.add(oid)
                cursor = result.get('nextPageCursor') or None
                if not cursor:
                    break
        except BybitError:
            logger.warning(
                "Bybit recovery: open-orders snapshot read failed; "
                "skipping the startup orphan pass", exc_info=True,
            )
            return ids, False
        return ids, True

    async def _recovery_has_exposure(
            self, market: InstrumentInfo,
    ) -> tuple[bool, bool]:
        """Whether the symbol currently carries live exposure.

        Derivatives read the venue ``/v5/position/list`` snapshot (any
        non-zero leg is exposure); spot reads the core inventory ledger's
        net base. ``conclusive`` is ``False`` on a failed read (or a missing
        spot ledger), so the orphan pass is skipped rather than retiring a
        row against an unproven flat.
        """
        if market.category != CATEGORY_SPOT:
            try:
                rows = await self._fetch_position_rows(market)
            except BybitError:
                logger.warning(
                    "Bybit recovery: position snapshot read failed; "
                    "skipping the startup orphan pass", exc_info=True,
                )
                return False, False
            for row in rows:
                try:
                    if float(row.get('size') or 0.0) > 0.0:
                        return True, True
                except (TypeError, ValueError):
                    continue
            return False, True
        manager = self._spot_manager
        if manager is None:
            # No ledger to consult (persistence-off spot never reaches here,
            # but a quarantine that never built the manager might) — cannot
            # prove flat, so leave the orphan pass out.
            return False, False
        return manager.fold.net_base > 0, True

    async def _recovery_fill_ids(
            self, market: InstrumentInfo, order_id: str, from_ms: int,
            until_ms: int | None = None,
    ) -> tuple[set[str], float, bool]:
        """Collect one order's fills for the de-dup seed + the fill cursor.

        Filtered by ``orderId``, walking contiguous 7-day windows (the
        endpoint's span cap) from ``startTime = dispatch-start − skew`` all
        the way to the present — a single window would silently truncate the
        seed for a GTC order that filled more than 7 days after its
        creation, and an incomplete seed lets the durable backfill replay
        those executions past the de-dup gate. Returns the ``execId`` set,
        the summed wire-domain ``execQty`` of those executions (base on
        linear, whole USD contracts on inverse — the ids and the quantity
        come from the SAME read, so a cursor advanced by this sum is always
        covered by the seeded de-dup), and ``seeded``, which is ``False`` on
        any transport failure so the caller leaves the row pending rather
        than advancing a fill cursor with no de-dup anchor.

        ``until_ms`` clamps the walk's end to at least that VENUE-clocked
        timestamp: the adoption baseline passes its floor so a lagging
        local clock can never truncate the seed below the floor (an
        execution missed there would be permanently unreachable — the
        backfill starts at the floor).
        """
        ids: set[str] = set()
        qty_sum = 0.0
        if not order_id:
            return ids, qty_sum, False
        now_ms = int(epoch_time() * 1000)
        if until_ms is not None and until_ms > now_ms:
            now_ms = until_ms
        start = from_ms
        try:
            while start < now_ms:
                end = min(start + EXECUTION_WINDOW_MS, now_ms)
                cursor: str | None = None
                while True:
                    result = await self._call('/v5/execution/list', {
                        'category': market.category,
                        'symbol': market.symbol,
                        'orderId': order_id,
                        'startTime': start,
                        'endTime': end,
                        'execType': 'Trade',
                        'limit': EXECUTION_PAGE_LIMIT,
                        'cursor': cursor,
                    }, auth=True)
                    for entry in result.get('list') or []:
                        eid = str(entry.get('execId') or '')
                        if not eid or eid in ids:
                            continue
                        ids.add(eid)
                        try:
                            qty_sum += float(entry.get('execQty') or 0.0)
                        except (TypeError, ValueError):
                            pass
                    cursor = result.get('nextPageCursor') or None
                    if not cursor:
                        break
                start = end
        except BybitError:
            logger.warning(
                "Bybit recovery: execution read failed for order %s; "
                "leaving the row parked", order_id, exc_info=True,
            )
            return ids, qty_sum, False
        return ids, qty_sum, True

    # --- orphan retirement ------------------------------------------------------

    def _retire_startup_orphans(
            self, open_order_ids: set[str], has_exposure: bool,
            promoted_coids: set[str],
    ) -> None:
        """Retire live rows whose venue counterpart is definitively gone.

        A ``confirmed`` / ``closing`` row is an orphan when its broker order
        id is absent from the open-orders snapshot AND the symbol carries no
        live exposure: the bot was stopped, then the order filled-and-closed
        or was cancelled manually outside it. Close the row and delete its
        envelope anchor, so the runtime reconcile does not later stamp a
        missing-pending marker and halt a clean restart.

        Guards (the cTrader orphan-pass rules):

        * ``promoted_coids`` — rows this recovery just confirmed are skipped;
          a freshly recovered fill's position may not be in these snapshots.
        * A row with NO broker handle at all cannot be proven an orphan —
          leave it for the runtime reconcile.
        * Any live exposure blocks the pass: a Bybit position object carries
          no per-order handle, so a filled MARKET entry (shed from the open
          set) is indistinguishable from a cancel by the order id alone — a
          live position is the only signal that such a row's fill landed,
          and it must NOT be retired while exposure exists. When exposure is
          present the whole pass is a no-op (conservative; the runtime
          reconcile resolves the rest).
        """
        if self.store_ctx is None or has_exposure:
            return
        retired = 0
        for row in list(self.store_ctx.iter_live_orders()):
            if row.client_order_id in promoted_coids:
                continue
            if row.state not in ('confirmed', 'closing'):
                continue
            order_id = (row.extras or {}).get('order_id')
            ref = row.exchange_order_id
            handle = ref or (str(order_id) if order_id else None)
            if handle is None:
                # No broker handle — cannot prove it is an orphan.
                continue
            if handle in open_order_ids:
                # Still resting on the exchange.
                continue
            self.store_ctx.log_event(
                'startup_orphan_retired', client_order_id=row.client_order_id,
                exchange_order_id=ref,
                payload={'state': row.state, 'order_id': order_id},
            )
            self.store_ctx.close_order(row.client_order_id)
            if row.intent_key:
                self.store_ctx.record_complete(row.intent_key)
            retired += 1
        if retired:
            logger.info(
                "Bybit startup: retired %d orphan order row(s) — no matching "
                "order on the exchange and the symbol is flat", retired,
            )

    def _retire_orphaned_envelopes(self, has_exposure: bool) -> None:
        """Clear intent envelopes with no surviving order row on a flat symbol.

        Self-heals a store damaged by a prior instance's premature flat sweep
        (it closed a fully-filled entry row via ``close_order`` WITHOUT the
        matching ``record_complete``, orphaning the envelope). The already-
        closed row is NOT adopted into this instance — order adoption carries
        over only ``closed_ts_ms IS NULL`` rows — so the orphan pass above,
        which walks :meth:`iter_live_orders`, never sees it and the stale
        envelope survives; a re-entry of the same Pine id would then rebuild
        the SAME spent ``orderLinkId`` from it.

        Runs only on a conclusively flat symbol (``has_exposure`` False — the
        caller already gated on the read being conclusive). A replayed
        envelope with no surviving order row can only be one of two safe
        cases: a fully-filled-and-swept entry whose position is gone, or an
        intent whose order never reached the wire (a crash before the persist-
        first row write) — clearing either merely makes the next dispatch mint
        a fresh id. Any envelope that still owns a live or pending row (an
        adopted position, a resting order, a parked dispatch) keeps its
        ``intent_key`` in ``live_keys`` and is preserved, so a genuinely
        in-flight intent is never cleared.
        """
        if self.store_ctx is None or has_exposure:
            return
        envelopes, _pending = self.store_ctx.replay()
        if not envelopes:
            return
        live_keys = {
            row.intent_key
            for row in self.store_ctx.iter_live_orders()
            if row.intent_key
        }
        cleared = 0
        for key in envelopes:
            if key in live_keys:
                continue
            self.store_ctx.log_event(
                'startup_stale_envelope_cleared', intent_key=key,
            )
            self.store_ctx.record_complete(key)
            cleared += 1
        if cleared:
            logger.info(
                "Bybit startup: cleared %d stale intent envelope(s) with no "
                "surviving order row on a flat symbol", cleared,
            )
