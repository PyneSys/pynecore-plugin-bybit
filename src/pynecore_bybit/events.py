"""Live order-event stream mix-in for the Bybit plugin (spot M2, linear M3,
inverse M4).

Implements :meth:`watch_orders` (``watch_orders = NATIVE``) over the private
WebSocket stream (``/v5/private``): the ``execution`` topic drives fills,
the ``order`` topic the created / cancelled / rejected transitions, both
translated into :class:`~pynecore.core.broker.models.OrderEvent` objects
with Pine identity reverse-mapped from the dispatch bookkeeping.

Division of labour between the two topics — deliberate, not redundant:

- **Fills come ONLY from ``execution``.** Its rows are per-execution
  slices with a stable ``execId``, exactly the incremental ``fill_qty`` +
  idempotency key the engine's duplicate-fill gate wants. Every fill is
  booked into the core spot-inventory ledger FIRST
  (:meth:`SpotInventoryManager.record_live_fill`, the outbox pattern) and
  emitted only when the ledger accepted it as new.
- **``order`` supplies the non-fill lifecycle** (created / cancelled /
  rejected). Its ``Filled`` / ``PartiallyFilled`` statuses are skipped —
  emitting them alongside the execution rows would double-apply fills.

The stream owns the private transport: it reconnects with bounded backoff
on any death and never raises for transient trouble — the engine's
``run_event_stream`` does not restart a failed stream, so an escaping
exception here would leave the bot fill-blind for the rest of the run.
The only deliberate raises are the halt signals
(:class:`~pynecore.core.broker.exceptions.BrokerManualInterventionError`
descendants) from the inventory manager's fail-closed paths.

A per-category reconcile pass piggybacks on this loop at a fixed cadence,
mirroring the cTrader plugin's reconcile piggyback pattern: spot runs the
inventory reconcile (lease heartbeat + balance invariant + stream-gap
catch-up), the derivatives refresh the venue position snapshot behind the
entry-row flat sweep (the ``position`` WS topic keeps it fresh in
between).

Inverse execution pushes carry contract-denominated quantities: the
partial-vs-filled bookkeeping stays in that wire domain (it compares
exactly against the dispatched contracts), and the core-facing event
quantities convert to base at the dispatch's recorded anchor price — so a
full fill sums to exactly the base quantity the core dispatched. Each own
fill also folds into the inverse net-position mirror behind the
reduce-side conversions.
"""
import asyncio
import logging
from abc import ABC
from collections.abc import AsyncIterator, Callable
from decimal import Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.models import (
    CANCEL_REASON_VENUE_OCA,
    CANCEL_REASON_VENUE_POSITION_CLEARED,
    CANCEL_REASON_VENUE_REDUCE_ONLY,
    CANCEL_REASON_VENUE_REJECTED,
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_BRACKET_OWN_LEG_ID,
    iter_active_bracket_ownerships,
)
from pynecore.core.plugin import override

from ._base import _BybitBase
from .exceptions import BybitAdoptionBaselineError, BybitError, traceback_wanted
from .helpers import (
    BRACKET_OWN_WIRE_QTY_EXTRA,
    CATEGORY_SPOT,
    ENTRY_CANCEL_CUM_EXEC_EXTRA,
    ENTRY_NETTED_QTY_EXTRA,
    ENTRY_REMAINDER_CANCELLED_EXTRA,
    EXECUTION_CURSOR_OVERLAP_MS,
    EXECUTION_PAGE_LIMIT,
    EXECUTION_SINCE_SKEW_MS,
    EXECUTION_WINDOW_MS,
    PRIVATE_WS_BACKOFF_S,
    PRIVATE_WS_TOPIC_POSITION,
    PRIVATE_WS_TOPICS,
    RECONCILE_CADENCE_S,
    execution_sort_key,
    venue_filled_qty,
)
from .state import parse_exchange_order
from .ws import BybitWebSocket

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow, SpotExecutionRow

    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: ``order`` topic statuses translated to non-fill OrderEvents. Fill-type
#: statuses are absent on purpose — fills come from the execution topic.
_ORDER_STATUS_EVENTS = {
    'New': 'created',
    'Untriggered': 'created',
    'Cancelled': 'cancelled',
    'PartiallyFilledCanceled': 'cancelled',
    'Deactivated': 'cancelled',
    'Rejected': 'rejected',
}

def _venue_trimmed_qty(entry: dict, dispatched: float) -> float | None:
    """The size the venue cut an order down to, if it did.

    An execution row carries the order's own ``orderQty``; one below the
    dispatched size means the venue trimmed the order.

    :param entry: One execution row.
    :param dispatched: The wire-domain size this run dispatched.
    :return: The trimmed size, or ``None`` when the order kept its size or
        the row does not carry the field.
    """
    try:
        order_qty = float(entry.get('orderQty'))
    except (TypeError, ValueError):
        return None
    if not 0.0 < order_qty < dispatched - _FILL_EPS:
        return None
    return order_qty


#: ``cancelType`` values that mean the VENUE terminated the order (never a
#: user or operator action), mapped onto the engine's venue-driven cancel
#: reasons (the V5 ``cancelType`` enum):
#:
#: * ``CancelByReduceOnly``: a reduce-only order squeezed to nothing after
#:   the net position fell below the summed reduce-only quantity (measured
#:   live: a sibling bracket's SL fill shrank the shared inverse position
#:   and the venue amended this TP to the residual, then cancelled it).
#: * The OCO pair types: the sibling leg of a venue-linked pair triggered.
#: * The position-clearing types: the venue cleared the position the order
#:   worked on (TP/SL/trailing clear, liquidation and ADL preparation,
#:   settlement, delivery).
#: * The venue-refusal types: the venue terminated the order on its own
#:   rules (an unaffordable triggered order, margin and account checks,
#:   trading limits, rebalancing, an admin action, the disconnect-cancel
#:   protection).
#:
#: ``CancelByUser`` stays unmapped, and so does ``CancelBySmp``: self-match
#: prevention fires when another order of the same account crossed this
#: one — an order someone else placed.
_CANCEL_TYPE_REASONS = {
    'CancelByReduceOnly': CANCEL_REASON_VENUE_REDUCE_ONLY,
    'CancelByOCOTpCanceledBySlTriggered': CANCEL_REASON_VENUE_OCA,
    'CancelByOCOSlCanceledByTpTriggered': CANCEL_REASON_VENUE_OCA,
    'CancelByTpSlTsClear': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelByPrepareLiq': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelAllBeforeLiq': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelByPrepareAdl': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelAllBeforeAdl': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelBySettle': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelByDelivery': CANCEL_REASON_VENUE_POSITION_CLEARED,
    'CancelByCannotAffordOrderCost': CANCEL_REASON_VENUE_REJECTED,
    'CancelByPmTrialMmOverEquity': CANCEL_REASON_VENUE_REJECTED,
    'CancelByAccountBlocking': CANCEL_REASON_VENUE_REJECTED,
    'CancelByCrossSelfMuch': CANCEL_REASON_VENUE_REJECTED,
    'CancelByCrossReachMaxTradeNum': CANCEL_REASON_VENUE_REJECTED,
    'CancelByRebalance': CANCEL_REASON_VENUE_REJECTED,
    'CancelByAdmin': CANCEL_REASON_VENUE_REJECTED,
    'CancelByDCP': CANCEL_REASON_VENUE_REJECTED,
}

#: Small float slack when comparing cumulative fills against the dispatch
#: quantity — the quantities themselves come from exact decimal strings,
#: the slack only absorbs the float round-trip.
_FILL_EPS = 1e-9


def _cum_exec_qty(entry: dict) -> float | None:
    """The ``cumExecQty`` of one venue order object, ``None`` when absent or unparsable."""
    raw = entry.get('cumExecQty')
    if raw is None or raw == '':
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


#: Server pages drained per backfill window before the read is declared
#: inconclusive — mirrors the spot inventory port's page cap (200 pages x
#: 100 rows = 20k fills in one 7-day window, far beyond a single-symbol
#: bot's plausible fill rate; hitting it means the fail-closed path is the
#: honest answer).
_MAX_BACKFILL_WINDOW_PAGES = 200

#: Audit-event kind under which the durable derivative execution-backfill
#: watermark is persisted (read back at startup via
#: ``iter_events_by_kind_for_run_id`` — the storage layer's documented
#: activity-cursor rebuild mechanism).
_DERIV_EXEC_CURSOR_EVENT = 'deriv_exec_cursor'

#: ``stopOrderType`` values a venue-materialised trading-stop execution can
#: carry, mapped to the bracket leg they fire for. On a hedge account the
#: bracket is a POSITION attribute (``/v5/position/trading-stop``): when it
#: triggers, Bybit creates the closing market order ITSELF, with no
#: ``orderLinkId`` and an ``orderId`` this run never journalled — so the
#: dispatch-bookkeeping reverse map cannot see it and the bot's own
#: protective close would be dropped as external activity (measured live:
#: the S-X stop fired at 17:55, the fill was ignored twice, and the book
#: stayed short 0.02 for 47 minutes, mis-sizing the next folded reversal).
#: Only the ``Full``-mode types: the plugin writes every trading stop with
#: ``tpslMode=Full``, so a ``PartialTakeProfit`` / ``PartialStopLoss`` fire
#: is a stop someone else placed (measured: none of the 908 own venue
#: bracket attributions in the lab journal carried a Partial type).
_VENUE_STOP_ORDER_LEG = {
    'TakeProfit': LegType.TAKE_PROFIT,
    'StopLoss': LegType.STOP_LOSS,
    'TrailingStop': LegType.TRAILING_STOP,
}

#: A venue mechanism that closes the position on its own — a liquidation,
#: its takeover, auto-deleveraging, an admin close, a delivery — by
#: ``createType`` of a ``Trade`` execution or by ``execType``. Read at
#: adoption for the time the bot was down (:meth:`_unbooked_venue_closeouts`).
_VENUE_CLOSEOUT_CREATE_TYPES = frozenset({
    'CreateByLiq',
    'CreateByTakeOver_PassThrough',
    'CreateByAdl_PassThrough',
    'CreateByAdminClosing',
})
_VENUE_CLOSEOUT_EXEC_TYPES = frozenset({'AdlTrade', 'BustTrade', 'Delivery'})


class _EventStreamMixin(_BybitBase, ABC):
    """Order-event PUSH stream: private ``order`` + ``execution`` topics."""

    @override
    async def watch_orders(self) -> AsyncIterator[OrderEvent]:
        """Stream order status updates, driving the inventory reconcile inline.

        Runs for the lifetime of the strategy on the broker event loop.
        The private WS is (re)opened here with bounded backoff; each
        iteration blocks on the frame queue only until the next reconcile
        deadline, so a quiet stream cannot starve the invariant check and
        a busy stream cannot starve it either (deadline checked at the
        loop top). While the stream is down the reconcile keeps its
        cadence between the reconnect attempts: its REST reads and the
        execution backfill are the only way a fill of the outage reaches
        the engine before the stream is back.
        """
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._broker_market)
        is_deriv = market.category != CATEGORY_SPOT
        await self._observe_resume_baseline(market)
        loop = asyncio.get_running_loop()
        next_reconcile = loop.time() + RECONCILE_CADENCE_S
        next_reconnect = loop.time()
        reconnect_attempt = 0
        while True:
            self._raise_pending_halt()
            for event in self._replay_pre_adoption_frames(market):
                yield event
            ws = self._private_ws
            if ws is None or not ws.is_open:
                if is_deriv:
                    # Until the backfill has re-read the window the stream
                    # was down (or not open yet), the position ledgers prove
                    # nothing: the views of that window are lost too. Armed
                    # again on every pass of the outage, which lasts until now.
                    self._ledger_stream_opened_ms = int(epoch_time() * 1000)
                if loop.time() >= next_reconnect:
                    error = await self._open_private_ws(market)
                    if error is None:
                        if reconnect_attempt:
                            logger.info("Bybit private WS reconnected after %d attempt(s)",
                                        reconnect_attempt)
                        reconnect_attempt = 0
                        if is_deriv:
                            self._ledger_stream_opened_ms = int(epoch_time() * 1000)
                            # A (re)connect closes the stream-gap: any fill
                            # that landed while the private PUSH was down is
                            # read back from the durable execution cursor.
                            # The first open at startup only seeds the
                            # watermark (nothing prior is the bot's gap);
                            # every later open recovers the outage window.
                            for event in await self._run_deriv_fill_backfill(market):
                                yield event
                    else:
                        delay = PRIVATE_WS_BACKOFF_S[
                            min(reconnect_attempt, len(PRIVATE_WS_BACKOFF_S) - 1)]
                        reconnect_attempt += 1
                        logger.warning(
                            "Bybit private WS connect failed (attempt %d, retry in %.0fs): %s",
                            reconnect_attempt, delay, error,
                        )
                        next_reconnect = loop.time() + delay
            now = loop.time()
            if now >= next_reconcile:
                recovered = (await self._run_deriv_reconcile(market)
                             if is_deriv
                             else await self._run_spot_reconcile(market))
                for event in recovered:
                    yield event
                next_reconcile = loop.time() + RECONCILE_CADENCE_S
                continue
            queue = self._private_events
            if queue is None:
                # The stream is down: pace to the earlier of the next
                # reconcile and the next reconnect attempt.
                await asyncio.sleep(min(next_reconcile, next_reconnect) - now)
                continue
            try:
                frame = await asyncio.wait_for(queue.get(), timeout=next_reconcile - now)
            except TimeoutError:
                continue
            if frame is None:
                # Transport died — drop it and let the top of the loop
                # reconnect with backoff.
                await self._close_private_ws()
                continue
            if self._adoption_gate_active(market):
                # The startup adoption baseline (F2) has not committed yet:
                # translating this frame now could emit a fill the baseline
                # is concurrently folding into the adopted snapshot (its
                # stable-pass walks reach every execution up to the verify
                # read), double-applying the slice when the engine drains
                # the queue after adoption. Park the frame; the loop top
                # replays it once the baseline latches, when the seeded
                # ``execId`` frontier decides ownership atomically. Bybit's
                # private WS never replays missed history, so nothing here
                # is a restart replay the engine's pre-drain would need.
                self._pre_adoption_frames.append(frame)
                continue
            # The gate may have cleared while this frame sat in the queue:
            # replay the parked frames FIRST so arrival order is preserved.
            for event in self._replay_pre_adoption_frames(market):
                yield event
            for event in self._translate_private_frame(frame, market):
                yield event

    def _adoption_gate_active(self, market: 'InstrumentInfo') -> bool:
        """Whether private-frame translation must wait for the F2 baseline.

        Derivatives with persistence only: the baseline latches inside the
        engine's startup ``get_position`` read, and until it does, any
        translated fill could be double-owned (adopted into the position
        snapshot AND queued as an OrderEvent). Spot has no adoption
        baseline, and without a store the baseline never runs — neither
        may park frames, or they would be parked forever.
        """
        return (market.category != CATEGORY_SPOT
                and self.store_ctx is not None
                and not self._adoption_baselined)

    def _replay_pre_adoption_frames(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Translate the parked frames once the F2 baseline has latched.

        Replayed in arrival order through the normal translator. The
        baseline seeded every adopted execution into ``_seen_exec_ids``,
        so a replayed fill the adopted snapshot already owns is dropped
        and only genuinely post-adoption activity is emitted — each fill
        ends up with exactly one owner. No-op while the gate is still
        active or nothing is parked.
        """
        if not self._pre_adoption_frames or self._adoption_gate_active(market):
            return []
        parked = self._pre_adoption_frames
        self._pre_adoption_frames = []
        events: list[OrderEvent] = []
        for frame in parked:
            events.extend(self._translate_private_frame(frame, market))
        return events

    def _raise_pending_halt(self) -> None:
        """Deliver an armed inventory-halt signal on the engine's channel."""
        manager = self._spot_manager
        if manager is None:
            return
        halt = manager.consume_pending_halt()
        if halt is not None:
            raise halt

    def soak_subscribe_account_updates(
            self, callback: Callable[[dict], None],
    ) -> Callable[[], None]:
        """Observe raw private account frames without consuming the event queue."""
        self._private_account_update_subscribers.add(callback)

        def unsubscribe() -> None:
            self._private_account_update_subscribers.discard(callback)

        return unsubscribe

    async def soak_wait_account_updates_started(self) -> None:
        """Wait until the authenticated private account stream is subscribed."""
        await self._private_account_updates_started.wait()

    def soak_set_account_update_task(self, task: asyncio.Task[None]) -> None:
        """Bind the production ``watch_orders`` task started by the direct canary."""
        if self._soak_account_update_task is not None:
            raise RuntimeError("Bybit direct account-update task is already bound")
        self._soak_account_update_task = task

    async def soak_close_account_updates(self) -> None:
        """Close and reap the private account stream used by a direct canary."""
        task = self._soak_account_update_task
        self._soak_account_update_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._close_private_ws()

    def _publish_private_account_update(self, frame: dict) -> None:
        for callback in tuple(self._private_account_update_subscribers):
            try:
                callback(frame)
            except (LookupError, RuntimeError, TypeError, ValueError):
                logger.exception("Bybit private account-update observer failed")

    # --- private transport ------------------------------------------------------

    async def _open_private_ws(self, market: 'InstrumentInfo') -> BybitError | None:
        """One attempt to (re)open + authenticate the private stream.

        Never raises for transient trouble: credential validity was
        already proven by the account-identity latch at startup, so a
        failing private connect is recoverable and :meth:`watch_orders`
        retries it forever, paced by :data:`PRIVATE_WS_BACKOFF_S` — halting
        the bot on it would strand the user's open exposure.

        :return: ``None`` once the stream is open, else the connect error.
        """
        self._raise_pending_halt()
        host = self._hosts.ws_private
        if not host:
            raise BrokerManualInterventionError(
                f"Bybit region {self.config.region!r} has no private "
                f"stream host; set the ws_private_host override"
            )
        queue: asyncio.Queue[dict | None] = asyncio.Queue()

        def _on_message(data: dict, q: asyncio.Queue = queue) -> None:
            self._publish_private_account_update(data)
            q.put_nowait(data)

        async def _on_closed(q: asyncio.Queue = queue) -> None:
            q.put_nowait(None)

        ws = BybitWebSocket(
            f"wss://{host}/v5/private",
            on_message=_on_message,
            on_closed=_on_closed,
        )
        topics = list(PRIVATE_WS_TOPICS)
        if market.category != CATEGORY_SPOT:
            # The position topic feeds the last-known-size cache the
            # entry-row flat sweep keys off; spot has no position object.
            topics.append(PRIVATE_WS_TOPIC_POSITION)
        try:
            await ws.open(api_key=self.config.api_key,
                          api_secret=self.config.api_secret)
            await ws.subscribe(topics)
        except asyncio.CancelledError:
            await asyncio.shield(ws.close())
            raise
        except BybitError as e:
            await ws.close()
            return e
        except BaseException:
            await ws.close()
            raise
        self._private_ws = ws
        self._private_events = queue
        self._private_account_updates_started.set()
        return None

    async def _close_private_ws(self) -> None:
        """Drop the private transport (idempotent)."""
        ws = self._private_ws
        self._private_ws = None
        self._private_events = None
        self._private_account_updates_started.clear()
        if ws is not None:
            await ws.close()

    # --- inbound translation ------------------------------------------------------

    def _translate_private_frame(
            self, frame: dict, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Translate one private-stream frame into OrderEvents."""
        topic = str(frame.get('topic') or '')
        if topic == 'execution':
            return self._translate_executions(frame, market)
        if topic == 'order':
            return self._translate_order_rows(frame, market)
        if topic == PRIVATE_WS_TOPIC_POSITION:
            self._ingest_position_frame(frame, market)
        return []

    def _ingest_position_frame(
            self, frame: dict, market: 'InstrumentInfo',
    ) -> None:
        """Fold one ``position`` push into the net-size cache (derivatives).

        No OrderEvent is emitted — position accounting is driven by fills;
        the push keeps the flat sweep's view fresh between reconcile
        snapshots and shows the trading stops to the protection guard.
        """
        rows = [
            entry for entry in frame.get('data') or ()
            if str(entry.get('symbol') or '') == market.symbol
            and str(entry.get('category') or market.category) == market.category
        ]
        if not rows:
            return
        self._ingest_position_sizes(rows)
        self._ledger_advance(market)
        self._close_entry_rows_when_flat(market)
        self._observe_position_rows(rows, market, read_started_at=None)

    def _translate_executions(
            self, frame: dict, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Book + emit the fill slices of one ``execution`` push."""
        events: list[OrderEvent] = []
        manager = self._spot_manager
        port = self._spot_port
        with self._ledger_batch(market):
            for entry in frame.get('data') or ():
                if str(entry.get('symbol') or '') != market.symbol:
                    continue
                if str(entry.get('category') or market.category) != market.category:
                    # The same symbol name can exist in several categories
                    # (spot and linear BTCUSDT) — activity of the other
                    # category must not be attributed to this run.
                    continue
                if str(entry.get('execType') or 'Trade') != 'Trade':
                    # Non-trade executions (linear ``Funding`` / ``Settle`` /
                    # ADL / bust / block-trade rows) carry no order fill of
                    # the run: funding is a settle-coin cashflow the wallet
                    # read reflects, the size-changing exotics surface
                    # through the venue-authoritative position read. No
                    # OrderEvent slot exists for a fill-less cash event
                    # (cTrader skips swaps the same way); the position
                    # ledger still learns them.
                    self._note_non_trade_execution(entry, market)
                    continue
                exec_id = str(entry.get('execId') or '')
                if not exec_id or exec_id in self._seen_exec_ids:
                    continue
                coid = str(entry.get('orderLinkId') or '')
                pine_id, from_entry, leg_type = self._resolve_identity(
                    coid or None, str(entry.get('orderId') or '') or None,
                )
                claims: list[tuple[str, str | None, LegType, float]] = []
                ambiguous = False
                if leg_type is None:
                    # Second chance: a venue-materialised trading-stop close
                    # carries no coid and an unknown orderId, but the durable
                    # bracket-ownership row(s) prove it is ours — a pyramid's
                    # shared 'Full' stop closes several exits in one order and
                    # arrives as multiple claims.
                    claims, ambiguous = self._attribute_venue_bracket_execution(
                        entry, market,
                    )
                if leg_type is None and not claims:
                    # External activity (manual trade, another bot): it must
                    # not move this strategy's position (the engine adopts the
                    # venue's position change). The balance invariant is the
                    # guard that catches base-asset interference; a close of
                    # the run's derivative share is reported.
                    if self.store_ctx is not None:
                        self.store_ctx.log_event(
                            'external_activity_ignored',
                            exchange_order_id=str(entry.get('orderId') or '') or None,
                            payload={'exec_id': exec_id},
                        )
                    self._note_external_execution(entry, market, ambiguous=ambiguous)
                    continue
                if manager is not None and port is not None:
                    execution = port.to_execution(entry)
                    if execution is None or not manager.record_live_fill(execution):
                        # Unparsable (already logged by the port) or a replay
                        # the ledger already booked — do not emit.
                        self._seen_exec_ids.add(exec_id)
                        continue
                self._seen_exec_ids.add(exec_id)
                events.extend(self._book_own_execution(
                    entry, market, coid=coid, pine_id=pine_id,
                    from_entry=from_entry, leg_type=leg_type, claims=claims,
                ))
        if events:
            self._close_entry_rows_when_flat(market)
        return events

    def _book_own_execution(
            self, entry: dict, market: 'InstrumentInfo', *, coid: str,
            pine_id: str | None, from_entry: str | None,
            leg_type: LegType | None,
            claims: list[tuple[str, str | None, LegType, float]],
    ) -> list[OrderEvent]:
        """Build the fill event(s) of one own execution and enter it into the ledger.

        A venue-bracket execution with claims splits into one event per
        owning exit; any other own execution is one event. The position
        ledger learns the whole execution with the change its booking made
        to the run's journal share.
        """
        before = self._journal_leg_shares() if market.category != CATEGORY_SPOT else {}
        self._ledger_credit_taken = 0.0
        events: list[OrderEvent] = []
        if claims:
            for slice_entry, s_pine, s_from, s_leg in \
                    self._venue_bracket_claim_slices(entry, claims):
                event = self._fill_event(
                    slice_entry, market, coid=coid, pine_id=s_pine,
                    from_entry=s_from, leg_type=s_leg,
                )
                if event is not None:
                    events.append(event)
        else:
            assert leg_type is not None
            event = self._fill_event(
                entry, market, coid=coid, pine_id=pine_id,
                from_entry=from_entry, leg_type=leg_type,
            )
            if event is not None:
                events.append(event)
        if market.category != CATEGORY_SPOT and self.store_ctx is not None:
            after = self._journal_leg_shares()
            self._note_share_start(entry, before, after, market)
            self._note_own_execution(
                entry, market,
                reducing=bool(claims) or leg_type is not LegType.ENTRY,
                share={idx: after.get(idx, 0.0) - before.get(idx, 0.0)
                       for idx in set(before) | set(after)},
                credit=self._ledger_credit_taken,
            )
        return events

    def _attribute_venue_bracket_execution(
            self, entry: dict, market: 'InstrumentInfo',
    ) -> tuple[list[tuple[str, str | None, LegType, float]], bool]:
        """Attribute a coid-less trading-stop execution to the owning bracket(s).

        Second-chance identity resolution for executions
        :meth:`_resolve_identity` could not map: a position-attribute
        bracket's closing order (hedge-leg amend or the netting
        trading-stop) carries no ``orderLinkId`` and an unknown
        ``orderId`` — but the durable bracket-ownership row this run
        persisted BEFORE attaching (:data:`_VENUE_STOP_ORDER_LEG` explains
        the shape) proves the protection that fired is ours. Only a
        reduce execution (``closedSize > 0``) with a known
        ``stopOrderType``, on this symbol, matching live ownership rows
        of the execution's side, is claimed; a manual venue-UI close
        carries no ``stopOrderType`` and stays external.

        Returns ``(claims, ambiguous)``, the claims as ``(exit_id,
        from_entry, leg, claim_qty)``. One matching row claims the whole
        slice. SEVERAL matching rows on the
        SAME broker leg are the pyramid shape: every exit amends the one
        shared position attribute, so when the ``tpslMode`` 'Full' stop
        fires it closes ALL of them in one venue order — measured live on
        cycle 31, where refusing left the whole journal exposure stale
        and cascaded into a venue/book mismatch. When the slice covers
        the rows' total quantity (grid tolerance), the fill is split
        row-by-row in insertion (FIFO) order — the TV semantics of every
        pyramid exit closing its own entry. A partial slice of a
        multi-row bracket and a genuinely double-sided state (both
        brackets momentarily live mid-reversal, or distinct legs) still
        refuse to guess and leave the disappearance pass to reconcile: no
        claims, ``ambiguous`` set — the execution may well be the bot's own,
        so it must never be read as someone else's close.

        A ``Full`` stop closes the whole leg, foreign exposure merged into it
        included, and the venue may fill it in several executions. The run
        claims its own share — its journal share of the leg — across all of
        them, whatever the rows cover: an entry with no exit of its own (or
        a pyramid entry that filled before its exit was attached) is closed
        by the same stop, and its part rides on the newest row's claim (the
        core closes the run's trades FIFO). Only the rest is foreign. An
        execution after the earlier ones of the same stop order took the
        run's whole share claims nothing (zero-quantity claims: the stop is
        still the run's own, it closed foreign exposure only). A run that
        holds nothing on the leg by its ledger-proven share
        (:meth:`_leg_share_proven`), with nothing claimed before, claims
        nothing at all: the execution is judged as foreign activity, which
        takes nothing of the run's share.
        """
        if self.store_ctx is None:
            return [], False
        if str(entry.get('orderLinkId') or ''):
            return [], False
        leg = _VENUE_STOP_ORDER_LEG.get(str(entry.get('stopOrderType') or ''))
        if leg is None:
            return [], False
        try:
            closed_size = float(entry.get('closedSize') or 0.0)
            exec_qty = float(entry.get('execQty') or 0.0)
            exec_price = float(entry.get('execPrice') or 0.0)
        except (TypeError, ValueError):
            return [], False
        if closed_size <= 0.0 or exec_qty <= 0.0:
            return [], False
        exec_side = str(entry.get('side') or '').lower()
        # No symbol filter: the ownership writers journal under the ENGINE
        # symbol (the chart ticker, e.g. 'ETHUSDT.P') while ``market.symbol``
        # is the venue wire name ('ETHUSDT') — a wire-symbol filter matches
        # nothing (measured live on cycle 33: the TP/SL trading-stop fills
        # fell through to external_activity_ignored and the book desynced
        # +0.01 on the next folded reversal). The store iteration is
        # run-scoped and a run trades exactly one symbol, so the unfiltered
        # walk is exact.
        candidates = [
            row for row in iter_active_bracket_ownerships(self.store_ctx)
            if (row.side or '').lower() == exec_side
            and row.pine_entry_id is not None
        ]
        if not candidates:
            logger.warning(
                "Bybit venue trading-stop execution %s (%s %s closed=%s) on "
                "%s matches no live bracket-ownership row — treating as "
                "external activity",
                entry.get('execId'), entry.get('side'),
                entry.get('execQty'), entry.get('closedSize'), market.symbol,
            )
            return [], False
        claims: list[tuple[str, str | None, LegType, float]]
        split = False
        tolerance = market.qty_step / 2 if market.qty_step > 0 else _FILL_EPS
        # What the earlier executions of this stop order already claimed:
        # the journal share below is net of them.
        claimed = self._venue_stop_claimed(entry)
        # A give-up that ran ahead of this execution clamped the run's share
        # off the journal, wholly or in part; its open credit is the part of
        # the share the stop may still have closed. Both are net of the
        # earlier executions: the fill booking takes the credit first.
        share = (abs(self._journal_leg_shares().get(self._closed_leg(exec_side), 0.0))
                 + self._ledger_reduce_credit_open(entry, exec_side, market))
        # With the leg's share proven by the position ledger, a run holding
        # nothing there had nothing the stop could close: a claim would book
        # a phantom exit fill. Without that proof the journal may lag the
        # venue, and the rows' quantity stands in for the share.
        proven = self._leg_share_proven(exec_side, market)
        if share <= tolerance and claimed <= tolerance and proven:
            return [], False
        if len(candidates) == 1:
            only = candidates[0]
            assert only.pine_entry_id is not None
            claim_qty = exec_qty
            if share > tolerance:
                claim_qty = min(exec_qty, share)
            elif claimed > tolerance:
                claim_qty = 0.0
            claims = [(only.pine_entry_id, only.from_entry, leg, claim_qty)]
        else:
            leg_ids = {
                (row.extras or {}).get(EXTRAS_KEY_BRACKET_OWN_LEG_ID)
                for row in candidates
            }
            row_qtys = [self._ownership_wire_qty(row, market, exec_price)
                        for row in candidates]
            total = sum(row_qtys)
            exhausted = claimed >= total - tolerance
            if len(leg_ids) != 1 or (exec_qty < total - tolerance and not exhausted):
                logger.warning(
                    "Bybit venue trading-stop execution %s matches %d live "
                    "bracket-ownership rows on %s (legs=%s, total=%s, "
                    "exec=%s) — refusing to guess, leaving it to the "
                    "disappearance reconcile",
                    entry.get('execId'), len(candidates), market.symbol,
                    sorted(str(x) for x in leg_ids), total, exec_qty,
                )
                return [], True
            split = True
            # The run's own part: its journal share, which the rows may not
            # cover whole (an entry with no exit of its own, a pyramid entry
            # that filled before its exit was attached); without one, what
            # the rows left unclaimed.
            if share > tolerance:
                own = min(exec_qty, share)
            elif proven:
                own = 0.0
            else:
                own = min(exec_qty, max(0.0, total - claimed))
            claims = []
            used = claimed
            left = own
            for row, row_qty in zip(candidates, row_qtys):
                assert row.pine_entry_id is not None
                part = min(max(0.0, row_qty - used), left)
                used = max(0.0, used - row_qty)
                left -= part
                claims.append((row.pine_entry_id, row.from_entry, leg, part))
            if left > 0.0:
                # The core closes the run's trades FIFO whatever exit a fill
                # names: the part no row covers rides on the newest exit.
                pine_id, from_entry, _, part = claims[-1]
                claims[-1] = (pine_id, from_entry, leg, part + left)
        excess = exec_qty - sum(claim[3] for claim in claims)
        if excess > tolerance:
            # The shared stop also closed exposure the run does not own (a
            # same-direction add someone else merged into the leg): only the
            # run's share is booked, the rest is the foreign party's.
            logger.warning(
                "Bybit venue trading-stop execution %s on %s closed %s, %s "
                "more than this run's share — booking only the run's part",
                entry.get('execId'), market.symbol, exec_qty, excess,
            )
            self.store_ctx.log_event(
                'venue_bracket_fill_foreign_excess',
                exchange_order_id=str(entry.get('orderId') or '') or None,
                payload={'exec_id': str(entry.get('execId') or ''),
                         'exec_qty': exec_qty, 'excess': excess,
                         'stop_order_type': str(entry.get('stopOrderType') or '')},
            )
        for row, claim in zip(candidates, claims):
            self.store_ctx.log_event(
                'venue_bracket_fill_attributed',
                client_order_id=row.client_order_id,
                exchange_order_id=str(entry.get('orderId') or '') or None,
                payload={
                    'exec_id': str(entry.get('execId') or ''),
                    'stop_order_type': str(entry.get('stopOrderType') or ''),
                    'exit_id': row.pine_entry_id,
                    'from_entry': row.from_entry,
                    'claim_qty': claim[3],
                    'split': split,
                },
            )
        return claims, False

    def _leg_share_proven(self, side: str, market: 'InstrumentInfo') -> bool:
        """Whether the journal share of the leg a reduce on ``side`` closes is proven.

        Once the adoption baseline committed and the leg's position ledger
        started, the journal holds the run's share as the ledger judged it:
        every flat sweep waits for it, and a give-up leaves its netting as an
        open credit (:meth:`_ledger_reduce_credit_open`).
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT \
                or not self._adoption_baselined:
            return False
        ledger = self._leg_ledgers.get(self._closed_leg(side))
        return ledger is not None and ledger.started

    def _ownership_wire_qty(
            self, row: 'OrderRow', market: 'InstrumentInfo', exec_price: float,
    ) -> float:
        """A bracket-ownership row's quantity in the wire domain of the executions.

        The row's ``qty`` is the core's base quantity; the attach records the
        wire quantity beside it (:data:`BRACKET_OWN_WIRE_QTY_EXTRA`). On
        inverse a row without it converts at its parent entry's anchor, else
        at the execution price: a split in base would hand a contract-sized
        fill almost wholly to the last exit.
        """
        raw = (row.extras or {}).get(BRACKET_OWN_WIRE_QTY_EXTRA)
        if isinstance(raw, (int, float)) and not isinstance(raw, bool):
            return float(raw)
        if not market.is_inverse:
            return float(row.qty)
        anchor: Decimal | None = None
        if row.from_entry is not None:
            entry_coid = self._entry_coid_for(row.from_entry)
            if entry_coid is not None:
                anchor = self._inverse_anchor_for(entry_coid)
        price = float(anchor) if anchor is not None else exec_price
        return float(row.qty) * price

    def _venue_stop_claimed(self, entry: dict) -> float:
        """What the other executions of ``entry``'s stop order claimed for the run.

        Read from the ``venue_bracket_fill_attributed`` audit trail of the
        run (every process instance), so an execution of the same stop that
        arrives after a restart still sees the claims made before it.
        """
        order_id = str(entry.get('orderId') or '')
        if not order_id or self.store_ctx is None:
            return 0.0
        exec_id = str(entry.get('execId') or '')
        claimed = 0.0
        for _ik, _coid, eoid, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id('venue_bracket_fill_attributed'):
            if eoid != order_id or payload.get('exec_id') == exec_id:
                continue
            qty = payload.get('claim_qty')
            if isinstance(qty, (int, float)) and not isinstance(qty, bool):
                claimed += float(qty)
        return claimed

    async def _unbooked_venue_stop_executions(
            self, market: 'InstrumentInfo', floor_ms: int,
    ) -> list[dict]:
        """The run's own fired trading stops no process of the run booked, up to ``floor_ms``.

        A position TP/SL or trailing stop that fires while the bot is down
        is a coid-less venue execution under an order id no journal row
        holds: the adoption baseline's per-order walk never reads it, and
        the execution backfill starts at the adoption floor. Read here from
        the symbol's execution history, from the oldest live
        bracket-ownership row (persisted before its attach, so its stop
        cannot fire earlier; no earlier than the run's last backfill
        watermark, below which every execution was attributed already) up
        to the floor. Kept: a reduce with a position ``stopOrderType`` on the
        side of a live ownership row older than it, whose ``execId`` no
        attribution of the run recorded.

        :raises BybitAdoptionBaselineError: When a history window cannot be
            read completely.
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return []
        owners = [row for row in iter_active_bracket_ownerships(self.store_ctx)
                  if row.side in ('buy', 'sell')]
        if not owners:
            return []
        return await self._unbooked_executions(
            market, min(row.created_ts_ms for row in owners), floor_ms,
            'venue_bracket_fill_attributed',
            lambda entry: self._own_venue_stop_execution(entry, market, owners),
        )

    async def _unbooked_venue_closeouts(
            self, market: 'InstrumentInfo', floor_ms: int,
    ) -> list[dict]:
        """The venue's own closes of the run's position while the bot was down, up to ``floor_ms``.

        A liquidation, auto-deleveraging, an admin close or a delivery
        (:data:`_VENUE_CLOSEOUT_CREATE_TYPES`, :data:`_VENUE_CLOSEOUT_EXEC_TYPES`)
        carries no coid and no size of the leg it closed: how much of it was
        the run's share cannot be told once someone else holds the leg too.
        Read from the oldest live entry row on, like
        :meth:`_unbooked_venue_stop_executions`; kept: a close on the side of
        a live entry row older than it, whose ``execId`` no adoption of the
        run recorded.

        :raises BybitAdoptionBaselineError: When a history window cannot be
            read completely.
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return []
        entries = [row for row in self.store_ctx.iter_live_orders()
                   if (row.extras or {}).get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                   and row.filled_qty > _FILL_EPS]
        if not entries:
            return []
        return await self._unbooked_executions(
            market, min(row.created_ts_ms for row in entries), floor_ms,
            'venue_closeout_while_down',
            lambda entry: self._venue_closeout_execution(entry, market, entries),
        )

    async def _unbooked_executions(
            self, market: 'InstrumentInfo', oldest_ms: int, floor_ms: int,
            booked_kind: str, keep: Callable[[dict], bool],
    ) -> list[dict]:
        """The executions ``keep`` selects from ``oldest_ms`` up to ``floor_ms``
        whose ``execId`` no ``booked_kind`` event of the run recorded.

        The read starts no earlier than the run's last backfill watermark:
        below it every execution was attributed already.

        :raises BybitAdoptionBaselineError: When a history window cannot be
            read completely.
        """
        assert self.store_ctx is not None
        start = oldest_ms - EXECUTION_SINCE_SKEW_MS
        watermark = self._load_deriv_exec_watermark(market)
        if watermark is not None:
            start = max(start, watermark - EXECUTION_CURSOR_OVERLAP_MS)
        booked = {
            str(payload.get('exec_id') or '') for _ik, _coid, _eoid, payload
            in self.store_ctx.iter_events_by_kind_for_run_id(booked_kind)
        }
        found: dict[str, dict] = {}
        while start < floor_ms:
            end = min(start + EXECUTION_WINDOW_MS, floor_ms)
            entries, conclusive = await self._drain_deriv_exec_window(market, start, end)
            if not conclusive:
                raise BybitAdoptionBaselineError(
                    "adoption baseline: execution history read failed for "
                    f"{market.symbol} ({start}..{end})",
                )
            for entry in entries:
                exec_id = str(entry.get('execId') or '')
                if exec_id and exec_id not in booked and exec_id not in found and keep(entry):
                    found[exec_id] = entry
            start = end
        return sorted(found.values(), key=execution_sort_key)

    @staticmethod
    def _venue_closeout_execution(
            entry: dict, market: 'InstrumentInfo', entries: list['OrderRow'],
    ) -> bool:
        """Whether ``entry`` is the venue closing a side the live entry rows ``entries`` own."""
        if str(entry.get('symbol') or '') != market.symbol:
            return False
        if str(entry.get('category') or market.category) != market.category:
            return False
        exec_type = str(entry.get('execType') or 'Trade')
        if exec_type not in _VENUE_CLOSEOUT_EXEC_TYPES and not (
                exec_type == 'Trade'
                and str(entry.get('createType') or '') in _VENUE_CLOSEOUT_CREATE_TYPES):
            return False
        try:
            closed_size = float(entry.get('closedSize') or 0.0)
            exec_ms = int(entry.get('execTime') or 0)
        except (TypeError, ValueError):
            return False
        if closed_size <= 0.0:
            return False
        entry_side = 'buy' if str(entry.get('side') or '').lower() == 'sell' else 'sell'
        return any(row.side == entry_side
                   and row.created_ts_ms - EXECUTION_SINCE_SKEW_MS <= exec_ms
                   for row in entries)

    @staticmethod
    def _own_venue_stop_execution(
            entry: dict, market: 'InstrumentInfo', owners: list['OrderRow'],
    ) -> bool:
        """Whether ``entry`` is a fired trading stop of one of the ownership rows ``owners``."""
        if str(entry.get('symbol') or '') != market.symbol:
            return False
        if str(entry.get('category') or market.category) != market.category:
            return False
        if str(entry.get('execType') or 'Trade') != 'Trade':
            return False
        if str(entry.get('orderLinkId') or ''):
            return False
        if str(entry.get('stopOrderType') or '') not in _VENUE_STOP_ORDER_LEG:
            return False
        try:
            closed_size = float(entry.get('closedSize') or 0.0)
            exec_qty = float(entry.get('execQty') or 0.0)
            exec_ms = int(entry.get('execTime') or 0)
        except (TypeError, ValueError):
            return False
        if closed_size <= 0.0 or exec_qty <= 0.0:
            return False
        side = str(entry.get('side') or '').lower()
        return any(row.side == side
                   and row.created_ts_ms - EXECUTION_SINCE_SKEW_MS <= exec_ms
                   for row in owners)

    @staticmethod
    def _venue_bracket_claim_slices(
            entry: dict, claims: list[tuple[str, str | None, LegType, float]],
    ) -> list[tuple[dict, str, str | None, LegType]]:
        """Cut one venue-bracket execution into per-claim entry slices.

        Each claim gets a shallow copy of the raw execution row with
        ``execQty`` (and a pro-rated ``execFee``, remainder on the last
        slice so the total is exact) replaced by its share, ready for the
        shared :meth:`_fill_event` builder. A single whole-slice claim
        passes the original row through untouched.

        Every slice also gets its OWN ``execId`` (the venue id suffixed
        with the owning exit and entry): the core's duplicate-fill gate
        keys on :attr:`~pynecore.core.broker.models.OrderEvent.fill_id`,
        so slices sharing the venue execution id would book only the
        first one — measured live on cycle 219, where a three-exit
        pyramid stop left the book two legs short of flat, the reversal
        close no-op'd against the already-flat venue and the position
        state was finally cleared as an "external close". The suffix is
        derived from the ownership rows, so the live stream and the
        reconnect backfill produce the same ids for the same execution
        and a replay still dedups.
        """
        try:
            exec_qty = float(entry.get('execQty') or 0.0)
        except (TypeError, ValueError):
            exec_qty = 0.0
        total_qty = sum(claim[3] for claim in claims)
        if total_qty <= _FILL_EPS:
            # The run's stop closed foreign exposure only: no fill of the run.
            return []
        if len(claims) == 1 and abs(total_qty - exec_qty) <= _FILL_EPS:
            pine_id, from_entry, leg, _ = claims[0]
            return [(entry, pine_id, from_entry, leg)]
        try:
            total_fee = float(entry.get('execFee') or 0.0)
        except (TypeError, ValueError):
            total_fee = 0.0
        # The fee is pro-rated over the whole execution: a part closing
        # foreign exposure carries its own share of it, never booked here.
        if exec_qty > 0.0 and total_qty < exec_qty:
            total_fee *= total_qty / exec_qty
        exec_id = str(entry.get('execId') or '')
        slices: list[tuple[dict, str, str | None, LegType]] = []
        fee_left = total_fee
        for index, (pine_id, from_entry, leg, qty) in enumerate(claims):
            if index == len(claims) - 1:
                fee = fee_left
            else:
                fee = total_fee * (qty / total_qty) if total_qty > 0 else 0.0
                fee_left -= fee
            slice_id = (exec_id if len(claims) == 1
                        else f"{exec_id}#{pine_id}#{from_entry or ''}")
            slices.append((
                {**entry, 'execQty': repr(qty), 'execFee': repr(fee),
                 'execId': slice_id},
                pine_id, from_entry, leg,
            ))
        return slices

    def _close_entry_rows_when_flat(self, market: 'InstrumentInfo') -> None:
        """Close the fully filled entry rows once the position is gone.

        Entry rows live as long as the position they opened (parent-row
        lifecycle, cTrader pattern) — see :meth:`_fill_event`. The
        flatness source is category-specific: spot reads the inventory
        ledger (sub-grid fee dust counts as flat, matching
        :meth:`get_position`); linear reads the last-known venue position
        cache, which starts unknown — the sweep never fires on ignorance.
        A one-way flat view the position ledger cannot explain yet waits for
        its executions (:meth:`_sweep_waits_for_ledger`): it may show the
        run's own close before that close is booked.
        A row still carrying the engine's ``defensive_close_pending``
        marker is left for a later sweep: the engine clears the marker
        when the defensive close settles, and closing the row before that
        would orphan the restart replay of the marker.
        """
        if self.store_ctx is None:
            return
        if market.category != CATEGORY_SPOT:
            if not self._deriv_is_flat() or self._sweep_waits_for_ledger():
                return
        else:
            manager = self._spot_manager
            if manager is None:
                return
            net = manager.fold.net_base
            if net > 0 and (market.qty_step <= 0 or float(net) >= market.qty_step):
                return
        for row in self.store_ctx.iter_live_orders(symbol=market.symbol):
            extras = row.extras or {}
            if extras.get('kind') not in (ENTRY_KIND_POSITION,
                                          ENTRY_KIND_WORKING):
                continue
            if 'defensive_close_pending' in extras:
                continue
            if row.filled_qty >= row.qty - _FILL_EPS:
                # Retire the row AND clear the intent envelope in one step,
                # exactly like the startup orphan pass
                # (``_retire_startup_orphans`` / ``_clear_intent_anchor``):
                # ``close_order`` alone leaves the ``envelopes`` anchor
                # behind, so a later re-entry of the same Pine id would
                # rebuild the SAME (now spent) ``orderLinkId`` from it. The
                # freshness gate above guarantees this only fires on a
                # genuine flat, where the intent is truly done.
                self.store_ctx.close_order(row.client_order_id)
                if row.intent_key:
                    self.store_ctx.record_complete(row.intent_key)

    def _reduce_entry_ownership(self, side: str, qty: float) -> None:
        """Apply one reducing derivative fill to this run's durable entry rows.

        Entry rows carry the run-owned slice across restart (hedge-mode
        ownership cannot be reconstructed from Bybit's two account-wide
        aggregate legs at all, and one-way adoption/K3 read the live rows'
        signed sum). The flat sweep only retires rows on a genuine flat: a
        reversal is non-flat after its residual opens, and a partially
        closed position holds live entry rows whose consumed exposure would
        otherwise keep counting — a cycle ending mid-position then reads a
        book that owns closes the venue already executed (measured on the
        inverse lane, 2026-08-21: book 305 contracts vs venue 1). Netting
        every reducing fill keeps the rows truthful in all modes; quantities
        are wire-domain on both sides, so no anchor conversion is involved.

        Consume the opposite-side entry rows FIFO through
        :meth:`_net_entry_row`: a partial reduction lowers the owned quantity
        (a still-working entry keeps its unfilled remainder), a full
        reduction of a settled row retires only that physical row. It
        deliberately does not complete the shared intent key because a
        same-ID reversal may already own a fresh residual entry envelope
        under that key.
        """
        if self.store_ctx is None or qty <= 0.0:
            return
        entry_side = 'buy' if side == 'sell' else 'sell'
        remaining = qty
        rows = sorted(
            (
                row for row in self.store_ctx.iter_live_orders()
                if row.side == entry_side
                and (row.extras or {}).get('kind')
                in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                and row.filled_qty > _FILL_EPS
            ),
            key=lambda row: row.created_ts_ms,
        )
        for row in rows:
            if remaining <= _FILL_EPS:
                break
            consumed = min(remaining, row.filled_qty)
            self._net_entry_row(row, consumed)
            remaining -= consumed

    def _net_entry_row(self, row: 'OrderRow', consumed: float) -> None:
        """Net ``consumed`` owned exposure out of one entry row.

        ``qty`` and ``filled_qty`` drop together, so ``filled_qty`` stays the
        owned exposure while ``qty - filled_qty`` keeps a still-working
        order's unfilled remainder: its later executions are booked instead
        of being skipped as already covered. The cumulative netted quantity
        is recorded under :data:`ENTRY_NETTED_QTY_EXTRA`, from which
        :func:`venue_filled_qty` restores the venue-domain cursor that
        comparisons against the order's ``cumExecQty`` need. A row left with
        neither owned exposure nor a working remainder is retired (only the
        physical row — see :meth:`_reduce_entry_ownership`).

        :param row: The live entry row.
        :param consumed: The owned quantity to net out (wire domain, at most
            ``row.filled_qty``).
        """
        if self.store_ctx is None or consumed <= 0.0:
            return
        qty = row.qty - consumed
        if qty <= _FILL_EPS:
            self.store_ctx.close_order(row.client_order_id)
            return
        extras = dict(row.extras or {})
        extras[ENTRY_NETTED_QTY_EXTRA] = (
            venue_filled_qty(row) - row.filled_qty + consumed
        )
        self.store_ctx.upsert_order(
            row.client_order_id,
            qty=qty,
            filled_qty=max(0.0, row.filled_qty - consumed),
            extras=extras,
        )

    def _retire_cancelled_row(
            self, coid: str, venue_cum_exec: float | None = None,
            order_id: str | None = None,
    ) -> None:
        """Retire the row of a cancelled order, keeping an entry's exposure owned.

        A cancel ends only the order's unfilled remainder. An entry row that
        executed anything still carries live exposure, and entry rows own
        that exposure for the lifetime of the position: hedge-mode run
        scoping and restart adoption read the live rows' fill cursors, and
        the flat sweep (:meth:`_close_entry_rows_when_flat`) retires them.
        Every other row (exit / close legs) is closed at once.

        An entry row is flagged :data:`ENTRY_REMAINDER_CANCELLED_EXTRA` and
        settled from the venue's terminal ``cumExecQty``:

        * Unknown (a REST cancel ACK): an execution may have landed before
          the cancel and still be undelivered, so the row is neither
          normalized nor closed — its ``qty`` keeps room for that execution
          and the fill paths book it as usual. The private ``order`` push or
          the reconcile cadence (:meth:`_settle_cancelled_entries`) supplies
          the figure.
        * Zero: the entry never executed — the row is closed.
        * Positive: ``qty`` is normalized to the executed quantity (net of
          the exposure reducing fills consumed) — a settled position row once
          its cursor covers it, the same normalization the adoption baseline
          applies to a dead partially filled entry — and a cursor lagging
          the figure queues the per-order fill recovery.

        Idempotent, and a no-op for an unknown or already closed row, so a
        REST cancel ACK and the venue's terminal ``order`` echo can both
        call it in either order.

        :param coid: The cancelled order's ``orderLinkId``.
        :param venue_cum_exec: The venue order's terminal ``cumExecQty``
            (wire domain), ``None`` when the caller does not hold it.
        :param order_id: The venue ``orderId``, for the per-order fill
            recovery; the row's recorded order id when omitted.
        """
        if self.store_ctx is None:
            return
        row = self.store_ctx.get_order(coid)
        if row is None or row.closed_ts_ms is not None:
            return
        extras = dict(row.extras or {})
        self._forget_protection(coid)
        if extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
            self.store_ctx.close_order(coid)
            return
        if venue_cum_exec is None:
            if not extras.get(ENTRY_REMAINDER_CANCELLED_EXTRA):
                extras[ENTRY_REMAINDER_CANCELLED_EXTRA] = True
                self.store_ctx.upsert_order(coid, extras=extras)
            return
        # ``cumExecQty`` is venue-domain; the row is net of the exposure
        # reducing fills consumed from it.
        netted = venue_filled_qty(row) - row.filled_qty
        executed = max(float(row.filled_qty), venue_cum_exec - netted)
        if executed <= _FILL_EPS:
            self.store_ctx.close_order(coid)
            return
        extras[ENTRY_REMAINDER_CANCELLED_EXTRA] = True
        extras[ENTRY_CANCEL_CUM_EXEC_EXTRA] = venue_cum_exec
        self.store_ctx.upsert_order(coid, qty=executed, extras=extras)
        market = self._market
        ref = order_id or row.exchange_order_id or str(extras.get('order_id') or '')
        if market is not None and market.category != CATEGORY_SPOT and ref:
            self._note_unbooked_fills(row, ref, Decimal(str(venue_cum_exec)))

    def _fill_event(
            self, entry: dict, market: 'InstrumentInfo', *,
            coid: str, pine_id: str | None, from_entry: str | None,
            leg_type: LegType,
    ) -> OrderEvent | None:
        """Build the OrderEvent of one execution slice.

        Quantities compare in the wire domain (exact against the
        dispatched value); on inverse the core-facing event converts to
        base through :meth:`_inverse_fill_anchor` (full-cover snap, the
        dispatch's recorded anchor, the parent entry's anchor, the
        execution price) and the slice folds into the net-position mirror. The
        settle-coin fee normalizes to the quote currency at the execution
        price — the core books the numeric fee in the quote P&L domain.
        """
        try:
            exec_qty = float(entry.get('execQty') or 0.0)
            exec_price = float(entry.get('execPrice') or 0.0)
            fee = float(entry.get('execFee') or 0.0)
            ts_ms = int(entry.get('execTime') or 0)
        except (TypeError, ValueError):
            logger.error("Bybit execution push unparsable: %r", entry)
            return None
        if exec_qty <= 0.0 or exec_price <= 0.0:
            return None
        if market.category != CATEGORY_SPOT and ts_ms:
            # Remember the venue time of this strategy's own derivative fill:
            # the flat sweep must not trust a ``position`` snapshot that
            # predates it (:meth:`_deriv_is_flat`).
            self._last_own_fill_ms = max(self._last_own_fill_ms, ts_ms)
        order_id = str(entry.get('orderId') or '')
        side = str(entry.get('side') or '').lower()
        total_qty = self._dispatch_qty.get(coid, exec_qty)
        cumulative = self._filled_cum.get(coid, 0.0) + exec_qty
        self._filled_cum[coid] = cumulative
        entry_credit_due = (leg_type is LegType.ENTRY
                            and market.category != CATEGORY_SPOT
                            and self._ledger_entry_credit_due(entry, market))
        journal_before = self._run_owned_signed_size() if entry_credit_due else 0.0
        if self.store_ctx is not None and coid:
            # A MARKET entry / CLOSE that parked on an unknown-disposition
            # response and then filled never re-enters ``get_open_orders``,
            # so its engine park would linger until the next restart; the
            # fill is the proof it landed. ``record_unpark`` is idempotent
            # (DELETE-by-coid), a no-op for the common not-parked fill.
            self.store_ctx.record_unpark(coid)
            row = self.store_ctx.get_order(coid)
            if row is not None:
                # The in-memory running sum is venue-domain; the row's
                # cursor is net of the exposure reducing fills consumed.
                netted = venue_filled_qty(row) - row.filled_qty
                total_qty = row.qty
                cumulative = max(cumulative - netted, row.filled_qty + exec_qty)
                self.store_ctx.set_filled(coid, min(total_qty, cumulative))
        trimmed = (_venue_trimmed_qty(entry, total_qty)
                   if leg_type is not LegType.ENTRY else None)
        if trimmed is not None:
            # The venue cut a reduce-only order down to the position left to
            # reduce and reports it Filled at that size (measured live:
            # bybit-inverse cycle 225, a 165-contract close on a 164-contract
            # position); the order is complete, not partially filled.
            total_qty = trimmed
            if self.store_ctx is not None and coid \
                    and self.store_ctx.get_order(coid) is not None:
                self.store_ctx.upsert_order(coid, qty=trimmed)
        is_full = cumulative >= total_qty - _FILL_EPS
        fill_qty = exec_qty
        reducing_deriv = (leg_type is not LegType.ENTRY
                          and market.category != CATEGORY_SPOT)
        # A reduce the position ledger already booked as venue truth (it
        # gave up waiting for this very execution) is not netted twice.
        credit = (self._ledger_reduce_credit(entry, side, exec_qty, market)
                  if reducing_deriv else 0.0)
        # Linear execution rows carry no ``feeCurrency`` — the fee is
        # always the settle coin (empty for spot, where the row has it).
        fee_currency = str(entry.get('feeCurrency') or '') or market.settle_coin
        if market.category == CATEGORY_SPOT:
            # Bybit charges the spot buy-side fee in the BASE coin, so the
            # base actually RECEIVED on a buy (or SHED on a sell) differs
            # from the gross executed quantity by that fee. The core
            # inventory ledger books the fee-adjusted base delta and
            # ``get_position`` synthesizes the resulting net inventory
            # (see :mod:`pynecore.core.broker.spot_inventory` and
            # :meth:`get_position`), so the engine position must track the
            # SAME net quantity. Emitting the gross quantity would let a
            # legitimate ``strategy.close`` size itself from a position the
            # ledger never held and oversell it, driving the fold negative
            # into a ``spot_ledger_negative_inventory`` quarantine. A
            # quote-currency fee does not move the base inventory. The
            # order-completion tracking above stays in the gross wire domain
            # (``is_full`` / ``filled_qty`` compare against the dispatched
            # quantity); only the position-moving ``fill_qty`` is netted.
            spot_fee_ccy = str(entry.get('feeCurrency') or '') or (
                market.base_coin if side == 'buy' else market.quote_coin
            )
            if spot_fee_ccy == market.base_coin:
                fill_qty = exec_qty - fee if side == 'buy' else exec_qty + fee
        if market.is_inverse:
            factor = float(self._inverse_fill_anchor(
                coid, from_entry=from_entry, side=side, contracts=exec_qty,
                exec_price=exec_price, reducing=leg_type is not LegType.ENTRY,
            ))
            fill_qty = exec_qty / factor
            total_qty = total_qty / factor
            cumulative = cumulative / factor
            # The credited part left the mirror with the journal netting
            # of the give-up that booked it ahead.
            self._apply_inverse_fill(side, exec_qty - credit,
                                     fill_qty * (exec_qty - credit) / exec_qty)
            # Inverse fees are charged in the settle coin, but the core
            # books the numeric fee in the quote-currency P&L domain —
            # normalize at the execution price (the fee's own fill).
            fee = fee * exec_price
            fee_currency = market.quote_coin
        if coid and is_full:
            # A fully filled order rests no more: nothing of it to guard.
            self._forget_protection(coid)
        if self.store_ctx is not None and coid and is_full \
                and leg_type is not LegType.ENTRY:
            # Exit / close rows are done once fully filled. ENTRY rows stay
            # LIVE for the lifetime of the position they opened (mirroring
            # the cTrader plugin): the engine persists its defensive-close
            # pending marker on the parent ENTRY row and replays it from
            # the live rows after a restart, so closing the row at fill
            # time would orphan that recovery path. The flat sweep in
            # :meth:`_close_entry_rows_when_flat` closes them once the
            # ledger position is gone.
            self.store_ctx.close_order(coid)
        if reducing_deriv:
            # Every derivative reducing fill — fanned close leg, venue-
            # materialised trading-stop or dispatched TP/SL exit leg — nets
            # the entry rows it consumed; without this the durable
            # entry-row ownership keeps counting exposure the venue already
            # closed, and the next restart's adoption (or a cycle-end book
            # read) over-counts. Exit legs matter when the position never
            # returns to flat afterwards, so the flat sweep cannot retire
            # the rows: inverse contract quantization strands a sub-entry
            # residue (measured on the inverse lane, cycle 47: book 465
            # contracts vs venue 2). Spot stays out: its rows belong to the
            # inventory-ledger machinery.
            self._ledger_credit_taken += credit
            self._reduce_entry_ownership(side, exec_qty - credit)
            self._forget_trading_stop_if_flat(side)
        elif entry_credit_due:
            # A late one-way entry against the run's position (a folded
            # reversal) reduced it first; a give-up may have netted that
            # reduce already.
            self._ledger_credit_taken += self._ledger_entry_credit(
                entry, side, exec_qty, journal_before, market)
        order = ExchangeOrder(
            id=order_id,
            symbol=self.symbol or market.symbol,
            side=side,
            order_type=(OrderType.LIMIT if leg_type is LegType.TAKE_PROFIT
                        else OrderType.STOP if leg_type is LegType.STOP_LOSS
                        else OrderType.MARKET),
            qty=total_qty,
            filled_qty=min(total_qty, cumulative),
            remaining_qty=max(0.0, total_qty - cumulative),
            price=None,
            stop_price=None,
            average_fill_price=exec_price,
            status=OrderStatus.FILLED if is_full else OrderStatus.PARTIALLY_FILLED,
            timestamp=ts_ms / 1000.0 if ts_ms else epoch_time(),
            fee=fee,
            fee_currency=fee_currency,
            reduce_only=leg_type is not LegType.ENTRY,
            client_order_id=coid or None,
        )
        return OrderEvent(
            order=order,
            event_type='filled' if is_full else 'partial',
            fill_price=exec_price,
            fill_qty=fill_qty,
            timestamp=order.timestamp,
            pine_id=pine_id,
            from_entry=from_entry,
            leg_type=leg_type,
            fee=fee,
            fee_currency=order.fee_currency,
            fill_id=str(entry.get('execId') or '') or None,
        )

    def _translate_order_rows(
            self, frame: dict, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Translate the non-fill lifecycle rows of one ``order`` push."""
        events: list[OrderEvent] = []
        for entry in frame.get('data') or ():
            if str(entry.get('symbol') or '') != market.symbol:
                continue
            coid = str(entry.get('orderLinkId') or '')
            if not coid:
                # A position's partial TP/SL order: every status counts,
                # the ones that end its resting too.
                self._observe_foreign_tpsl_order(entry, market)
            event_type = _ORDER_STATUS_EVENTS.get(
                str(entry.get('orderStatus') or ''),
            )
            if event_type is None:
                continue
            order_id = str(entry.get('orderId') or '')
            pine_id, from_entry, leg_type = self._resolve_identity(
                coid or None, order_id or None,
            )
            if leg_type is None:
                continue
            if event_type == 'created':
                # Every resting-order push is judged against the levels and
                # quantities the bot set — never the ``amended`` relabel
                # below: that memory is in-process, so the first push after
                # a restart reads as ``created`` even when it carries an edit.
                if leg_type is LegType.ENTRY:
                    self._observe_entry_push(entry, coid, market)
                else:
                    self._observe_exit_leg_push(entry, coid, leg_type, market)
            if event_type == 'created' and order_id:
                # Bybit re-pushes ``New`` / ``Untriggered`` on the SAME order id
                # after an in-place amend (``POST /v5/order/amend``). The first
                # push is the genuine creation; every later one confirms an
                # amend — relabel it so the lifecycle log reads ``amended``
                # rather than a spurious second ``created``.
                if order_id in self._created_order_ids:
                    event_type = 'amended'
                else:
                    self._created_order_ids.add(order_id)
            if event_type in ('cancelled', 'rejected') \
                    and self.store_ctx is not None and coid:
                # A parked (unknown-disposition) dispatch that resolved to a
                # terminal cancel / reject is done — drop its engine park
                # alongside the store row (``record_unpark`` is idempotent).
                self.store_ctx.record_unpark(coid)
                # The REST cancel ACK retires the row synchronously; the venue
                # then echoes the same terminal transition on this private
                # ``order`` push. The retire is a no-op on an already closed
                # row (``close_order`` appends an ``order_closed`` audit event
                # every call), and the push's ``cumExecQty`` refines the
                # executed quantity a kept entry row owns.
                self._retire_cancelled_row(
                    coid, _cum_exec_qty(entry),
                    str(entry.get('orderId') or '') or None,
                )
            order = parse_exchange_order(entry)
            if market.is_inverse:
                order = self._inverse_order_to_base(order)
            cancel_reason = (
                _CANCEL_TYPE_REASONS.get(str(entry.get('cancelType') or ''))
                if event_type == 'cancelled' else None
            )
            events.append(OrderEvent(
                order=order,
                event_type=event_type,
                fill_price=None,
                fill_qty=None,
                timestamp=order.timestamp or epoch_time(),
                pine_id=pine_id,
                from_entry=from_entry,
                leg_type=leg_type,
                cancel_reason=cancel_reason,
            ))
        return events

    # --- reconcile passes ------------------------------------------------------

    async def _run_deriv_reconcile(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Refresh the venue position snapshot + run disappearance detection.

        The position REST read is the gap-fill backstop of the ``position``
        push (a reconnect can drop pushes); transient failures are logged
        and swallowed — the stream must survive them, the next pass
        retries. The disappearance pass then stamps / retires the resting
        orders + settled positions that vanished behind the engine's back
        (see :meth:`_reconcile_disappearance`), emitting the synthetic
        cancelled events and re-raising a halting-policy escalation. The
        execution backfill is the cadence safety net for the reconnect
        catch-up: a disappearance FILLED verdict deliberately books no fill
        slice (:meth:`_confirm_vanished_order`) and relies on this pass to
        deliver the missed fill event — through the time-windowed backfill,
        or the per-order recovery once the venue's ``cumExecQty`` proves an
        execution the window did not return.
        """
        await self._resolve_parked_dispatches(market)
        await self._settle_cancelled_entries(market)
        rows: list[dict] | None
        read_started_at = epoch_time()
        try:
            rows = await self._fetch_position_rows(market)
        except Exception as exc:  # noqa: BLE001 - the reconcile pass must not kill the stream
            logger.warning(
                "Bybit derivative position reconcile pass failed (transient): %s",
                exc, exc_info=traceback_wanted(exc),
            )
            rows = None
        if rows is not None:
            self._ingest_position_sizes(rows)
            self._close_entry_rows_when_flat(market)
            self._observe_position_rows(rows, market,
                                        read_started_at=read_started_at)
        events = await self._reconcile_disappearance(market, rows)
        await self._note_unbooked_market_entries(market)
        events.extend(await self._run_deriv_fill_backfill(market))
        events.extend(await self._recover_unbooked_order_fills(market))
        # Judge what the fresh views and recovered executions now prove, and
        # give up the position intervals that stayed open past the grace; a
        # flat sweep that waited for them runs now.
        self._ledger_advance(market)
        self._close_entry_rows_when_flat(market)
        return events

    async def _run_spot_reconcile(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Run one inventory reconcile pass, emitting recovered fills.

        Transient failures are logged and swallowed (the manager already
        skips its cycle on a failed balance read); a deliberate halt
        propagates so the engine performs its graceful stop.
        """
        await self._resolve_parked_dispatches(market)
        await self._settle_cancelled_entries(market)
        manager = self._spot_manager
        if manager is None:
            return []
        try:
            recovered = await manager.reconcile(int(epoch_time() * 1000))
        except BrokerManualInterventionError:
            raise
        except Exception as exc:  # noqa: BLE001 - the reconcile pass must not kill the stream
            logger.warning(
                "Bybit spot inventory reconcile pass failed (transient): %s",
                exc, exc_info=traceback_wanted(exc),
            )
            return []
        self._raise_pending_halt()
        events: list[OrderEvent] = []
        for row in recovered:
            event = self._recovered_row_event(row, market)
            if event is not None:
                events.append(event)
        # Sweep on every pass (not only on recovered fills): it also
        # closes the parent rows deferred while the engine's
        # ``defensive_close_pending`` marker was still set.
        self._close_entry_rows_when_flat(market)
        # Spot resting exit legs are disappearance-tracked the same way as
        # the derivatives (open-orders diff on the spot category); spot has
        # no position object, so the settled-exposure side stays owned by
        # the inventory balance invariant.
        events.extend(await self._reconcile_disappearance(market, None))
        return events

    def _recovered_row_event(
            self, row: 'SpotExecutionRow', market: 'InstrumentInfo',
    ) -> OrderEvent | None:
        """Build the OrderEvent of one catch-up-recovered ledger row.

        The ledger stores fee-adjusted deltas; the executed quantity is
        reconstructed by undoing the base-fee adjustment so the engine's
        position math sees the same qty a live push would have carried.
        """
        if row.fill_id in self._seen_exec_ids:
            return None
        self._seen_exec_ids.add(row.fill_id)
        coid = row.client_order_id or ''
        pine_id, from_entry, leg_type = self._resolve_identity(
            coid or None, row.exchange_order_id,
        )
        if leg_type is None:
            # The ledger only books attributable fills, so an unresolved
            # identity can only mean a restart lost the in-memory map AND
            # the store row — emit as an entry-less fill would corrupt
            # the position, so log and drop (startup adoption will fold it).
            logger.warning(
                "Bybit recovered fill %s has no resolvable identity; "
                "left to startup adoption", row.fill_id,
            )
            return None
        base_delta = abs(Decimal(row.base_delta))
        fee = Decimal(row.fee_amount)
        if row.fee_currency == market.base_coin:
            qty = base_delta + fee if row.side == 'buy' else base_delta - fee
        else:
            qty = base_delta
        entry = {
            'execId': row.fill_id,
            'orderId': row.exchange_order_id or '',
            'execQty': str(qty),
            'execPrice': row.price,
            'execFee': row.fee_amount,
            'feeCurrency': row.fee_currency,
            'execTime': str(row.ts_ms),
            'side': row.side.capitalize(),
        }
        # ``_seen_exec_ids`` already holds the id, so re-processing is
        # impossible; the shared builder keeps the event shape identical
        # to the live path.
        return self._fill_event(
            entry, market, coid=coid, pine_id=pine_id,
            from_entry=from_entry, leg_type=leg_type,
        )

    # --- derivative stream-gap fill recovery (F4) ------------------------------

    async def _run_deriv_fill_backfill(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Recover missed derivative fills from the durable execution cursor.

        Reads ``/v5/execution/list`` from the time watermark forward (per
        deriv category, every ``execType``), walking gaps longer than the
        endpoint's 7-day span cap window by window, each window drained
        NEWEST-FIRST to completion before the watermark advances — exactly
        the spot inventory port's discipline. Each ``Trade`` row is a fill
        the private PUSH stream may have dropped across a reconnect; it is
        translated by the SAME builder the live ``execution`` path uses
        (:meth:`_fill_event`), so the emitted event shape is identical. The
        other rows reach the position ledger only.

        Double-apply barriers, in order: the shared ``execId`` frontier
        (:attr:`_seen_exec_ids`) skips a fill the PUSH stream already
        delivered; a live row already fully covered by its ``filled_qty``
        cursor (the F2 adoption baseline, or a completed order) is a no-op;
        an unattributable execution is foreign activity, logged and skipped.

        Gated on :attr:`_adoption_baselined` so the F2 baseline (which
        seeds live rows' cursors + ``execId`` de-dup from per-order venue
        truth) always precedes the first backfill — a backfill before
        adoption could re-apply an already-adopted slice. Transient read failure leaves the watermark
        unadvanced and returns the events booked from the windows that DID
        drain; the next cadence pass re-reads. A
        :class:`BrokerManualInterventionError` propagates.

        A pass that drained every window, at least the read overlap after the
        private stream's latest open, lifts the position ledgers' outage
        hold (:attr:`_ledger_stream_opened_ms`).
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return []
        if not self._adoption_baselined:
            # The adoption baseline has not run yet (the engine's startup
            # ``get_position`` seeds it); defer so F2 precedes F4.
            return []
        now_ms = int(epoch_time() * 1000)
        events: list[OrderEvent] = []
        drained = False
        try:
            watermark = self._deriv_exec_watermark
            if watermark is None:
                loaded = self._load_deriv_exec_watermark(market)
                if loaded is None:
                    # Fresh run: anchor at the ADOPTION FLOOR (the venue
                    # clock stamped before the F2 adoption snapshot — the
                    # guard above proves the baseline committed, so the
                    # floor is set) and fall through to drain up to now.
                    # Anchoring at ``now`` instead would permanently skip a
                    # WS-missed fill that landed between the adoption
                    # commit and this first pass. The floor-to-now re-read
                    # is double-apply-safe: every pre-adoption execution of
                    # a live row is execId-seeded by the baseline, and the
                    # rest is skipped as foreign / cursor-covered. The
                    # drain loop persists the watermark as windows advance.
                    watermark = self._deriv_exec_floor_ms
                    self._deriv_exec_watermark = watermark
                else:
                    watermark = loaded
                    self._deriv_exec_watermark = loaded
                    self._deriv_exec_persisted_ms = loaded
            # The adoption floor (venue-clocked, stamped BEFORE the adoption
            # snapshot) caps how far back a resumed watermark may reach: the
            # F2 baseline owns everything below it — live rows' pre-adoption
            # fills are execId-seeded, the rest is folded into the adopted
            # size — so re-reading (and re-emitting) a pre-adoption slice
            # would double-count on top of the adopted size. A post-adoption
            # fill's ``execTime`` is at or above the floor, so nothing
            # genuinely new is ever clamped away.
            if watermark < self._deriv_exec_floor_ms:
                watermark = self._deriv_exec_floor_ms
                self._deriv_exec_watermark = watermark
            while watermark < now_ms:
                # The window is anchored at the OVERLAPPED start — anchoring
                # at the raw watermark would request WINDOW + OVERLAP ms and
                # blow the endpoint's 7-day span cap on a long catch-up gap.
                # The overlap must not dip below the adoption floor either.
                start = max(0, watermark - EXECUTION_CURSOR_OVERLAP_MS,
                            self._deriv_exec_floor_ms)
                window_end = min(start + EXECUTION_WINDOW_MS, now_ms)
                entries, conclusive = await self._drain_deriv_exec_window(
                    market, start, window_end,
                )
                entries.sort(key=execution_sort_key)
                with self._ledger_batch(market):
                    for entry in entries:
                        events.extend(self._backfill_one_execution(entry, market))
                if not conclusive:
                    # The window did not drain — leave the watermark where it
                    # is (the execIds seen so far are in the dedup frontier, a
                    # re-read is free) and retry on the next cadence pass.
                    break
                watermark = window_end
                self._deriv_exec_watermark = window_end
                self._persist_deriv_exec_watermark(market, window_end)
            else:
                drained = True
        except BrokerManualInterventionError:
            raise
        except Exception as exc:  # noqa: BLE001 - the backfill must not kill the stream
            logger.warning(
                "Bybit derivative fill backfill failed (transient): %s",
                exc, exc_info=traceback_wanted(exc),
            )
        opened_ms = self._ledger_stream_opened_ms
        if drained and opened_ms is not None \
                and now_ms >= opened_ms + EXECUTION_CURSOR_OVERLAP_MS:
            # Every window up to now drained, the stream's outage included,
            # and late enough that an execution the venue indexed late is in
            # it too (the overlap re-read covers that much indexing lag): the
            # position ledgers may prove intervals again. The caller's
            # reconcile pass judges once the per-order fill recovery ran too.
            self._ledger_stream_opened_ms = None
        if events:
            self._close_entry_rows_when_flat(market)
        return events

    async def _recover_unbooked_order_fills(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Book the executions the venue reports for an order but the row lacks.

        The time-windowed backfill re-reads only a fixed overlap behind its
        watermark, so an execution the venue indexes later than that is never
        returned by it. The rows queued in :attr:`_unbooked_fill_orders` carry
        venue proof that such executions exist (``cumExecQty`` above the
        venue-domain cursor, :func:`venue_filled_qty`); each one's executions
        are read by ``orderId`` from its dispatch on, which no watermark can
        outrun, and the unseen ones go through
        :meth:`_backfill_one_execution` — the same gates and event builder as
        the windowed backfill. A row stays queued until its cursor covers the
        venue figure, so an execution not yet indexed is retried every
        reconcile pass instead of being lost. Deferred, queue kept, until the
        adoption baseline has latched (the windowed backfill's barrier).
        """
        pending = self._unbooked_fill_orders
        if not pending or self.store_ctx is None or market.category == CATEGORY_SPOT:
            return []
        if not self._adoption_baselined:
            # Same barrier as the windowed backfill: the adoption baseline
            # may be folding these very executions into the adopted
            # position. The queue is kept; the baseline's cursor seed
            # settles what it owns before the first per-order read.
            return []
        events: list[OrderEvent] = []
        for coid, (order_id, cum_exec) in list(pending.items()):
            row = self.store_ctx.get_order(coid)
            if row is None or venue_filled_qty(row) >= float(cum_exec) - _FILL_EPS:
                pending.pop(coid, None)
                continue
            entries, conclusive = await self._order_executions(
                market, order_id, row.created_ts_ms - EXECUTION_SINCE_SKEW_MS,
            )
            if not conclusive:
                continue
            entries.sort(key=execution_sort_key)
            with self._ledger_batch(market):
                for entry in entries:
                    events.extend(self._backfill_one_execution(entry, market))
            row = self.store_ctx.get_order(coid)
            if row is None or venue_filled_qty(row) >= float(cum_exec) - _FILL_EPS:
                pending.pop(coid, None)
        if events:
            self._close_entry_rows_when_flat(market)
        return events

    async def _drain_deriv_exec_window(
            self, market: 'InstrumentInfo', start_ms: int, end_ms: int,
    ) -> tuple[list[dict], bool]:
        """Drain every execution page of one time window (newest-first API).

        Returns the raw execution rows and whether the window drained
        completely. Every ``execType`` is read: the non-trade rows carry no
        order fill of the run, but a size-changing one (ADL, takeover,
        settlement, a moved position) the stream missed must still reach
        the position ledger, which cannot prove the interval around it
        without it. A transport failure mid-pagination returns
        ``conclusive=False`` so the caller never advances the watermark past
        a window it could not fully read.
        """
        entries: list[dict] = []
        cursor: str | None = None
        for _ in range(_MAX_BACKFILL_WINDOW_PAGES):
            try:
                result = await self._call('/v5/execution/list', {
                    'category': market.category,
                    'symbol': market.symbol,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'limit': EXECUTION_PAGE_LIMIT,
                    'cursor': cursor,
                }, auth=True)
            except BybitError as exc:
                logger.warning(
                    "Bybit derivative fill backfill: execution/list read "
                    "failed for %s: %s", market.symbol, exc,
                    exc_info=traceback_wanted(exc),
                )
                return entries, False
            for entry in result.get('list') or []:
                entries.append(entry)
            cursor = result.get('nextPageCursor') or None
            if not cursor:
                return entries, True
        logger.error(
            "Bybit derivative fill backfill: window exceeded %d pages for %s; "
            "treating as inconclusive", _MAX_BACKFILL_WINDOW_PAGES, market.symbol,
        )
        return entries, False

    def _backfill_one_execution(
            self, entry: dict, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Translate one backfilled execution row into fill event(s), or skip.

        Mirrors the per-row gate of :meth:`_translate_executions`: wrong
        symbol / category and already-seen ``execId`` are dropped, a
        non-``Trade`` row goes to the position ledger only
        (:meth:`_note_non_trade_execution`), an unattributable row is logged
        as foreign and skipped, and
        the survivor goes through the shared :meth:`_fill_event` builder —
        a venue-bracket row closing a pyramid's shared 'Full' stop splits
        into one event per owning exit, exactly like the live stream. The
        extra deriv barrier is the ``filled_qty`` cursor diff: a row the
        adoption baseline (or a completed order) already covers to its full
        size is a no-op — re-booking would double-count on top of the
        adopted position.
        """
        if str(entry.get('symbol') or '') != market.symbol:
            return []
        if str(entry.get('category') or market.category) != market.category:
            return []
        if str(entry.get('execType') or 'Trade') != 'Trade':
            self._note_non_trade_execution(entry, market)
            return []
        exec_id = str(entry.get('execId') or '')
        if not exec_id or exec_id in self._seen_exec_ids:
            return []
        coid = str(entry.get('orderLinkId') or '')
        pine_id, from_entry, leg_type = self._resolve_identity(
            coid or None, str(entry.get('orderId') or '') or None,
        )
        claims: list[tuple[str, str | None, LegType, float]] = []
        ambiguous = False
        if leg_type is None:
            # Second chance, mirroring the live-stream gate: a
            # venue-materialised trading-stop close is ours when the durable
            # bracket-ownership row says so — a reconnect backfill delivering
            # it late must attribute it the same way the stream would have.
            claims, ambiguous = self._attribute_venue_bracket_execution(
                entry, market,
            )
        if leg_type is None and not claims:
            # External activity (manual trade, another bot): never book it.
            # Seed the id so the fixed overlap re-read does not re-log it
            # every pass; the position adoption / disappearance pass own any
            # interference with this strategy's exposure, and a manual close
            # of the run's exposure the stream missed is reported here.
            self._seen_exec_ids.add(exec_id)
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'external_activity_ignored',
                    exchange_order_id=str(entry.get('orderId') or '') or None,
                    payload={'exec_id': exec_id, 'source': 'deriv_backfill'},
                )
            self._note_external_execution(entry, market, ambiguous=ambiguous)
            return []
        if coid and self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            if row is not None and row.filled_qty >= row.qty - _FILL_EPS:
                # The row's wire-domain cursor already covers its full size
                # (the F2 adoption baseline seeded it from per-order venue
                # truth, or a prior fill completed it): the size side is
                # already owned, so dedup and skip instead of re-applying
                # the slice.
                self._seen_exec_ids.add(exec_id)
                return []
        self._seen_exec_ids.add(exec_id)
        return self._book_own_execution(
            entry, market, coid=coid, pine_id=pine_id,
            from_entry=from_entry, leg_type=leg_type, claims=claims,
        )

    def _load_deriv_exec_watermark(self, market: 'InstrumentInfo') -> int | None:
        """Read the persisted derivative execution watermark for this run.

        Scans the ``deriv_exec_cursor`` audit events across every process
        instance of this logical run (the newest wins), returning the
        watermark in epoch-ms or ``None`` when none was ever written. A
        cursor whose scope no longer matches (a plugin upgrade changing the
        cursor meaning) is ignored, so a stale watermark can never be
        resumed against a different read shape.
        """
        if self.store_ctx is None:
            return None
        latest: int | None = None
        for _ik, _coid, _eoid, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(_DERIV_EXEC_CURSOR_EVENT):
            if payload.get('cursor_scope') != 'time':
                continue
            if payload.get('category') != market.category:
                continue
            watermark = payload.get('watermark_ms')
            if isinstance(watermark, (int, float)):
                latest = int(watermark)
        return latest

    def _persist_deriv_exec_watermark(
            self, market: 'InstrumentInfo', watermark_ms: int,
    ) -> None:
        """Persist the durable execution watermark to the audit log (throttled).

        Re-written only after the watermark advances at least the read
        overlap since the last persisted value, bounding the append-log
        write rate on a quiet stream (the fixed overlap on the next read
        covers the sub-persistence-granularity slice). The dedup frontier is
        the in-memory :attr:`_seen_exec_ids`, seeded from the very reads that
        moved the watermark — so a resumed cursor and its execIds always
        advance together.
        """
        if self.store_ctx is None:
            return
        if watermark_ms - self._deriv_exec_persisted_ms < EXECUTION_CURSOR_OVERLAP_MS:
            return
        self._deriv_exec_persisted_ms = watermark_ms
        self.store_ctx.log_event(
            _DERIV_EXEC_CURSOR_EVENT,
            payload={'category': market.category,
                     'watermark_ms': watermark_ms,
                     'cursor_scope': 'time'},
        )
