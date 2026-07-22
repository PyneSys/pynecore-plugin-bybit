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
from decimal import Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING, AsyncIterator

from pynecore.core.broker.exceptions import BrokerManualInterventionError
from pynecore.core.broker.models import (
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
from pynecore.core.plugin import override

from ._base import _BybitBase
from .exceptions import BybitError
from .helpers import (
    CATEGORY_SPOT,
    EXECUTION_CURSOR_OVERLAP_MS,
    EXECUTION_PAGE_LIMIT,
    EXECUTION_WINDOW_MS,
    PRIVATE_WS_BACKOFF_S,
    PRIVATE_WS_TOPIC_POSITION,
    PRIVATE_WS_TOPICS,
    RECONCILE_CADENCE_S,
)
from .positions import POSITION_MODE_HEDGE
from .state import parse_exchange_order
from .ws import BybitWebSocket

if TYPE_CHECKING:
    from pynecore.core.broker.storage import SpotExecutionRow

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

#: Small float slack when comparing cumulative fills against the dispatch
#: quantity — the quantities themselves come from exact decimal strings,
#: the slack only absorbs the float round-trip.
_FILL_EPS = 1e-9

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


class _EventStreamMixin(_BybitBase):
    """Order-event PUSH stream: private ``order`` + ``execution`` topics."""

    @override
    async def watch_orders(self) -> AsyncIterator[OrderEvent]:
        """Stream order status updates, driving the inventory reconcile inline.

        Runs for the lifetime of the strategy on the broker event loop.
        The private WS is (re)opened here with bounded backoff; each
        iteration blocks on the frame queue only until the next reconcile
        deadline, so a quiet stream cannot starve the invariant check and
        a busy stream cannot starve it either (deadline checked at the
        loop top).
        """
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._broker_market)
        is_deriv = market.category != CATEGORY_SPOT
        loop = asyncio.get_running_loop()
        next_reconcile = loop.time() + RECONCILE_CADENCE_S
        while True:
            self._raise_pending_halt()
            for event in self._replay_pre_adoption_frames(market):
                yield event
            ws = self._private_ws
            if ws is None or not ws.is_open:
                await self._open_private_ws(market)
                if is_deriv:
                    # A (re)connect closes the stream-gap: any fill that
                    # landed while the private PUSH was down is read back
                    # from the durable execution cursor. The first open at
                    # startup only seeds the watermark (nothing prior is the
                    # bot's gap); every later open recovers the outage window.
                    for event in await self._run_deriv_fill_backfill(market):
                        yield event
            queue = self._private_events
            assert queue is not None
            now = loop.time()
            if now >= next_reconcile:
                recovered = (await self._run_deriv_reconcile(market)
                             if is_deriv
                             else await self._run_spot_reconcile(market))
                for event in recovered:
                    yield event
                next_reconcile = loop.time() + RECONCILE_CADENCE_S
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

    # --- private transport ------------------------------------------------------

    async def _open_private_ws(self, market: 'InstrumentInfo') -> None:
        """(Re)open + authenticate the private stream, with bounded backoff.

        Never raises for transient trouble: credential validity was
        already proven by the account-identity latch at startup, so a
        failing private connect is treated as recoverable and retried
        forever — halting the bot on it would strand the user's open
        exposure. The backoff sleep is the retry pacing tick, bounded by
        :data:`PRIVATE_WS_BACKOFF_S`.
        """
        attempt = 0
        host = self._hosts.ws_private
        while True:
            self._raise_pending_halt()
            if not host:
                raise BrokerManualInterventionError(
                    f"Bybit region {self.config.region!r} has no private "
                    f"stream host; set the ws_private_host override"
                )
            queue: asyncio.Queue[dict | None] = asyncio.Queue()

            def _on_message(data: dict, q: asyncio.Queue = queue) -> None:
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
            except BybitError as e:
                await ws.close()
                delay = PRIVATE_WS_BACKOFF_S[min(attempt, len(PRIVATE_WS_BACKOFF_S) - 1)]
                attempt += 1
                logger.warning(
                    "Bybit private WS connect failed (attempt %d, retry in %.0fs): %s",
                    attempt, delay, e,
                )
                await asyncio.sleep(delay)
                continue
            self._private_ws = ws
            self._private_events = queue
            if attempt:
                logger.info("Bybit private WS reconnected after %d attempt(s)", attempt)
            return

    async def _close_private_ws(self) -> None:
        """Drop the private transport (idempotent)."""
        ws = self._private_ws
        self._private_ws = None
        self._private_events = None
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
        the push only keeps the flat sweep's view fresh between reconcile
        snapshots.
        """
        rows = [
            entry for entry in frame.get('data') or ()
            if str(entry.get('symbol') or '') == market.symbol
            and str(entry.get('category') or market.category) == market.category
        ]
        if not rows:
            return
        self._ingest_position_sizes(rows)
        self._close_entry_rows_when_flat(market)

    def _translate_executions(
            self, frame: dict, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Book + emit the fill slices of one ``execution`` push."""
        events: list[OrderEvent] = []
        manager = self._spot_manager
        port = self._spot_port
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
                # ADL / bust rows) carry no order fill: funding is a
                # settle-coin cashflow the wallet read reflects, the
                # size-changing exotics surface through the venue-
                # authoritative position read. No OrderEvent slot exists
                # for a fill-less cash event (cTrader skips swaps the
                # same way).
                continue
            exec_id = str(entry.get('execId') or '')
            if not exec_id or exec_id in self._seen_exec_ids:
                continue
            coid = str(entry.get('orderLinkId') or '')
            pine_id, from_entry, leg_type = self._resolve_identity(
                coid or None, str(entry.get('orderId') or '') or None,
            )
            if leg_type is None:
                # External activity (manual trade, another bot): it must
                # not move this strategy's position. The balance invariant
                # is the guard that catches base-asset interference.
                if self.store_ctx is not None:
                    self.store_ctx.log_event(
                        'external_activity_ignored',
                        exchange_order_id=str(entry.get('orderId') or '') or None,
                        payload={'exec_id': exec_id},
                    )
                continue
            if manager is not None and port is not None:
                execution = port.to_execution(entry)
                if execution is None or not manager.record_live_fill(execution):
                    # Unparsable (already logged by the port) or a replay
                    # the ledger already booked — do not emit.
                    self._seen_exec_ids.add(exec_id)
                    continue
            self._seen_exec_ids.add(exec_id)
            event = self._fill_event(
                entry, market, coid=coid, pine_id=pine_id,
                from_entry=from_entry, leg_type=leg_type,
            )
            if event is not None:
                events.append(event)
        if events:
            self._close_entry_rows_when_flat(market)
        return events

    def _close_entry_rows_when_flat(self, market: 'InstrumentInfo') -> None:
        """Close the fully filled entry rows once the position is gone.

        Entry rows live as long as the position they opened (parent-row
        lifecycle, cTrader pattern) — see :meth:`_fill_event`. The
        flatness source is category-specific: spot reads the inventory
        ledger (sub-grid fee dust counts as flat, matching
        :meth:`get_position`); linear reads the last-known venue position
        cache, which starts unknown — the sweep never fires on ignorance.
        A row still carrying the engine's ``defensive_close_pending``
        marker is left for a later sweep: the engine clears the marker
        when the defensive close settles, and closing the row before that
        would orphan the restart replay of the marker.
        """
        if self.store_ctx is None:
            return
        if market.category != CATEGORY_SPOT:
            if not self._deriv_is_flat():
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

    def _reduce_hedge_entry_ownership(self, side: str, qty: float) -> None:
        """Apply one filled hedge close to this run's durable entry rows.

        Hedge-mode ownership cannot be reconstructed from Bybit's two
        account-wide aggregate legs. Entry rows therefore carry the run-owned
        slice across restart. A reversal is non-flat after its residual opens,
        so the ordinary flat sweep cannot retire the entry rows consumed by
        its close leg; leaving them live would cancel the new opposite entry
        out of the durable signed sum on the next restart.

        Consume the opposite-side entry rows FIFO. A partial reduction shrinks
        the row to its residual owned quantity; a full reduction retires only
        that physical row. It deliberately does not complete the shared intent
        key because a same-ID reversal may already own a fresh residual entry
        envelope under that key.
        """
        if (self.store_ctx is None
                or self._position_mode != POSITION_MODE_HEDGE
                or qty <= 0.0):
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
            residual = row.filled_qty - consumed
            if residual <= _FILL_EPS:
                self.store_ctx.close_order(row.client_order_id)
            else:
                self.store_ctx.upsert_order(
                    row.client_order_id,
                    qty=residual,
                    filled_qty=residual,
                )
            remaining -= consumed

    def _fill_event(
            self, entry: dict, market: 'InstrumentInfo', *,
            coid: str, pine_id: str | None, from_entry: str | None,
            leg_type: LegType,
    ) -> OrderEvent | None:
        """Build the OrderEvent of one execution slice.

        Quantities compare in the wire domain (exact against the
        dispatched value); on inverse the core-facing event converts to
        base at the dispatch's recorded anchor (falling back to the
        execution price when the anchor is unresolvable — a restart crash
        window) and the slice folds into the net-position mirror. The
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
        if self.store_ctx is not None and coid:
            # A MARKET entry / CLOSE that parked on an unknown-disposition
            # response and then filled never re-enters ``get_open_orders``,
            # so its engine park would linger until the next restart; the
            # fill is the proof it landed. ``record_unpark`` is idempotent
            # (DELETE-by-coid), a no-op for the common not-parked fill.
            self.store_ctx.record_unpark(coid)
            row = self.store_ctx.get_order(coid)
            if row is not None:
                total_qty = row.qty
                cumulative = max(cumulative, row.filled_qty + exec_qty)
                self.store_ctx.set_filled(coid, min(total_qty, cumulative))
        is_full = cumulative >= total_qty - _FILL_EPS
        fill_qty = exec_qty
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
            anchor = self._inverse_anchor_for(coid, fallback=exec_price)
            assert anchor is not None  # exec_price > 0 guarantees a fallback
            factor = float(anchor)
            fill_qty = exec_qty / factor
            total_qty = total_qty / factor
            cumulative = cumulative / factor
            self._apply_inverse_fill(side, exec_qty, fill_qty)
            # Inverse fees are charged in the settle coin, but the core
            # books the numeric fee in the quote-currency P&L domain —
            # normalize at the execution price (the fee's own fill).
            fee = fee * exec_price
            fee_currency = market.quote_coin
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
        if leg_type is LegType.CLOSE:
            self._reduce_hedge_entry_ownership(side, exec_qty)
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
            event_type = _ORDER_STATUS_EVENTS.get(
                str(entry.get('orderStatus') or ''),
            )
            if event_type is None:
                continue
            coid = str(entry.get('orderLinkId') or '')
            order_id = str(entry.get('orderId') or '')
            pine_id, from_entry, leg_type = self._resolve_identity(
                coid or None, order_id or None,
            )
            if leg_type is None:
                continue
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
                # The REST cancel ACK closes the row synchronously; the venue
                # then echoes the same terminal transition on this private
                # ``order`` push. ``close_order`` appends an ``order_closed``
                # audit event every call, so re-closing an already-closed row
                # would duplicate the terminal audit trail — only close a row
                # that is still live.
                row = self.store_ctx.get_order(coid)
                if row is not None and row.closed_ts_ms is None:
                    self.store_ctx.close_order(coid)
            order = parse_exchange_order(entry)
            if market.is_inverse:
                order = self._inverse_order_to_base(order)
            events.append(OrderEvent(
                order=order,
                event_type=event_type,
                fill_price=None,
                fill_qty=None,
                timestamp=order.timestamp or epoch_time(),
                pine_id=pine_id,
                from_entry=from_entry,
                leg_type=leg_type,
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
        deliver the missed fill event.
        """
        rows: list[dict] | None
        try:
            rows = await self._fetch_position_rows(market)
        except Exception as exc:  # noqa: BLE001 - the reconcile pass must not kill the stream
            logger.warning(
                "Bybit derivative position reconcile pass failed (transient): %s",
                exc, exc_info=True,
            )
            rows = None
        if rows is not None:
            self._ingest_position_sizes(rows)
            self._close_entry_rows_when_flat(market)
        events = await self._reconcile_disappearance(market, rows)
        events.extend(await self._run_deriv_fill_backfill(market))
        return events

    async def _run_spot_reconcile(
            self, market: 'InstrumentInfo',
    ) -> list[OrderEvent]:
        """Run one inventory reconcile pass, emitting recovered fills.

        Transient failures are logged and swallowed (the manager already
        skips its cycle on a failed balance read); a deliberate halt
        propagates so the engine performs its graceful stop.
        """
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
                exc, exc_info=True,
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
        deriv category, ``execType=Trade``), walking gaps longer than the
        endpoint's 7-day span cap window by window, each window drained
        NEWEST-FIRST to completion before the watermark advances — exactly
        the spot inventory port's discipline. Each execution row is a fill
        the private PUSH stream may have dropped across a reconnect; it is
        translated by the SAME builder the live ``execution`` path uses
        (:meth:`_fill_event`), so the emitted event shape is identical.

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
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return []
        if not self._adoption_baselined:
            # The adoption baseline has not run yet (the engine's startup
            # ``get_position`` seeds it); defer so F2 precedes F4.
            return []
        now_ms = int(epoch_time() * 1000)
        events: list[OrderEvent] = []
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
                entries.sort(key=lambda e: int(e.get('execTime') or 0))
                for entry in entries:
                    event = self._backfill_one_execution(entry, market)
                    if event is not None:
                        events.append(event)
                if not conclusive:
                    # The window did not drain — leave the watermark where it
                    # is (the execIds seen so far are in the dedup frontier, a
                    # re-read is free) and retry on the next cadence pass.
                    break
                watermark = window_end
                self._deriv_exec_watermark = window_end
                self._persist_deriv_exec_watermark(market, window_end)
        except BrokerManualInterventionError:
            raise
        except Exception as exc:  # noqa: BLE001 - the backfill must not kill the stream
            logger.warning(
                "Bybit derivative fill backfill failed (transient): %s",
                exc, exc_info=True,
            )
        if events:
            self._close_entry_rows_when_flat(market)
        return events

    async def _drain_deriv_exec_window(
            self, market: 'InstrumentInfo', start_ms: int, end_ms: int,
    ) -> tuple[list[dict], bool]:
        """Drain every execution page of one time window (newest-first API).

        Returns the raw execution rows and whether the window drained
        completely. ``execType=Trade`` filters the query (non-trade rows —
        funding / settle / ADL / bust — carry no order fill and have
        unverified field shapes). A transport failure mid-pagination returns
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
                    'execType': 'Trade',
                    'limit': EXECUTION_PAGE_LIMIT,
                    'cursor': cursor,
                }, auth=True)
            except BybitError:
                logger.warning(
                    "Bybit derivative fill backfill: execution/list read "
                    "failed for %s", market.symbol, exc_info=True,
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
    ) -> OrderEvent | None:
        """Translate one backfilled execution row into a fill event, or skip.

        Mirrors the per-row gate of :meth:`_translate_executions`: wrong
        symbol / category / exec-type and already-seen ``execId`` are
        dropped, an unattributable row is logged as foreign and skipped, and
        the survivor goes through the shared :meth:`_fill_event` builder. The
        extra deriv barrier is the ``filled_qty`` cursor diff: a row the
        adoption baseline (or a completed order) already covers to its full
        size is a no-op — re-booking would double-count on top of the
        adopted position.
        """
        if str(entry.get('symbol') or '') != market.symbol:
            return None
        if str(entry.get('category') or market.category) != market.category:
            return None
        if str(entry.get('execType') or 'Trade') != 'Trade':
            return None
        exec_id = str(entry.get('execId') or '')
        if not exec_id or exec_id in self._seen_exec_ids:
            return None
        coid = str(entry.get('orderLinkId') or '')
        pine_id, from_entry, leg_type = self._resolve_identity(
            coid or None, str(entry.get('orderId') or '') or None,
        )
        if leg_type is None:
            # External activity (manual trade, another bot): never book it.
            # Seed the id so the fixed overlap re-read does not re-log it
            # every pass; the position adoption / disappearance pass own any
            # interference with this strategy's exposure.
            self._seen_exec_ids.add(exec_id)
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'external_activity_ignored',
                    exchange_order_id=str(entry.get('orderId') or '') or None,
                    payload={'exec_id': exec_id, 'source': 'deriv_backfill'},
                )
            return None
        if coid and self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            if row is not None and row.filled_qty >= row.qty - _FILL_EPS:
                # The row's wire-domain cursor already covers its full size
                # (the F2 adoption baseline seeded it from per-order venue
                # truth, or a prior fill completed it): the size side is
                # already owned, so dedup and skip instead of re-applying
                # the slice.
                self._seen_exec_ids.add(exec_id)
                return None
        event = self._fill_event(
            entry, market, coid=coid, pine_id=pine_id,
            from_entry=from_entry, leg_type=leg_type,
        )
        self._seen_exec_ids.add(exec_id)
        return event

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
