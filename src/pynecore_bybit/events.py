"""Live order-event stream mix-in for the Bybit plugin (spot, M2).

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

The spot-inventory reconcile pass (lease heartbeat + balance invariant +
stream-gap catch-up) piggybacks on this loop at a fixed cadence, mirroring
the cTrader plugin's reconcile piggyback pattern.
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
    PRIVATE_WS_BACKOFF_S,
    PRIVATE_WS_TOPICS,
    SPOT_RECONCILE_CADENCE_S,
)
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
        market = await asyncio.to_thread(self._spot_market)
        loop = asyncio.get_running_loop()
        next_reconcile = loop.time() + SPOT_RECONCILE_CADENCE_S
        while True:
            self._raise_pending_halt()
            ws = self._private_ws
            if ws is None or not ws.is_open:
                await self._open_private_ws()
            queue = self._private_events
            assert queue is not None
            now = loop.time()
            if now >= next_reconcile:
                for event in await self._run_spot_reconcile(market):
                    yield event
                next_reconcile = loop.time() + SPOT_RECONCILE_CADENCE_S
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
            for event in self._translate_private_frame(frame, market):
                yield event

    def _raise_pending_halt(self) -> None:
        """Deliver an armed inventory-halt signal on the engine's channel."""
        manager = self._spot_manager
        if manager is None:
            return
        halt = manager.consume_pending_halt()
        if halt is not None:
            raise halt

    # --- private transport ------------------------------------------------------

    async def _open_private_ws(self) -> None:
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
            try:
                await ws.open(api_key=self.config.api_key,
                              api_secret=self.config.api_secret)
                await ws.subscribe(list(PRIVATE_WS_TOPICS))
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
        return []

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
            if str(entry.get('execType') or 'Trade') != 'Trade':
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
        """Close the fully filled entry rows once the ledger is flat.

        Entry rows live as long as the position they opened (parent-row
        lifecycle, cTrader pattern) — see :meth:`_fill_event`. Sub-grid
        fee dust counts as flat, matching :meth:`get_position`. A row
        still carrying the engine's ``defensive_close_pending`` marker is
        left for a later sweep: the engine clears the marker when the
        defensive close settles, and closing the row before that would
        orphan the restart replay of the marker.
        """
        manager = self._spot_manager
        if manager is None or self.store_ctx is None:
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
                self.store_ctx.close_order(row.client_order_id)

    def _fill_event(
            self, entry: dict, market: 'InstrumentInfo', *,
            coid: str, pine_id: str | None, from_entry: str | None,
            leg_type: LegType,
    ) -> OrderEvent | None:
        """Build the OrderEvent of one execution slice."""
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
        order_id = str(entry.get('orderId') or '')
        side = str(entry.get('side') or '').lower()
        total_qty = self._dispatch_qty.get(coid, exec_qty)
        cumulative = self._filled_cum.get(coid, 0.0) + exec_qty
        self._filled_cum[coid] = cumulative
        if self.store_ctx is not None and coid:
            row = self.store_ctx.get_order(coid)
            if row is not None:
                total_qty = row.qty
                cumulative = max(cumulative, row.filled_qty + exec_qty)
                self.store_ctx.set_filled(coid, min(total_qty, cumulative))
        is_full = cumulative >= total_qty - _FILL_EPS
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
            fee_currency=str(entry.get('feeCurrency') or ''),
            reduce_only=leg_type is not LegType.ENTRY,
            client_order_id=coid or None,
        )
        return OrderEvent(
            order=order,
            event_type='filled' if is_full else 'partial',
            fill_price=exec_price,
            fill_qty=exec_qty,
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
            pine_id, from_entry, leg_type = self._resolve_identity(
                coid or None, str(entry.get('orderId') or '') or None,
            )
            if leg_type is None:
                continue
            if event_type in ('cancelled', 'rejected') \
                    and self.store_ctx is not None and coid:
                self.store_ctx.close_order(coid)
            order = parse_exchange_order(entry)
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

    # --- spot inventory reconcile --------------------------------------------------

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
