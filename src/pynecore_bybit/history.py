"""Order-history read for the Bybit plugin: the engine's own execution recovery.

Implements :meth:`~pynecore.core.plugin.broker.BrokerPlugin.fetch_order_execution`
for every category (spot, linear, inverse): what the venue executed of one
order, read again from its order record and its executions, so the engine
recovers an execution the private stream never delivered without anyone's
help.

Venue sources (Bybit v5 documentation, "Get Open & Closed Orders", "Get
Order History", "Get Trade History"):

- ``GET /v5/order/realtime`` then ``GET /v5/order/history``, by ``orderId``
  when the engine knows it, by ``orderLinkId`` otherwise (and as the second
  lookup of an order id the first one did not find, when the journal names
  its link id). The ``orderLinkId`` lookup is not bound to the history time
  window (measured on the global demo, see :mod:`~pynecore_bybit.recovery`).
- ``GET /v5/execution/list`` by ``orderId`` (or ``orderLinkId``), walked in
  the endpoint's 7-day windows from the order's creation
  (:meth:`~_RecoveryMixin._order_executions`). Its ``execId`` is the stream's
  ``fill_id``, so the engine takes each execution once on whatever path it
  arrives.

Order history retention (documented): an order that executed anything stays
queryable (fully filled, or partially filled and cancelled, beyond seven
days); a fully cancelled, rejected or deactivated order only for 24 hours.
So an order that shows on neither order endpoint has no execution: that is
the only case answered with ``OrderExecution(order=None)``, and only when the
lookup went by ``orderLinkId``, the journal's dispatch of the order is older
than the indexing grace and the execution list holds nothing of it either.
Bybit documents that the history "may delay" without a bound, so any other
gap is no answer (``None``) and the engine asks again.

Quantities of the order record are in the journal's unit (base on spot and
linear, contracts on inverse, gross of a spot base-coin fee); the fill events
carry the core's units exactly as the stream builds them, with
``journal_fill_qty`` in the journal's. ``cumExecFee`` is deprecated for
linear and spot, so the order's fee is the sum of its executions' ``execFee``,
normalized like the stream's (inverse: to the quote currency at the execution
price).

An entry the plugin handed to the engine at startup
(:meth:`~_RecoveryMixin._hand_over_unreported_execution`) may have had its
position closed while the bot was down: the adoption baseline folds such a
close without an event, so once the engine books the held opening, the plugin
delivers what the venue closed of it as the entry's close fills, behind the
opening, once per ``execId`` (:meth:`~_OrderHistoryMixin._held_entry_closes`).
"""
import asyncio
import logging
import math
from abc import ABC
from dataclasses import replace
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.models import (
    ExchangeOrder,
    LegType,
    OrderEvent,
    OrderExecution,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.store_helpers import ENTRY_KIND_POSITION, ENTRY_KIND_WORKING
from pynecore.core.plugin import override
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _BybitBase
from .events import _VENUE_CLOSEOUT_EXEC_TYPES
from .exceptions import BybitError, classify_read_error
from .execution import _CLOSE_SEND_VISIBILITY_GRACE_MS, _PRIOR_INSTANCE_ORDER_EXTRA
from .helpers import (
    CATEGORY_SPOT,
    ENTRY_EXECUTION_HOLD_EXTRA,
    EXECUTION_HOLD_CLOSE_EVENT,
    EXECUTION_SINCE_SKEW_MS,
    EXECUTION_WINDOW_MS,
    execution_sort_key,
    venue_filled_qty,
    wire_link_id,
)
from .positions import POSITION_MODE_HEDGE
from .state import _ENDED_ORDER_STATUSES, parse_exchange_order

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: Slack when comparing executed quantities: they are exact decimal strings,
#: the slack only absorbs the float round-trip.
_FILL_EPS = 1e-9


class _OrderHistoryMixin(_BybitBase, ABC):
    """The venue read behind the engine's own recovery of an execution."""

    @override
    async def fetch_order_execution(
            self, order_id: str | None, client_order_id: str | None,
    ) -> OrderExecution | None:
        """Read what the venue executed of one order, from its order and execution history.

        Decision table (first matching line wins):

        =====================  ===================  ==================================
        Order record           Executions           Answer
        =====================  ===================  ==================================
        lookup inconclusive    (not read)           ``None``
        found                  read failed          ``None``
        found, ended           sum != ``cumExecQty``  ``None`` (the list lags)
        found                  read                 the record and every execution
        not found, no link id  (not read)           ``None`` (nothing proven)
        not found, young       (not read)           ``None`` (indexing grace)
        not found              read failed / some   ``None``
        not found              none                 ``OrderExecution(order=None)``
        =====================  ===================  ==================================

        An execution of the run's own order the plugin never booked is booked
        here through the same path the stream's backfill takes (its journal
        row, the inverse mirror, the spot inventory ledger and the position
        ledgers move as if the stream had delivered it), and its event also
        goes to the stream's outbox: the engine takes each ``fill_id`` once,
        so a read whose answer the engine does not book (a close's outcome)
        still delivers the fill. For an entry the plugin handed over, what
        the venue closed of its position while the bot was down is booked and
        queued behind the opening as the entry's close fills
        (:meth:`_held_entry_closes`). Nothing is written to the venue.

        :param order_id: The venue order id, ``None`` when only the client
            order id is known.
        :param client_order_id: The client order id the order was sent with
            (the core's form; its wire ``orderLinkId`` is derived).
        :return: The venue's record, ``None`` when it cannot answer now.
        :raises ExchangeConnectionError: Or another broker-taxonomy error
            (:func:`~pynecore_bybit.exceptions.classify_read_error`) when an
            order read fails.
        """
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._broker_market)
        link_id = wire_link_id(client_order_id) if client_order_id else None
        row = self._history_row(order_id, link_id)
        if link_id is None and row is not None:
            link_id = row.client_order_id
        try:
            entry, conclusive = await self._history_order(market, order_id, link_id)
        except BybitError as e:
            raise classify_read_error(e) from e
        if not conclusive:
            return None
        if entry is None:
            return await self._history_absent(market, order_id, link_id, row)
        return await self._history_record(market, entry, link_id, row)

    def _history_row(self, order_id: str | None, link_id: str | None) -> 'OrderRow | None':
        """The journal row of the order asked for, ``None`` when the journal has none."""
        store = self.store_ctx
        if store is None:
            return None
        row = store.get_order_for_run_id(link_id) if link_id else None
        if row is None and order_id:
            row = store.find_by_ref('order_id', order_id)
        if row is not None and (row.extras or {}).get(_PRIOR_INSTANCE_ORDER_EXTRA):
            return None
        return row

    async def _history_order(
            self, market: 'InstrumentInfo', order_id: str | None, link_id: str | None,
    ) -> tuple[dict | None, bool]:
        """Find the venue's order object: by ``orderId``, then by ``orderLinkId``.

        A link id lookup that finds an order with another ``orderId`` than the
        one asked for found another order, so ours is not visible.

        :return: ``(order object or None, conclusive)``.
        :raises BybitError: When an ``orderId`` read fails.
        """
        if order_id:
            for endpoint in ('/v5/order/realtime', '/v5/order/history'):
                result = await self._call(endpoint, {
                    'category': market.category,
                    'symbol': market.symbol,
                    'orderId': order_id,
                }, auth=True)
                entries = result.get('list') or []
                if entries:
                    return entries[0], True
        if not link_id:
            return None, True
        found, conclusive = await self._confirm_lookup(market, link_id)
        if not conclusive:
            return None, False
        if found is not None and order_id and str(found.get('orderId') or '') != order_id:
            return None, True
        return found, True

    async def _history_absent(
            self, market: 'InstrumentInfo', order_id: str | None, link_id: str | None,
            row: 'OrderRow | None',
    ) -> OrderExecution | None:
        """Answer for an order neither order endpoint shows.

        :return: ``OrderExecution(order=None)`` when the venue proves nothing
            of it executed, else ``None``.
        """
        if not link_id:
            # Only an ``orderId`` lookup ran; whether it is bound to the
            # history window is not documented, so its emptiness proves nothing.
            return None
        if row is None:
            # The journal purged the row: the order is older than any
            # indexing lag, and an order with an execution stays queryable.
            return OrderExecution(order=None)
        if int(epoch_time() * 1000) - row.created_ts_ms < _CLOSE_SEND_VISIBILITY_GRACE_MS:
            return None
        entries, complete = await self._order_executions(
            market, order_id or '', row.created_ts_ms - EXECUTION_SINCE_SKEW_MS,
            order_link_id=link_id,
        )
        if not complete or entries:
            # Executions without an order record: the history lags behind.
            return None
        return OrderExecution(order=None)

    async def _history_record(
            self, market: 'InstrumentInfo', entry: dict, link_id: str | None,
            row: 'OrderRow | None',
    ) -> OrderExecution | None:
        """The venue's record of a found order with every execution of it.

        :return: The record, ``None`` when the executions cannot be read in
            full now.
        """
        venue_order_id = str(entry.get('orderId') or '')
        coid = (str(entry.get('orderLinkId') or '') or link_id
                or self._own_order_link_id(None, venue_order_id))
        try:
            created_ms = int(entry.get('createdTime') or 0)
        except (TypeError, ValueError):
            created_ms = 0
        if created_ms <= 0:
            created_ms = row.created_ts_ms if row is not None else int(epoch_time() * 1000)
        executions, complete = await self._order_executions(
            market, venue_order_id, created_ms - EXECUTION_SINCE_SKEW_MS,
            order_link_id=None if venue_order_id else coid or None,
        )
        if not complete:
            return None
        trades = sorted(
            (execution for execution in executions
             if str(execution.get('execType') or 'Trade') == 'Trade'),
            key=execution_sort_key,
        )
        parsed = parse_exchange_order(entry)
        executed = 0.0
        for trade in trades:
            quantity = _finite(trade.get('execQty'))
            if quantity is None or quantity <= 0.0:
                return None
            executed += quantity
        ended = str(entry.get('orderStatus') or '') in _ENDED_ORDER_STATUSES
        if (ended and parsed.filled_qty_known
                and abs(executed - parsed.filled_qty) > _FILL_EPS):
            return None
        identity = self._resolve_identity(coid or None, venue_order_id or None)
        if row is None and coid:
            # The engine reads a held order by its venue id alone, which the
            # journal of a handed-over row does not alias.
            row = self._history_row(None, coid)
        journaled = self._journaled_earlier(market, row, coid)
        recovered: list[OrderEvent] = []
        cumulative = 0.0
        for trade in trades:
            event = self._recovered_fill_event(
                trade, market, coid=coid, identity=identity,
                before=cumulative, total=parsed.qty,
            )
            if event is None:
                return None
            cumulative += _finite(trade.get('execQty')) or 0.0
            if (cumulative <= journaled + _FILL_EPS and event.fill_id
                    and event.fill_id not in self._undelivered_fill_ids.get(coid, ())
                    and self.store_ctx is not None
                    and not self.store_ctx.fill_delivered(event.fill_id)):
                # The journal held this execution before this process booked
                # anything of the order: the position this process started
                # with holds it, so the engine takes only its receipt.
                event = replace(event, journaled_before_restart=True)
            recovered.append(event)
        closes: list[dict] = []
        if ended and row is not None and (row.extras or {}).get(ENTRY_EXECUTION_HOLD_EXTRA):
            found = await self._held_entry_closes(market, row, trades, identity)
            if found is None:
                return None
            closes = found
        booked, close_events = self._book_recovered_executions(
            market, trades, known=identity[2] is not None,
            closes=closes, held_coid=coid, held_pine_id=identity[0],
        )
        fills: list[OrderEvent] = []
        for trade, event in zip(trades, recovered):
            events = booked.get(str(trade.get('execId') or ''))
            fills.extend(events if events else (event,))
        if close_events:
            # The engine books the queue in order: the opening goes ahead of
            # the closes of the position it opened (it takes each ``fill_id``
            # once, so a copy of an opening it booked already is ignored).
            self._event_outbox.extend(fill for fill in fills if fill.fill_id not in booked)
            self._event_outbox.extend(close_events)
        fee, fee_currency, fee_reported = _order_fee(fills)
        if any(_finite(trade.get('execFee')) is None for trade in trades):
            # An execution without its fee: the total cannot be told.
            fee_reported = False
        order = replace(
            parsed,
            symbol=self.symbol or market.symbol,
            fee=fee,
            fee_currency=fee_currency,
            client_order_id=self._core_client_order_id(coid or None),
        )
        return OrderExecution(order=order, fills=tuple(fills), fee_reported=fee_reported)

    def _book_recovered_executions(
            self, market: 'InstrumentInfo', trades: list[dict], *, known: bool,
            closes: list[dict], held_coid: str, held_pine_id: str | None,
    ) -> tuple[dict[str, list[OrderEvent]], list[OrderEvent]]:
        """Book the run's own executions the plugin never booked, as the stream would.

        Derivatives go through the backfill's per-row gate and builder
        (:meth:`~_EventStreamMixin._backfill_one_execution`), spot through the
        stream's translation (:meth:`~_EventStreamMixin._translate_executions`,
        which enters the execution into the inventory ledger). An order whose
        identity does not resolve is not booked (the backfill would log the
        run's own execution as someone else's), nor is anything before the
        adoption baseline latched (it folds these executions itself). The
        events also go to :attr:`~_BybitBase._event_outbox`, the stream's own
        delivery.

        ``closes`` (:meth:`_held_entry_closes`, derivatives only) are booked
        right after the opening, in the same ledger batch and before the flat
        sweep: each one as the held entry's close through the stream's
        builder (:meth:`~_EventStreamMixin._book_execution_once`), with its
        delivery record (:data:`~pynecore_bybit.helpers.EXECUTION_HOLD_CLOSE_EVENT`)
        in the booking's transaction. Their events are returned, not queued:
        the caller queues them behind the opening.

        :param trades: The order's ``Trade`` executions, oldest first.
        :param known: Whether the order's identity resolves.
        :param closes: The venue closes of the held entry's position to book.
        :param held_coid: The held entry's journal row.
        :param held_pine_id: The held entry's Pine id.
        :return: ``execId`` -> the events its booking built, and the close
            events.
        """
        unseen = [trade for trade in trades
                  if str(trade.get('execId') or '') not in self._seen_exec_ids]
        if not known:
            return {}, []
        if not unseen and not closes:
            return {}, []
        spot = market.category == CATEGORY_SPOT
        if not spot and self.store_ctx is not None and not self._adoption_baselined:
            return {}, []
        events: list[OrderEvent] = []
        close_events: list[OrderEvent] = []
        if spot:
            events = self._translate_executions({'data': unseen}, market)
        else:
            start = len(self._event_outbox)
            try:
                with self._ledger_batch(market):
                    for trade in unseen:
                        events.extend(self._backfill_one_execution(trade, market))
                    for close in closes:
                        close_events.extend(self._book_held_entry_close(
                            close, market, held_coid=held_coid, held_pine_id=held_pine_id))
                if events or close_events:
                    self._close_entry_rows_when_flat(market)
            except BaseException:
                self._hand_over_failed_pass(start, [*events, *close_events])
                raise
        self._event_outbox.extend(events)
        booked: dict[str, list[OrderEvent]] = {}
        for event in events:
            if event.fill_id:
                booked.setdefault(event.fill_id, []).append(event)
        return booked, close_events

    @staticmethod
    def _journaled_earlier(market: 'InstrumentInfo', row: 'OrderRow | None', coid: str) -> float:
        """What the order's journal row held executed before this read (wire domain).

        Derivatives only: an execution within it that this process did not
        emit (:attr:`~_BybitBase._undelivered_fill_ids`) and the engine has no
        receipt of was journaled before the engine's startup adoption, so the
        position the engine started with holds it
        (:attr:`~pynecore.core.broker.models.OrderEvent.journaled_before_restart`).
        """
        if market.category == CATEGORY_SPOT or row is None or row.client_order_id != coid:
            return 0.0
        return venue_filled_qty(row)

    def _queue_inherited_hold_closes(self, market: 'InstrumentInfo') -> None:
        """Queue the held-entry closes an earlier process journaled and the engine never took.

        A close of :meth:`_held_entry_closes` is booked into the journal and
        recorded (:data:`~pynecore_bybit.helpers.EXECUTION_HOLD_CLOSE_EVENT`)
        before its event reaches the engine; a process that ended in between
        took the event with it. The engine's startup adoption of the next
        process holds the close (the journal netted it), so the record's fill
        goes out once more, flagged
        :attr:`~pynecore.core.broker.models.OrderEvent.journaled_before_restart`,
        while the engine has no receipt of it
        (:meth:`~pynecore.core.broker.storage.RunContext.fill_delivered`):
        the engine records the receipt and moves no book, so no restart
        delivers it twice. Delivered by the next backfill pass, with the
        inherited entry fills (:attr:`~_BybitBase._inherited_fill_events`).

        :param market: The chart instrument.
        """
        store = self.store_ctx
        if store is None or market.category == CATEGORY_SPOT:
            return
        queued = {event.fill_id for event in self._inherited_fill_events}
        for _ik, _coid, order_id, payload in store.iter_events_by_kind_for_run_id(
                EXECUTION_HOLD_CLOSE_EVENT):
            exec_id = str(payload.get('exec_id') or '')
            fill_qty = _finite(payload.get('fill_qty'))
            price = _finite(payload.get('fill_price'))
            if (not exec_id or exec_id in queued
                    or payload.get('instance') == store.run_instance_id
                    or fill_qty is None or fill_qty <= 0.0 or price is None or price <= 0.0
                    or store.fill_delivered(exec_id)):
                continue
            timestamp = _finite(payload.get('timestamp')) or epoch_time()
            fee = _finite(payload.get('fee')) or 0.0
            fee_currency = str(payload.get('fee_currency') or '')
            order = ExchangeOrder(
                id=order_id or '',
                symbol=self.symbol or market.symbol,
                side=str(payload.get('side') or ''),
                order_type=OrderType.MARKET,
                qty=fill_qty,
                filled_qty=fill_qty,
                remaining_qty=0.0,
                price=None,
                stop_price=None,
                average_fill_price=price,
                status=OrderStatus.FILLED,
                timestamp=timestamp,
                fee=fee,
                fee_currency=fee_currency,
                reduce_only=True,
                client_order_id=None,
            )
            self._inherited_fill_events.append(OrderEvent(
                order=order,
                event_type='filled',
                fill_price=price,
                fill_qty=fill_qty,
                timestamp=timestamp,
                pine_id=None,
                from_entry=str(payload.get('from_entry') or '') or None,
                leg_type=LegType.CLOSE,
                fee=fee,
                fee_currency=fee_currency,
                fill_id=exec_id,
                journal_fill_qty=_finite(payload.get('qty')) or 0.0,
                journaled_before_restart=True,
            ))
            queued.add(exec_id)

    def _book_held_entry_close(
            self, close: dict, market: 'InstrumentInfo', *, held_coid: str,
            held_pine_id: str | None,
    ) -> list[OrderEvent]:
        """Book one venue close of a held entry's position as the entry's close fill.

        The close nets the run's entry rows and the inverse mirror like any
        reducing fill of the stream; the delivery record commits with it and
        keeps the event's figures, so a later process redelivers it while the
        engine has no receipt (:meth:`_queue_inherited_hold_closes`).
        """
        exec_id = str(close.get('execId') or '')
        store = self.store_ctx
        if store is None:
            return []
        with self._journal_transaction():
            events = self._book_execution_once(
                exec_id, close, market, coid='', pine_id=None, from_entry=held_pine_id,
                leg_type=LegType.CLOSE, claims=[],
            )
            payload: dict = {'exec_id': exec_id, 'qty': _finite(close.get('execQty')) or 0.0,
                             'instance': store.run_instance_id}
            for event in events[:1]:
                payload.update(
                    fill_qty=event.fill_qty, fill_price=event.fill_price, fee=event.fee,
                    fee_currency=event.fee_currency, side=event.order.side,
                    timestamp=event.timestamp, from_entry=event.from_entry,
                )
            store.log_event(
                EXECUTION_HOLD_CLOSE_EVENT, client_order_id=held_coid,
                exchange_order_id=str(close.get('orderId') or '') or None,
                payload=payload,
            )
        logger.warning(
            "Bybit: the venue closed %s of handed-over entry %s (execution %s at %s) "
            "while the bot was down; delivered to the engine as the entry's close",
            close.get('execQty'), held_coid, exec_id, close.get('execPrice'),
        )
        return events

    async def _held_entry_closes(
            self, market: 'InstrumentInfo', row: 'OrderRow', trades: list[dict],
            identity: tuple[str | None, str | None, LegType | None],
    ) -> list[dict] | None:
        """The venue closes of a handed-over entry's position the engine has not had.

        The plugin hands an entry to the engine
        (:meth:`~_RecoveryMixin._hand_over_unreported_execution`) only at
        startup, for an order that ended while the bot was down. The adoption
        baseline folds what the venue closed of the position meanwhile
        without an event, and the engine adopts the venue position before it
        books the held opening; once it does, its book holds the closed part
        again. What the venue closed of the held entry is the shortfall of the
        venue leg against the run's other entry rows plus the held execution
        (one-way: the signed position on the entry's side; hedge: the entry
        side's leg), capped at the held execution, less what was delivered
        already (:data:`~pynecore_bybit.helpers.EXECUTION_HOLD_CLOSE_EVENT`).
        It is taken oldest first from the symbol's executions after the held
        order's last one, up to the adoption floor (later ones reached the
        stream): a close of the entry's side (``closedSize`` > 0, Bybit v5
        "Get Trade History") of a trade or a venue closeout, no order of the
        run's own journal (those reduced the run's other entries), whose fill
        the engine has not taken. A close larger than what is left is cut to
        it, its fee pro rata.

        :return: The closes to book, oldest first (cut where needed); ``[]``
            when nothing is owed or the row is no derivative entry; ``None``
            when the venue cannot be read now or the adoption baseline has
            not latched yet (the engine asks again).
        """
        store = self.store_ctx
        held_pine_id = identity[0]
        if (store is None or market.category == CATEGORY_SPOT
                or identity[2] is not LegType.ENTRY or not held_pine_id
                or row.side not in ('buy', 'sell') or not trades):
            return []
        if not self._adoption_baselined or self._deriv_exec_floor_ms <= 0:
            return None
        held_qty = sum(_finite(trade.get('execQty')) or 0.0 for trade in trades)
        delivered: dict[str, float] = {}
        for _ik, coid, _eoid, payload in store.iter_events_by_kind_for_run_id(
                EXECUTION_HOLD_CLOSE_EVENT):
            exec_id = str(payload.get('exec_id') or '')
            if coid == row.client_order_id and exec_id:
                delivered[exec_id] = _finite(payload.get('qty')) or 0.0
        try:
            positions = await self._fetch_position_rows(market)
            venue = self._held_leg_size(positions, row.side)
        except BybitError:
            return None
        other = self._other_entry_exposure(row)
        owed = (min(held_qty, max(0.0, max(other, 0.0) + held_qty - max(venue, 0.0)))
                - sum(delivered.values()))
        if owed <= _FILL_EPS:
            return []
        last = max(trades, key=execution_sort_key)
        start, _ = execution_sort_key(last)
        floor_ms = self._deriv_exec_floor_ms
        candidates: list[dict] = []
        while start < floor_ms:
            end = min(start + EXECUTION_WINDOW_MS, floor_ms)
            entries, conclusive = await self._drain_deriv_exec_window(market, start, end)
            if not conclusive:
                return None
            candidates.extend(entries)
            start = end
        held_ids = {str(trade.get('execId') or '') for trade in trades}
        close_side = 'sell' if row.side == 'buy' else 'buy'
        picked: dict[str, dict] = {}
        for entry in sorted(candidates, key=execution_sort_key):
            exec_id = str(entry.get('execId') or '')
            if (not exec_id or exec_id in held_ids or exec_id in delivered
                    or exec_id in picked
                    or execution_sort_key(entry) <= execution_sort_key(last)
                    or not self._closes_held_side(entry, market, close_side)
                    or store.fill_delivered(exec_id)):
                continue
            picked[exec_id] = entry
        closes: list[dict] = []
        for entry in picked.values():
            if owed <= _FILL_EPS:
                break
            closed = _finite(entry.get('closedSize')) or 0.0
            exec_qty = _finite(entry.get('execQty')) or 0.0
            take = min(closed, exec_qty, owed)
            if take <= _FILL_EPS:
                continue
            if take < exec_qty - _FILL_EPS:
                fee = _finite(entry.get('execFee')) or 0.0
                entry = {**entry, 'execQty': repr(take), 'execFee': repr(fee * take / exec_qty)}
            closes.append(entry)
            owed -= take
        return closes

    def _closes_held_side(self, entry: dict, market: 'InstrumentInfo', close_side: str) -> bool:
        """Whether one symbol execution closed position on the held entry's side.

        A trade or a venue closeout (liquidation, auto-deleveraging, delivery)
        on the closing side with ``closedSize`` > 0, of no order the run's
        journal knows.
        """
        if str(entry.get('symbol') or '') != market.symbol:
            return False
        if str(entry.get('category') or market.category) != market.category:
            return False
        exec_type = str(entry.get('execType') or 'Trade')
        if exec_type != 'Trade' and exec_type not in _VENUE_CLOSEOUT_EXEC_TYPES:
            return False
        if str(entry.get('side') or '').lower() != close_side:
            return False
        closed = _finite(entry.get('closedSize'))
        price = _finite(entry.get('execPrice'))
        if closed is None or closed <= 0.0 or price is None or price <= 0.0:
            return False
        link_id = str(entry.get('orderLinkId') or '')
        order_id = str(entry.get('orderId') or '')
        return self._resolve_identity(link_id or None, order_id or None)[2] is None

    def _held_leg_size(self, positions: list[dict], side: str) -> float:
        """The venue's position on ``side`` (wire domain): one-way signed, hedge the side's leg.

        :raises BybitPositionDataError: For an unparsable position row.
        """
        size = 0.0
        hedge = self._position_mode == POSITION_MODE_HEDGE
        for position in positions:
            quantity = self._position_row_size(position)
            row_side = str(position.get('side') or '').lower()
            if row_side == side:
                size += quantity
            elif not hedge and row_side in ('buy', 'sell'):
                size -= quantity
        return size

    def _other_entry_exposure(self, held: 'OrderRow') -> float:
        """The run's entry rows' exposure on the held entry's side, the held row left out.

        One-way folds the other side's rows against it; hedge counts the
        side's leg only. Wire domain, like the venue's position size.
        """
        assert self.store_ctx is not None
        hedge = self._position_mode == POSITION_MODE_HEDGE
        exposure = 0.0
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            if (row.client_order_id == held.client_order_id
                    or extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                    or extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                    or row.filled_qty <= _FILL_EPS):
                continue
            if row.side == held.side:
                exposure += float(row.filled_qty)
            elif not hedge:
                exposure -= float(row.filled_qty)
        return exposure

    def _recovered_fill_event(
            self, trade: dict, market: 'InstrumentInfo', *, coid: str,
            identity: tuple[str | None, str | None, LegType | None],
            before: float, total: float,
    ) -> OrderEvent | None:
        """The fill event of one execution the plugin booked already, built without state.

        The same quantities and fee normalization as the stream's builder
        (:meth:`~_EventStreamMixin._fill_event`): a spot base-coin fee nets
        the base the execution moved, an inverse execution converts at its
        dispatch's recorded anchor (the execution price without one) and its
        settle-coin fee to the quote currency. The engine takes its
        ``fill_id`` once, so it books this event only when it never took the
        stream's.

        :param before: The order's executed quantity before this execution
            (journal unit).
        :param total: The order's quantity (journal unit).
        :return: The event, ``None`` for an unparsable execution.
        """
        exec_qty = _finite(trade.get('execQty'))
        price = _finite(trade.get('execPrice'))
        fee = _finite(trade.get('execFee')) or 0.0
        exec_id = str(trade.get('execId') or '')
        if exec_qty is None or price is None or exec_qty <= 0.0 or price <= 0.0 or not exec_id:
            return None
        try:
            ts_ms = int(trade.get('execTime') or 0)
        except (TypeError, ValueError):
            ts_ms = 0
        pine_id, from_entry, leg_type = identity
        side = str(trade.get('side') or '').lower()
        cumulative = before + exec_qty
        fill_qty = exec_qty
        fee_currency = str(trade.get('feeCurrency') or '') or market.settle_coin
        if market.category == CATEGORY_SPOT:
            spot_fee_ccy = str(trade.get('feeCurrency') or '') or (
                market.base_coin if side == 'buy' else market.quote_coin)
            if spot_fee_ccy == market.base_coin:
                fill_qty = exec_qty - fee if side == 'buy' else exec_qty + fee
        order_qty = total
        order_cumulative = cumulative
        if market.is_inverse:
            factor = float(self._inverse_anchor_for(coid, fallback=price) or price)
            fill_qty = exec_qty / factor
            order_qty = total / factor
            order_cumulative = cumulative / factor
            fee = fee * price
            fee_currency = market.quote_coin
        is_full = total > 0.0 and cumulative >= total - _FILL_EPS
        order = ExchangeOrder(
            id=str(trade.get('orderId') or ''),
            symbol=self.symbol or market.symbol,
            side=side,
            order_type=(OrderType.LIMIT if leg_type is LegType.TAKE_PROFIT
                        else OrderType.STOP if leg_type is LegType.STOP_LOSS
                        else OrderType.MARKET),
            qty=order_qty,
            filled_qty=min(order_qty, order_cumulative),
            remaining_qty=max(0.0, order_qty - order_cumulative),
            price=None,
            stop_price=None,
            average_fill_price=price,
            status=OrderStatus.FILLED if is_full else OrderStatus.PARTIALLY_FILLED,
            timestamp=ts_ms / 1000.0 if ts_ms else epoch_time(),
            fee=fee,
            fee_currency=fee_currency,
            reduce_only=leg_type is not None and leg_type is not LegType.ENTRY,
            client_order_id=self._core_client_order_id(coid or None),
        )
        return OrderEvent(
            order=order,
            event_type='filled' if is_full else 'partial',
            fill_price=price,
            fill_qty=fill_qty,
            timestamp=order.timestamp,
            pine_id=pine_id,
            from_entry=from_entry,
            leg_type=leg_type,
            fee=fee,
            fee_currency=fee_currency,
            fill_id=exec_id,
            journal_fill_qty=exec_qty,
        )


def _finite(value: object) -> float | None:
    """Parse a finite venue number, ``None`` when it is absent or unusable."""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)) or value == '':
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _order_fee(fills: list[OrderEvent]) -> tuple[float, str, bool]:
    """The order's cumulative fee from its fill events.

    :return: ``(fee, currency, reported)``: ``reported`` is ``False`` when
        the executions charge the fee in more than one currency, which one
        figure cannot carry.
    """
    currencies = {fill.fee_currency for fill in fills if fill.fee_currency}
    total = sum(fill.fee for fill in fills)
    if len(currencies) > 1:
        return 0.0, '', False
    return total, next(iter(currencies), ''), True
