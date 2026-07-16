"""Order-execution mix-in for the Bybit plugin (spot, M2).

Implements the write side of :class:`~pynecore.core.plugin.broker.BrokerPlugin`:
every ``execute_*`` and ``modify_*`` path over ``POST /v5/order/*``, plus
quantity/price quantization onto the instrument grid.

Spot execution model:

- Entries are plain Market / Limit orders, or conditional
  (``orderFilter=StopOrder`` + ``triggerPrice``) market orders for the
  STOP type. Market orders always send ``marketUnit='baseCoin'`` — the
  venue default for a market BUY is quoteCoin, which would misread the
  Pine base-denominated quantity.
- The exit bracket is SOFTWARE: a plain limit TP leg plus a conditional
  stop-market SL leg. The engine owns the OCA cascade between them and
  the partial-fill amends; spot has no reduce-only flag, the semantics
  hold structurally (a sell cannot exceed the held inventory).
- ``orderLinkId`` carries the deterministic client-order-id (NATIVE
  idempotency): a duplicate submission is rejected with retCode 170141
  and resolved by looking the original order up by the same id.
- Every dispatch row is persisted BEFORE the wire send (when persistence
  is on): the spot inventory attribution keys on the ``orderLinkId``
  being resolvable, so a crash between send and ack must not orphan a
  fill into "foreign activity" (which would trip the balance invariant).

M2 scope note: this is the forward dispatch path — persist-first crash
*recovery*, disappearance detection and the cancel-tentative state machine
are the robustness milestone, mirroring the cTrader plugin's phasing.
"""
import asyncio
import logging
from decimal import Decimal
from time import time as epoch_time

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
    OrderSkippedByPlugin,
)
from pynecore.core.broker.idempotency import (
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_ENTRY_STOP,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
)
from pynecore.core.broker.models import (
    CancelDispositionOutcome,
    CancelIntent,
    CloseIntent,
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExitIntent,
    LegType,
    OrderStatus,
    OrderType,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    create_entry_order_row,
    mark_disposition_unknown,
    mark_rejected,
)
from pynecore.core.plugin import override

from ._base import _BybitBase
from .exceptions import (
    AMBIGUOUS_DISPOSITION_CODES,
    BybitAPIError,
    BybitError,
    DUPLICATE_COID_CODES,
    ORDER_NOT_FOUND_CODES,
    map_broker_error,
    reject_error,
)
from .helpers import format_decimal, quantize_qty, round_price
from .models import InstrumentInfo

logger = logging.getLogger(__name__)

_SIDE_WIRE = {'buy': 'Buy', 'sell': 'Sell'}


class _ExecutionMixin(_BybitBase):
    """Order execution mix-in: every ``execute_*`` and ``modify_*`` path."""

    # --- identity bookkeeping ----------------------------------------------

    def _record_identity(self, coid: str, *, pine_id: str | None,
                         from_entry: str | None, leg_type: LegType,
                         qty: float) -> None:
        """Remember a dispatch's Pine identity for event reverse-mapping.

        Also seeds the in-memory quantity cursor the event stream's
        partial-vs-filled discriminator reads when persistence is off.
        """
        self._order_identity[coid] = (pine_id, from_entry, leg_type)
        self._dispatch_qty[coid] = qty

    def _resolve_identity(
            self, order_link_id: str | None, order_id: str | None,
    ) -> tuple[str | None, str | None, LegType | None]:
        """Reverse-map an exchange event to its Pine identity.

        In-memory map first (always current within one process), then the
        BrokerStore rows (survive restarts). ``(None, None, None)`` marks
        external activity the event stream must drop.
        """
        if order_link_id:
            identity = self._order_identity.get(order_link_id)
            if identity is not None:
                return identity
        row = None
        if self.store_ctx is not None:
            if order_link_id:
                row = self.store_ctx.get_order(order_link_id)
            if row is None and order_id:
                row = self.store_ctx.find_by_ref('order_id', order_id)
        if row is None:
            return None, None, None
        extras = row.extras or {}
        kind = extras.get('kind')
        if kind == 'exit_leg':
            leg = (LegType.TAKE_PROFIT if extras.get('leg') == 'tp'
                   else LegType.STOP_LOSS)
            return extras.get('exit_id'), row.from_entry, leg
        if kind == 'close':
            return None, extras.get('close_of_entry'), LegType.CLOSE
        return row.pine_entry_id, None, LegType.ENTRY

    # --- dispatch core --------------------------------------------------------

    async def _order_post(self, endpoint: str, body: dict, *,
                          coid: str, context: str) -> dict:
        """POST one signed order request, classifying the failure modes.

        - Transport trouble (timeout, dropped connection) is ambiguous —
          the request may have reached the venue — so it parks as
          :class:`OrderDispositionUnknownError` and the engine verifies
          against ``get_open_orders`` (the deterministic ``orderLinkId``
          makes the match exact).
        - A mapped ``retCode`` (auth / rate limit / insufficient balance)
          raises its taxonomy class; anything else is a definitive
          :class:`ExchangeOrderRejectedError` — Bybit rejected before
          booking, nothing is live.
        - A duplicate ``orderLinkId`` reject means the original landed:
          the existing order is looked up by the id and returned as the
          result, keeping retries idempotent end-to-end.
        """
        try:
            return await self._call(endpoint, method='post', body=body, auth=True)
        except BybitAPIError as e:
            if e.ret_code in DUPLICATE_COID_CODES:
                existing = await self._lookup_order_by_coid(coid)
                if existing is not None:
                    logger.info(
                        "Bybit %s: duplicate orderLinkId %s — adopting the "
                        "already-landed order %s",
                        context, coid, existing.get('orderId'),
                    )
                    return existing
                raise OrderDispositionUnknownError(
                    f"Bybit {context}: duplicate orderLinkId {coid} but the "
                    f"original order is not readable",
                    client_order_id=coid, cause=e,
                ) from e
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                # Server timeout / internal error AFTER the request reached
                # Bybit — the order may have been booked before the error,
                # so a definitive reject here would let the engine retry
                # under a fresh orderLinkId and open duplicate exposure.
                # Park as disposition-unknown; the engine verifies against
                # ``get_open_orders`` by the deterministic orderLinkId.
                raise OrderDispositionUnknownError(
                    f"Bybit {context}: server-side failure "
                    f"(retCode={e.ret_code}); disposition unknown",
                    client_order_id=coid, cause=e,
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BybitError as e:
            raise OrderDispositionUnknownError(
                f"Bybit {context} transport failure; disposition unknown",
                client_order_id=coid, cause=e,
            ) from e

    async def _lookup_order_by_coid(self, coid: str) -> dict | None:
        """Find an order by its ``orderLinkId`` (open set, then history)."""
        market = await asyncio.to_thread(self._spot_market)
        for endpoint in ('/v5/order/realtime', '/v5/order/history'):
            try:
                result = await self._call(endpoint, {
                    'category': market.category,
                    'symbol': market.symbol,
                    'orderLinkId': coid,
                }, auth=True)
            except BybitError:
                continue
            entries = result.get('list') or []
            if entries:
                return entries[0]
        return None

    # --- sizing / pre-flight ----------------------------------------------------

    @staticmethod
    def _quantize_or_skip(
            market: InstrumentInfo, qty: float, *,
            intent_key: str, label: str,
    ) -> Decimal:
        """Floor ``qty`` onto the base grid; skip a zero result."""
        quantized = quantize_qty(qty, market.qty_step_str)
        if quantized <= 0:
            raise OrderSkippedByPlugin(
                f"Skipping {label}: size {qty} quantizes to zero on the "
                f"{market.symbol} grid ({market.qty_step_str}). No order sent.",
                intent_key=intent_key, reason="below_min_size",
                context={'symbol': market.symbol, 'qty': qty,
                         'qty_step': market.qty_step_str},
            )
        return quantized

    def _preflight_order(
            self, market: InstrumentInfo, qty: Decimal, *,
            is_market: bool, price: Decimal | None,
            intent_key: str, label: str,
    ) -> None:
        """Enforce the venue's per-order bounds without clamping.

        Quantity ceilings (``maxMarketOrderQty`` / ``maxLimitOrderQty``)
        and the QUOTE-denominated spot minimum (``minOrderAmt``) both skip
        loudly — a single out-of-range order must not halt the bot. The
        market-order notional check uses the latest observed trade price;
        with no price seen yet the venue-side reject is the backstop.
        """
        cap = (market.max_market_order_qty if is_market
               else market.max_limit_order_qty)
        if 0 < cap < float(qty):
            raise OrderSkippedByPlugin(
                f"Skipping {label}: size {qty} above the {market.symbol} "
                f"per-order maximum {cap}. No order sent.",
                intent_key=intent_key, reason="above_max_size",
                context={'symbol': market.symbol, 'qty': float(qty),
                         'max_qty': cap},
            )
        if market.min_order_amt > 0:
            ref_price = price
            if ref_price is None and self._last_price is not None:
                ref_price = Decimal(str(self._last_price))
            if ref_price is not None \
                    and float(qty * ref_price) < market.min_order_amt:
                raise OrderSkippedByPlugin(
                    f"Skipping {label}: notional {float(qty * ref_price):.8g} "
                    f"{market.quote_coin} below the {market.symbol} minimum "
                    f"{market.min_order_amt}. No order sent.",
                    intent_key=intent_key, reason="below_min_notional",
                    context={'symbol': market.symbol, 'qty': float(qty),
                             'min_order_amt': market.min_order_amt},
                )

    # --- persistence helpers -----------------------------------------------------

    def _persist_leg_row(
            self, coid: str, *, market: InstrumentInfo, side: str,
            qty: Decimal, intent_key: str, extras: dict,
            from_entry: str | None = None,
            sl_level: float | None = None, tp_level: float | None = None,
    ) -> None:
        """Persist-first row for a non-entry dispatch (exit leg / close).

        Written BEFORE the wire send so the spot inventory attribution can
        resolve the ``orderLinkId`` even when the ack is lost — an
        unattributable own fill would otherwise book as foreign activity
        and trip the balance invariant.
        """
        if self.store_ctx is None:
            return
        self.store_ctx.upsert_order(
            coid,
            symbol=market.symbol,
            side=side,
            qty=float(qty),
            state='submitted',
            intent_key=intent_key,
            from_entry=from_entry,
            sl_level=sl_level,
            tp_level=tp_level,
            extras=extras,
        )

    def _confirm_row(self, coid: str, order_id: str, extras: dict) -> None:
        """Advance a persisted row to ``confirmed`` with its exchange id."""
        if self.store_ctx is None:
            return
        self.store_ctx.upsert_order(
            coid, state='confirmed', exchange_order_id=order_id,
            extras=extras,
        )
        self.store_ctx.add_ref(coid, 'order_id', order_id)

    def _mark_dispatch_failure(self, coid: str, exc: Exception) -> None:
        """Advance a persisted row on a failed POST (journal contract)."""
        if self.store_ctx is None:
            return
        if isinstance(exc, OrderDispositionUnknownError):
            mark_disposition_unknown(self.store_ctx, coid=coid)
        else:
            mark_rejected(self.store_ctx, coid=coid)

    # --- BrokerPlugin: execute path -----------------------------------------------

    @override
    async def execute_entry(self, envelope: DispatchEnvelope) -> list[ExchangeOrder]:
        """Open or add to the spot inventory (MARKET / LIMIT / STOP entry)."""
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        market = await asyncio.to_thread(self._spot_market)
        coid = envelope.client_order_id(
            KIND_ENTRY_STOP if intent.stop_fired_market else KIND_ENTRY,
        )
        label = (f"{market.symbol} {intent.side.upper()} entry "
                 f"id={intent.pine_id!r}")
        qty = self._quantize_or_skip(
            market, intent.qty, intent_key=intent.intent_key, label=label,
        )

        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'side': _SIDE_WIRE[intent.side],
            'qty': format_decimal(qty),
            'orderLinkId': coid,
            'isLeverage': 0,
        }
        price: Decimal | None = None
        is_market = intent.order_type is not OrderType.LIMIT
        if intent.order_type is OrderType.MARKET:
            body['orderType'] = 'Market'
            body['marketUnit'] = 'baseCoin'
        elif intent.order_type is OrderType.LIMIT:
            if intent.limit is None:
                raise ExchangeOrderRejectedError(
                    f"Bybit LIMIT entry needs a limit price (id={intent.pine_id!r})"
                )
            limit_price = round_price(intent.limit, market.tick_size_str)
            body['orderType'] = 'Limit'
            body['price'] = format_decimal(limit_price)
            body['timeInForce'] = 'GTC'
            price = limit_price
        else:  # STOP — conditional market entry
            if intent.stop is None:
                raise ExchangeOrderRejectedError(
                    f"Bybit STOP entry needs a stop price (id={intent.pine_id!r})"
                )
            trigger = round_price(intent.stop, market.tick_size_str)
            body['orderType'] = 'Market'
            body['marketUnit'] = 'baseCoin'
            body['orderFilter'] = 'StopOrder'
            body['triggerPrice'] = format_decimal(trigger)
        self._preflight_order(
            market, qty, is_market=is_market, price=price,
            intent_key=intent.intent_key, label=label,
        )

        entry_kind = (ENTRY_KIND_POSITION
                      if intent.order_type is OrderType.MARKET
                      else ENTRY_KIND_WORKING)
        self._record_identity(coid, pine_id=intent.pine_id,
                              from_entry=None, leg_type=LegType.ENTRY,
                              qty=float(qty))
        if self.store_ctx is not None:
            create_entry_order_row(
                self.store_ctx, coid=coid, symbol=market.symbol,
                side=intent.side, qty=float(qty),
                intent_key=intent.intent_key, pine_entry_id=intent.pine_id,
                kind=entry_kind, order_type=intent.order_type.value,
            )
            self.store_ctx.log_event(
                'dispatch_submitted', client_order_id=coid,
                intent_key=intent.intent_key,
                payload={'kind': entry_kind,
                         'order_type': intent.order_type.value},
            )
        try:
            result = await self._order_post('/v5/order/create', body,
                                            coid=coid, context="entry")
        except Exception as exc:
            self._mark_dispatch_failure(coid, exc)
            raise
        order_id = str(result.get('orderId') or '')
        self._confirm_row(coid, order_id, {
            'kind': entry_kind, 'order_type': intent.order_type.value,
        })
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'entry_dispatched', client_order_id=coid,
                exchange_order_id=order_id, intent_key=intent.intent_key,
            )
        return [ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=float(qty),
            filled_qty=0.0,
            remaining_qty=float(qty),
            price=float(price) if price is not None else None,
            stop_price=(float(round_price(intent.stop, market.tick_size_str))
                        if intent.order_type is OrderType.STOP
                        and intent.stop is not None else None),
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=coid,
        )]

    @override
    async def execute_exit(self, envelope: DispatchEnvelope) -> list[ExchangeOrder]:
        """Place the SOFTWARE exit bracket: limit TP leg + stop-market SL leg.

        The engine owns the OCA cascade between the legs (``oca_cancel``
        SOFTWARE) and the partial-fill qty amends (``tp_sl_bracket``
        SOFTWARE), so this path only places the resting orders. Spot has
        no trailing primitive and the capability declares UNSUPPORTED, so
        a trail-carrying intent is refused loudly (the validator already
        blocks such scripts at startup).
        """
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, ExitIntent)
        if intent.trail_price is not None or intent.trail_offset is not None:
            raise ExchangeOrderRejectedError(
                f"Bybit spot has no trailing-stop support "
                f"(exit id={intent.pine_id!r})"
            )
        market = await asyncio.to_thread(self._spot_market)
        label = (f"{market.symbol} exit id={intent.pine_id!r} "
                 f"from={intent.from_entry!r}")
        qty = self._quantize_or_skip(
            market, intent.qty, intent_key=intent.intent_key, label=label,
        )

        legs: list[ExchangeOrder] = []
        try:
            if intent.tp_price is not None:
                legs.append(await self._place_exit_leg(
                    envelope, intent, market, qty,
                    leg='tp',
                    price=round_price(intent.tp_price, market.tick_size_str),
                ))
            if intent.sl_price is not None:
                legs.append(await self._place_exit_leg(
                    envelope, intent, market, qty,
                    leg='sl',
                    price=round_price(intent.sl_price, market.tick_size_str),
                ))
        except ExchangeOrderRejectedError as exc:
            # The parent entry has already filled by the time an exit
            # dispatches, so a definitive leg reject leaves the inventory
            # OPEN AND UNPROTECTED (possibly with the sibling leg already
            # resting). Surface it distinctly so the sync engine flattens
            # with a defensive market close instead of halting; the
            # already-placed sibling is enumerated by
            # :meth:`get_residual_orders_after_bracket_attach_reject`.
            raise BracketAttachAfterFillRejectedError(
                f"Bybit exit leg rejected after entry fill "
                f"(exit={intent.pine_id!r}, from_entry={intent.from_entry!r}): "
                f"{exc}",
                position_coid=self._entry_coid_for(intent.from_entry)
                or f"__pyne_orphan__{intent.symbol}__{intent.from_entry}",
                symbol=intent.symbol,
                position_side='buy' if intent.side == 'sell' else 'sell',
                qty=float(qty),
                from_entry=intent.from_entry,
                exit_id=intent.pine_id,
            ) from exc
        return legs

    def _entry_coid_for(self, from_entry: str) -> str | None:
        """Resolve the live entry row's coid for a Pine entry id."""
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders():
                extras = row.extras or {}
                if (row.pine_entry_id == from_entry
                        and extras.get('kind') in (ENTRY_KIND_POSITION,
                                                   ENTRY_KIND_WORKING)):
                    return row.client_order_id
        # Same-session fallback: the in-memory identity map still knows the
        # dispatch when the store row is unavailable (persistence off, or a
        # row closed by a race); the newest matching entry dispatch wins.
        for coid, (pine_id, _, leg_type) in reversed(self._order_identity.items()):
            if leg_type is LegType.ENTRY and pine_id == from_entry:
                return coid
        return None

    async def _place_exit_leg(
            self, envelope: DispatchEnvelope, intent: ExitIntent,
            market: InstrumentInfo, qty: Decimal, *,
            leg: str, price: Decimal,
    ) -> ExchangeOrder:
        """Place one bracket leg: plain limit (TP) or conditional market (SL)."""
        coid = envelope.client_order_id(KIND_EXIT_TP if leg == 'tp' else KIND_EXIT_SL)
        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'side': _SIDE_WIRE[intent.side],
            'qty': format_decimal(qty),
            'orderLinkId': coid,
            'isLeverage': 0,
        }
        if leg == 'tp':
            body['orderType'] = 'Limit'
            body['price'] = format_decimal(price)
            body['timeInForce'] = 'GTC'
        else:
            body['orderType'] = 'Market'
            body['marketUnit'] = 'baseCoin'
            body['orderFilter'] = 'StopOrder'
            body['triggerPrice'] = format_decimal(price)
        leg_type = LegType.TAKE_PROFIT if leg == 'tp' else LegType.STOP_LOSS
        self._record_identity(coid, pine_id=intent.pine_id,
                              from_entry=intent.from_entry, leg_type=leg_type,
                              qty=float(qty))
        extras = {'kind': 'exit_leg', 'leg': leg, 'exit_id': intent.pine_id}
        self._persist_leg_row(
            coid, market=market, side=intent.side, qty=qty,
            intent_key=intent.intent_key, from_entry=intent.from_entry,
            tp_level=float(price) if leg == 'tp' else None,
            sl_level=float(price) if leg == 'sl' else None,
            extras=extras,
        )
        try:
            result = await self._order_post(
                '/v5/order/create', body, coid=coid, context=f"exit {leg} leg",
            )
        except Exception as exc:
            self._mark_dispatch_failure(coid, exc)
            raise
        order_id = str(result.get('orderId') or '')
        self._confirm_row(coid, order_id, extras)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'exit_leg_dispatched', client_order_id=coid,
                exchange_order_id=order_id, intent_key=intent.intent_key,
                payload={'leg': leg, 'price': float(price)},
            )
        return ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.LIMIT if leg == 'tp' else OrderType.STOP,
            qty=float(qty),
            filled_qty=0.0,
            remaining_qty=float(qty),
            price=float(price) if leg == 'tp' else None,
            stop_price=float(price) if leg == 'sl' else None,
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=True,
            client_order_id=coid,
        )

    @override
    async def execute_close(self, envelope: DispatchEnvelope) -> ExchangeOrder:
        """Reduce the inventory with a market sell (or cover with a buy)."""
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        market = await asyncio.to_thread(self._spot_market)
        coid = envelope.client_order_id(KIND_CLOSE)
        label = f"{market.symbol} close id={intent.pine_id!r}"
        qty = self._quantize_or_skip(
            market, intent.qty, intent_key=intent.intent_key, label=label,
        )
        self._preflight_order(
            market, qty, is_market=True, price=None,
            intent_key=intent.intent_key, label=label,
        )
        body = {
            'category': market.category,
            'symbol': market.symbol,
            'side': _SIDE_WIRE[intent.side],
            'orderType': 'Market',
            'marketUnit': 'baseCoin',
            'qty': format_decimal(qty),
            'orderLinkId': coid,
            'isLeverage': 0,
        }
        self._record_identity(coid, pine_id=None,
                              from_entry=intent.pine_id, leg_type=LegType.CLOSE,
                              qty=float(qty))
        extras = {'kind': 'close', 'close_of_entry': intent.pine_id}
        self._persist_leg_row(
            coid, market=market, side=intent.side, qty=qty,
            intent_key=intent.intent_key, from_entry=intent.pine_id,
            extras=extras,
        )
        try:
            result = await self._order_post('/v5/order/create', body,
                                            coid=coid, context="close")
        except Exception as exc:
            self._mark_dispatch_failure(coid, exc)
            raise
        order_id = str(result.get('orderId') or '')
        self._confirm_row(coid, order_id, extras)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'close_dispatched', client_order_id=coid,
                exchange_order_id=order_id, intent_key=intent.intent_key,
            )
        return ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.MARKET,
            qty=float(qty),
            filled_qty=0.0,
            remaining_qty=float(qty),
            price=None,
            stop_price=None,
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=True,
            client_order_id=coid,
        )

    # --- cancel -------------------------------------------------------------------

    def _live_coids_for_cancel(self, intent: CancelIntent) -> list[str]:
        """Collect the live dispatch coids a cancel intent addresses.

        An exit cancel (``from_entry`` set) targets the bracket legs of
        that ``(exit_id, from_entry)`` pair; an entry cancel targets the
        working entry order(s) of the Pine id. Sources: the in-memory
        identity map (always current in-process) merged with the live
        BrokerStore rows (authoritative across restarts).
        """
        want_exit = intent.from_entry is not None
        coids: list[str] = []
        for coid, (pine_id, from_entry, leg_type) in self._order_identity.items():
            if want_exit:
                if (leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS)
                        and pine_id == intent.pine_id
                        and from_entry == intent.from_entry):
                    coids.append(coid)
            elif leg_type is LegType.ENTRY and pine_id == intent.pine_id:
                coids.append(coid)
        if self.store_ctx is not None:
            for row in self.store_ctx.iter_live_orders(symbol=None):
                if row.client_order_id in coids:
                    continue
                extras = row.extras or {}
                if want_exit:
                    if (extras.get('kind') == 'exit_leg'
                            and extras.get('exit_id') == intent.pine_id
                            and row.from_entry == intent.from_entry):
                        coids.append(row.client_order_id)
                elif (extras.get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                        and row.pine_entry_id == intent.pine_id):
                    coids.append(row.client_order_id)
        return coids

    @override
    async def execute_cancel(self, envelope: DispatchEnvelope) -> bool:
        """Cancel the pending order(s) behind a Pine cancel intent.

        Idempotent: no matching dispatch, or an "order does not exist"
        response (already filled / cancelled — retCode 170213) is a
        benign no-op returning ``True``.
        """
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        market = await asyncio.to_thread(self._spot_market)
        cancelled_any = True
        for coid in self._live_coids_for_cancel(intent):
            if await self._cancel_by_coid(market, coid):
                if self.store_ctx is not None:
                    self.store_ctx.close_order(coid)
            else:
                cancelled_any = False
        return cancelled_any

    async def _cancel_by_coid(self, market: InstrumentInfo, coid: str) -> bool:
        """Cancel one order by ``orderLinkId``; not-found is a benign True."""
        try:
            await self._call('/v5/order/cancel', method='post', body={
                'category': market.category,
                'symbol': market.symbol,
                'orderLinkId': coid,
            }, auth=True)
        except BybitAPIError as e:
            if e.ret_code in ORDER_NOT_FOUND_CODES:
                return True
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                raise OrderDispositionUnknownError(
                    f"Bybit cancel server-side failure for {coid} "
                    f"(retCode={e.ret_code}); disposition unknown",
                    client_order_id=coid, cause=e,
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BybitError as e:
            raise OrderDispositionUnknownError(
                f"Bybit cancel transport failure for {coid}; disposition unknown",
                client_order_id=coid, cause=e,
            ) from e
        return True

    @override
    async def execute_cancel_with_outcome(
            self, envelope: DispatchEnvelope,
    ) -> CancelDispositionOutcome:
        """Cancel and classify the precise disposition.

        A clean cancel response is a confirmed cancel. On an
        "order does not exist" reject the order's terminal status is read
        back by ``orderLinkId`` from the history endpoint: ``Filled`` →
        the race was lost to a fill; ``Cancelled`` (or the partial-fill
        cancel variant) → the cancel landed earlier; anything unreadable
        stays UNKNOWN so the cancel-tentative machine keeps retrying.
        Multi-leg intents (a bracket's TP+SL) aggregate conservatively:
        any leg lost to a fill dominates, any ambiguity degrades to
        UNKNOWN, and only an all-legs-confirmed round reports confirmed.
        """
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, CancelIntent)
        market = await asyncio.to_thread(self._spot_market)
        coids = self._live_coids_for_cancel(intent)
        if not coids:
            return CancelDispositionOutcome.UNKNOWN
        outcomes: list[CancelDispositionOutcome] = []
        for coid in coids:
            outcomes.append(await self._cancel_outcome_for(market, coid))
        if CancelDispositionOutcome.ALREADY_FILLED in outcomes:
            return CancelDispositionOutcome.ALREADY_FILLED
        if all(o is CancelDispositionOutcome.CANCEL_CONFIRMED for o in outcomes):
            return CancelDispositionOutcome.CANCEL_CONFIRMED
        return CancelDispositionOutcome.UNKNOWN

    async def _cancel_outcome_for(
            self, market: InstrumentInfo, coid: str,
    ) -> CancelDispositionOutcome:
        """Cancel one order and classify its terminal disposition."""
        try:
            await self._call('/v5/order/cancel', method='post', body={
                'category': market.category,
                'symbol': market.symbol,
                'orderLinkId': coid,
            }, auth=True)
        except BybitAPIError as e:
            if e.ret_code not in ORDER_NOT_FOUND_CODES:
                if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                    # Server-side failure — the cancel may or may not have
                    # landed; UNKNOWN keeps the cancel-tentative machine
                    # retrying instead of trusting an ambiguous answer.
                    return CancelDispositionOutcome.UNKNOWN
                mapped = map_broker_error(e)
                if mapped is not None:
                    raise mapped from e
                raise reject_error(e) from e
            existing = await self._lookup_order_by_coid(coid)
            status = str((existing or {}).get('orderStatus') or '')
            if status == 'Filled':
                return CancelDispositionOutcome.ALREADY_FILLED
            if status in ('Cancelled', 'PartiallyFilledCanceled', 'Deactivated',
                          'Rejected'):
                if self.store_ctx is not None:
                    self.store_ctx.close_order(coid)
                return CancelDispositionOutcome.CANCEL_CONFIRMED
            return CancelDispositionOutcome.UNKNOWN
        except BybitError:
            return CancelDispositionOutcome.UNKNOWN
        if self.store_ctx is not None:
            self.store_ctx.close_order(coid)
        return CancelDispositionOutcome.CANCEL_CONFIRMED

    @override
    async def cancel_broker_order_ref(self, ref: str) -> None:
        """Cancel a residual order by its raw ``orderId`` (idempotent)."""
        market = await asyncio.to_thread(self._spot_market)
        try:
            await self._call('/v5/order/cancel', method='post', body={
                'category': market.category,
                'symbol': market.symbol,
                'orderId': ref,
            }, auth=True)
        except BybitAPIError as e:
            if e.ret_code in ORDER_NOT_FOUND_CODES:
                return
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                raise OrderDispositionUnknownError(
                    f"Bybit residual cancel server-side failure for order "
                    f"{ref} (retCode={e.ret_code}); disposition unknown",
                    client_order_id=f"__pyne_residual_cancel__{ref}", cause=e,
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BybitError as e:
            raise OrderDispositionUnknownError(
                f"Bybit residual cancel transport failure for order {ref}",
                client_order_id=f"__pyne_residual_cancel__{ref}", cause=e,
            ) from e

    @override
    async def execute_cancel_all(self, symbol: str | None = None) -> int:
        """Cancel every open order of the (single) instrument, natively."""
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._spot_market)
        try:
            result = await self._call('/v5/order/cancel-all', method='post', body={
                'category': market.category,
                'symbol': market.symbol,
            }, auth=True)
        except BybitAPIError as e:
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                raise OrderDispositionUnknownError(
                    f"Bybit cancel-all server-side failure "
                    f"(retCode={e.ret_code}); disposition unknown",
                    client_order_id='__pyne_cancel_all__', cause=e,
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BybitError as e:
            raise OrderDispositionUnknownError(
                "Bybit cancel-all transport failure; disposition unknown",
                client_order_id='__pyne_cancel_all__', cause=e,
            ) from e
        return len(result.get('list') or [])

    # --- modify (atomic amend) -----------------------------------------------------

    @override
    async def modify_entry(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend a pending entry in place (``POST /v5/order/amend``).

        Price and qty amend verified live on the demo (price); a change
        the amend endpoint rejects falls back to the base
        cancel+recreate. MARKET entries have nothing to amend and always
        take the base path.
        """
        intent = new.intent
        assert isinstance(intent, EntryIntent)
        if intent.order_type is OrderType.MARKET:
            return await super().modify_entry(old, new)
        old_intent = old.intent
        assert isinstance(old_intent, EntryIntent)
        old_coid = old.client_order_id(
            KIND_ENTRY_STOP if old_intent.stop_fired_market else KIND_ENTRY,
        )
        market = await asyncio.to_thread(self._spot_market)
        label = (f"{market.symbol} {intent.side.upper()} entry amend "
                 f"id={intent.pine_id!r}")
        qty = self._quantize_or_skip(
            market, intent.qty, intent_key=intent.intent_key, label=label,
        )
        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'orderLinkId': old_coid,
            'qty': format_decimal(qty),
        }
        price: Decimal | None = None
        if intent.order_type is OrderType.LIMIT and intent.limit is not None:
            limit_price = round_price(intent.limit, market.tick_size_str)
            body['price'] = format_decimal(limit_price)
            price = limit_price
        elif intent.order_type is OrderType.STOP and intent.stop is not None:
            body['triggerPrice'] = format_decimal(
                round_price(intent.stop, market.tick_size_str),
            )
        try:
            result = await self._amend_or_none(body, coid=old_coid)
        except BybitAPIError as e:
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        if result is None:
            # The working order is gone (filled / cancelled race) — the
            # base cancel+recreate resolves the disposition cleanly.
            return await super().modify_entry(old, new)
        if self.store_ctx is not None:
            self.store_ctx.upsert_order(old_coid, qty=float(qty))
            self.store_ctx.log_event(
                'entry_amended', client_order_id=old_coid,
                intent_key=intent.intent_key,
                payload={'qty': float(qty),
                         'price': float(price) if price is not None else None},
            )
        return [ExchangeOrder(
            id=str(result.get('orderId') or ''),
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=float(qty),
            filled_qty=0.0,
            remaining_qty=float(qty),
            price=float(price) if price is not None else None,
            stop_price=(float(round_price(intent.stop, market.tick_size_str))
                        if intent.order_type is OrderType.STOP
                        and intent.stop is not None else None),
            average_fill_price=None,
            status=OrderStatus.OPEN,
            timestamp=epoch_time(),
            fee=0.0,
            fee_currency='',
            reduce_only=False,
            client_order_id=old_coid,
        )]

    @override
    async def modify_exit(
            self, old: DispatchEnvelope, new: DispatchEnvelope,
    ) -> list[ExchangeOrder]:
        """Amend the bracket legs' prices/qty in place, leg by leg.

        Only when the leg SHAPE is unchanged (same TP/SL presence) — a
        shape change (leg added or removed) falls back to the base
        cancel+recreate, which handles the asymmetry uniformly.
        """
        new_intent = new.intent
        old_intent = old.intent
        assert isinstance(new_intent, ExitIntent)
        assert isinstance(old_intent, ExitIntent)
        same_shape = (
            (new_intent.tp_price is None) == (old_intent.tp_price is None)
            and (new_intent.sl_price is None) == (old_intent.sl_price is None)
            and new_intent.trail_offset is None and new_intent.trail_price is None
        )
        if not same_shape:
            return await super().modify_exit(old, new)
        market = await asyncio.to_thread(self._spot_market)
        label = (f"{market.symbol} exit amend id={new_intent.pine_id!r} "
                 f"from={new_intent.from_entry!r}")
        qty = self._quantize_or_skip(
            market, new_intent.qty, intent_key=new_intent.intent_key, label=label,
        )
        legs: list[ExchangeOrder] = []
        for leg, kind, level in (
                ('tp', KIND_EXIT_TP, new_intent.tp_price),
                ('sl', KIND_EXIT_SL, new_intent.sl_price),
        ):
            if level is None:
                continue
            old_coid = old.client_order_id(kind)
            price = round_price(level, market.tick_size_str)
            body: dict = {
                'category': market.category,
                'symbol': market.symbol,
                'orderLinkId': old_coid,
                'qty': format_decimal(qty),
            }
            if leg == 'tp':
                body['price'] = format_decimal(price)
            else:
                body['triggerPrice'] = format_decimal(price)
            try:
                result = await self._amend_or_none(body, coid=old_coid)
            except BybitAPIError as e:
                mapped = map_broker_error(e)
                if mapped is not None:
                    raise mapped from e
                raise reject_error(e) from e
            if result is None:
                # Leg vanished mid-amend (fill/cancel race) — resolve the
                # whole bracket through cancel+recreate.
                return await super().modify_exit(old, new)
            if self.store_ctx is not None:
                self.store_ctx.upsert_order(
                    old_coid, qty=float(qty),
                    tp_level=float(price) if leg == 'tp' else None,
                    sl_level=float(price) if leg == 'sl' else None,
                )
            legs.append(ExchangeOrder(
                id=str(result.get('orderId') or ''),
                symbol=new_intent.symbol,
                side=new_intent.side,
                order_type=OrderType.LIMIT if leg == 'tp' else OrderType.STOP,
                qty=float(qty),
                filled_qty=0.0,
                remaining_qty=float(qty),
                price=float(price) if leg == 'tp' else None,
                stop_price=float(price) if leg == 'sl' else None,
                average_fill_price=None,
                status=OrderStatus.OPEN,
                timestamp=epoch_time(),
                fee=0.0,
                fee_currency='',
                reduce_only=True,
                client_order_id=old_coid,
            ))
        if legs and self.store_ctx is not None:
            self.store_ctx.log_event(
                'exit_amended', intent_key=new_intent.intent_key,
                payload={'qty': float(qty), 'tp': new_intent.tp_price,
                         'sl': new_intent.sl_price},
            )
        return legs

    async def _amend_or_none(self, body: dict, *, coid: str) -> dict | None:
        """Run one ``/v5/order/amend``; ``None`` when the order is gone.

        Transport ambiguity parks as disposition-unknown like every other
        write; a not-found reject returns ``None`` so the caller can fall
        back to cancel+recreate. Other rejects propagate raw
        (:class:`BybitAPIError`) for the caller's uniform mapping.
        """
        try:
            return await self._call('/v5/order/amend', method='post',
                                    body=body, auth=True)
        except BybitAPIError as e:
            if e.ret_code in ORDER_NOT_FOUND_CODES:
                return None
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                raise OrderDispositionUnknownError(
                    f"Bybit amend server-side failure for {coid} "
                    f"(retCode={e.ret_code}); disposition unknown",
                    client_order_id=coid, cause=e,
                ) from e
            raise
        except BybitError as e:
            raise OrderDispositionUnknownError(
                f"Bybit amend transport failure for {coid}; disposition unknown",
                client_order_id=coid, cause=e,
            ) from e

    @override
    def get_residual_orders_after_bracket_attach_reject(self, context) -> list[str]:
        """Enumerate residual live orders after a bracket-leg reject.

        The SOFTWARE bracket places its legs one by one; a reject on the
        second leg leaves the first one live. Enumerated from the live
        BrokerStore rows of the same ``(exit_id, from_entry)`` pair; the
        refs go through :meth:`cancel_broker_order_ref`, whose not-found
        normalization makes repeated calls safe.
        """
        if self.store_ctx is None:
            return []
        refs: list[str] = []
        for row in self.store_ctx.iter_live_orders(symbol=context.symbol):
            extras = row.extras or {}
            if extras.get('kind') != 'exit_leg':
                continue
            if context.exit_id is not None \
                    and extras.get('exit_id') != context.exit_id:
                continue
            if context.from_entry is not None \
                    and row.from_entry != context.from_entry:
                continue
            if row.exchange_order_id:
                refs.append(row.exchange_order_id)
        return refs
