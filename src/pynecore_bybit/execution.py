"""Order-execution mix-in for the Bybit plugin (spot M2, linear M3).

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

Linear execution model (differences):

- No ``isLeverage`` / ``marketUnit`` — linear quantities are
  base-denominated contracts. Conditional orders are plain trigger orders
  (``triggerPrice`` + ``triggerDirection``; ``orderFilter=StopOrder`` is a
  spot-only concept).
- Exit legs and closes carry the native ``reduceOnly`` flag; the bracket
  itself stays the engine-driven SOFTWARE pair until the ``trading-stop``
  position attach is verified live (per the plan's conservative rule).
- On a hedge-mode account entries stamp the intent side's ``positionIdx``
  and the reduce/close/bracket paths run through the core one-way
  emulator's ``PositionPort`` primitives (``positions.py``) instead of
  ``execute_close`` / ``_place_exit_leg``.

Inverse execution model (differences from linear):

- Wire quantities are whole USD contracts while the Pine side stays
  base-denominated (TV semantics, measured). Every dispatch converts at
  an explicit anchor price — entries at the limit / trigger / last trade
  price, reduce paths (exit legs, closes) at the net-position mirror's
  effective anchor so a core full-close lands exactly on the venue's
  contract count even after price drift between entries. The per-coid
  anchor is remembered (map + store-row extras) and the event stream
  converts the fills back at the SAME anchor, so a full fill sums exactly
  to the dispatched base quantity.
- The dispatch bookkeeping (``_dispatch_qty`` / ``_filled_cum`` /
  BrokerStore row qty) runs in the WIRE domain (contracts); only the
  core-facing ``ExchangeOrder`` / ``OrderEvent`` objects carry base.
- ``minNotionalValue`` is the contract count itself (1 contract == 1 USD)
  and the qty ceilings / ``minOrderQty`` are contract-denominated, so the
  preflight checks run in the wire domain.
- Hedge mode is refused at startup on inverse (Bybit supports it on USDT
  perpetuals and inverse futures only, and the ``PositionPort`` volume
  contract cannot carry the price-dependent base->contract conversion).

Shared across categories:

- ``orderLinkId`` carries the deterministic client-order-id (NATIVE
  idempotency): a duplicate submission is rejected (spot 170141, linear
  110072) and resolved by looking the original order up by the same id.
  A live or filled original is adopted; a DEAD original (Bybit never
  allows client-id reuse, even after a confirmed cancel — measured live)
  raises ``ClientOrderIdSpentError`` so the engine re-dispatches under a
  fresh id instead of adopting a cancelled order as live.
- Every dispatch row is persisted BEFORE the wire send (when persistence
  is on): the spot inventory attribution keys on the ``orderLinkId``
  being resolvable, so a crash between send and ack must not orphan a
  fill into "foreign activity" (which would trip the balance invariant).

Scope note: this is the forward dispatch path — persist-first crash
*recovery*, disappearance detection and the cancel-tentative state machine
are the robustness milestone, mirroring the cTrader plugin's phasing.
"""
import asyncio
import logging
from decimal import Decimal, InvalidOperation
from time import time as epoch_time

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    ClientOrderIdSpentError,
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
    is_reduce_only_zero_position_reject,
    map_broker_error,
    reject_error,
)
from .helpers import (
    CATEGORY_SPOT,
    TRIGGER_DIRECTION_FALL,
    TRIGGER_DIRECTION_RISE,
    base_to_contracts,
    contracts_to_base,
    format_decimal,
    quantize_qty,
    round_price,
)
from .models import InstrumentInfo
from .positions import HEDGE_IDX_BUY, HEDGE_IDX_SELL, POSITION_MODE_HEDGE

logger = logging.getLogger(__name__)

_SIDE_WIRE = {'buy': 'Buy', 'sell': 'Sell'}

#: Terminal ``orderStatus`` values with nothing live and no fill behind
#: them: the order died without becoming a position. A duplicate
#: ``orderLinkId`` reject that resolves to one of these means the id is
#: SPENT (Bybit never allows client-id reuse — measured live on the demo:
#: spot 170141, inverse 110072 on a re-create after a confirmed cancel),
#: so adopting the row as a live order would silently leave the intent
#: without a working order. ``PartiallyFilledCanceled`` belongs here: its
#: fills were already reported on the ORIGINAL dispatch, the id itself is
#: dead.
_DEAD_ORDER_STATUSES = frozenset({
    'Cancelled', 'PartiallyFilledCanceled', 'Deactivated', 'Rejected',
})

#: Clock-skew margin (ms) applied to the duplicate-reject spent-original
#: guard. Bybit rejects a signed request whose client timestamp drifts
#: beyond its ``recv_window`` (~1 s), so client and server clocks are held
#: closely aligned; the margin absorbs that residual skew so an in-process
#: retry (whose original was created at ~the same instant as this
#: instance's persist-first dispatch row) can never be misread as spent.
_SPENT_COID_SKEW_MS = 5_000


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
        - A duplicate ``orderLinkId`` reject means the venue already knows
          the id. A live (or filled) original is adopted as the result,
          keeping retries idempotent end-to-end. A DEAD original
          (cancelled / rejected — nothing live, no fill) means the id is
          spent: Bybit never allows client-id reuse, so the create did NOT
          land and adopting the dead row would silently report a working
          order that does not exist. That case raises
          :class:`ClientOrderIdSpentError` — the sync engine re-anchors
          the envelope and re-dispatches under a fresh id.
        """
        try:
            return await self._call(endpoint, method='post', body=body, auth=True)
        except BybitAPIError as e:
            if e.ret_code in DUPLICATE_COID_CODES:
                existing = await self._lookup_order_by_coid(coid)
                if existing is not None:
                    status = str(existing.get('orderStatus') or '')
                    if status in _DEAD_ORDER_STATUSES:
                        raise ClientOrderIdSpentError(
                            f"Bybit {context}: orderLinkId {coid} is spent — "
                            f"the venue refused the create as a duplicate and "
                            f"the original order is terminal "
                            f"({status}); nothing is live under this id"
                        ) from e
                    if self._duplicate_original_is_spent(coid, existing):
                        raise ClientOrderIdSpentError(
                            f"Bybit {context}: orderLinkId {coid} is spent — "
                            f"the duplicate original was created "
                            f"({existing.get('createdTime')!r}) before this "
                            f"instance's dispatch, so it belongs to a prior "
                            f"run; re-dispatching under a fresh id"
                        ) from e
                    logger.info(
                        "Bybit %s: duplicate orderLinkId %s — adopting the "
                        "already-landed order %s (status %s)",
                        context, coid, existing.get('orderId'), status,
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
        market = await asyncio.to_thread(self._broker_market)
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

    def _duplicate_original_is_spent(self, coid: str, existing: dict) -> bool:
        """Whether a duplicate-reject original is a spent prior-instance order.

        A duplicate ``orderLinkId`` reject normally means an in-process retry
        of the SAME dispatch already landed — adopting the found order keeps
        the retry idempotent. But a crashed prior instance can leave an
        orphaned envelope whose replay rebuilds the SAME (spent) ``coid``; the
        venue then returns the PREVIOUS instance's already-filled order, and
        adopting it would report a phantom live order for a position that is
        gone. This instance wrote its persist-first dispatch row moments
        before the wire send, so the original is spent when its ``createdTime``
        predates that row's creation by more than the client/server
        clock-skew margin. An in-process retry cannot trip this: its original
        was created at ~the same instant as the row.

        Inert without persistence (no dispatch row to anchor on) and when the
        original carries no parseable ``createdTime`` — both fall back to the
        pre-existing adopt-the-original behaviour.
        """
        if self.store_ctx is None:
            return False
        row = self.store_ctx.get_order(coid)
        if row is None:
            return False
        try:
            created = int(existing.get('createdTime') or 0)
        except (TypeError, ValueError):
            return False
        if created <= 0:
            return False
        return created < row.created_ts_ms - _SPENT_COID_SKEW_MS

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

        ``qty`` arrives in the WIRE domain (base units on spot/linear,
        whole USD contracts on inverse), which is the domain every venue
        bound is quoted in. Quantity ceilings (``maxMarketOrderQty`` /
        ``maxLimitOrderQty`` on spot, ``maxMktOrderQty`` / ``maxOrderQty``
        on derivatives), the derivative minimum (``minOrderQty``) and the
        QUOTE-denominated minimum notional (spot ``minOrderAmt`` /
        derivative ``minNotionalValue``) all skip loudly — a single
        out-of-range order must not halt the bot. The linear market-order
        notional check uses the latest observed trade price (with no price
        seen yet the venue-side reject is the backstop); on inverse the
        contract count IS the notional.
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
        if 0 < float(qty) < market.min_order_qty:
            raise OrderSkippedByPlugin(
                f"Skipping {label}: size {qty} below the {market.symbol} "
                f"per-order minimum {market.min_order_qty}. No order sent.",
                intent_key=intent_key, reason="below_min_size",
                context={'symbol': market.symbol, 'qty': float(qty),
                         'min_order_qty': market.min_order_qty},
            )
        min_notional = market.min_order_amt or market.min_notional
        if min_notional > 0:
            if market.is_inverse:
                # 1 contract == 1 USD: the wire quantity IS the quote
                # notional, no price reference needed.
                if float(qty) < min_notional:
                    raise OrderSkippedByPlugin(
                        f"Skipping {label}: notional {float(qty):.8g} "
                        f"{market.quote_coin} below the {market.symbol} "
                        f"minimum {min_notional}. No order sent.",
                        intent_key=intent_key, reason="below_min_notional",
                        context={'symbol': market.symbol, 'qty': float(qty),
                                 'min_notional': min_notional},
                    )
                return
            ref_price = price
            if ref_price is None and self._last_price is not None:
                ref_price = Decimal(str(self._last_price))
            if ref_price is not None \
                    and float(qty * ref_price) < min_notional:
                raise OrderSkippedByPlugin(
                    f"Skipping {label}: notional {float(qty * ref_price):.8g} "
                    f"{market.quote_coin} below the {market.symbol} minimum "
                    f"{min_notional}. No order sent.",
                    intent_key=intent_key, reason="below_min_notional",
                    context={'symbol': market.symbol, 'qty': float(qty),
                             'min_notional': min_notional},
                )

    # --- inverse contract mapping ------------------------------------------------

    def _inverse_anchor_for(self, coid: str, *,
                            fallback: float | None = None) -> Decimal | None:
        """Resolve the base<->contract anchor price of one inverse dispatch.

        In-memory map first (always current in-process), then the
        BrokerStore row's ``anchor`` extra (survives restarts), then the
        caller's fallback (typically the execution price — degrades the
        exact base summation to a per-slice approximation, but never
        drops a fill).
        """
        anchor = self._wire_anchor.get(coid)
        if anchor is not None:
            return anchor
        if self.store_ctx is not None and coid:
            row = self.store_ctx.get_order(coid)
            raw = (row.extras or {}).get('anchor') if row is not None else None
            if raw:
                anchor = Decimal(str(raw))
                self._wire_anchor[coid] = anchor
                return anchor
        if fallback is not None and fallback > 0:
            return Decimal(str(fallback))
        return None

    async def _inverse_ref_price(
            self, market: InstrumentInfo, price: Decimal | None,
    ) -> Decimal:
        """Pick the entry-side conversion anchor.

        The order's own limit / trigger price when it has one (the price
        the fill is expected at), else the last trade seen on the kline
        stream, else one REST ticker read — a market entry must convert
        at SOME live price; with none obtainable the entry is rejected
        (a definitive per-order reject, the engine keeps running).
        """
        if price is not None and price > 0:
            return price
        if self._last_price is not None and self._last_price > 0:
            return Decimal(str(self._last_price))
        try:
            result = await self._call('/v5/market/tickers', {
                'category': market.category,
                'symbol': market.symbol,
            })
        except BybitError as e:
            raise ExchangeOrderRejectedError(
                f"Bybit inverse conversion needs a reference price and the "
                f"ticker read failed for {market.symbol}: {e}"
            ) from e
        rows = result.get('list') or []
        last = float((rows[0] if rows else {}).get('lastPrice') or 0.0)
        if last > 0:
            return Decimal(str(last))
        raise ExchangeOrderRejectedError(
            f"Bybit reports no last price for {market.symbol} — cannot "
            f"convert the base quantity to inverse contracts"
        )

    def _inverse_entry_contracts(
            self, market: InstrumentInfo, qty: float, anchor: Decimal, *,
            intent_key: str, label: str,
    ) -> Decimal:
        """Convert a base-denominated quantity to contracts at a fixed anchor.

        Entries anchor at their own price level; the in-place amend
        paths reuse the dispatch's recorded anchor through this helper.
        """
        contracts = base_to_contracts(qty, anchor, market.qty_step_str)
        if contracts <= 0:
            raise OrderSkippedByPlugin(
                f"Skipping {label}: size {qty} converts to zero contracts "
                f"at {anchor} on the {market.symbol} grid "
                f"({market.qty_step_str}). No order sent.",
                intent_key=intent_key, reason="below_min_size",
                context={'symbol': market.symbol, 'qty': qty,
                         'anchor': float(anchor),
                         'qty_step': market.qty_step_str},
            )
        return contracts

    async def _inverse_reduce_contracts(
            self, market: InstrumentInfo, qty: float, *,
            intent_key: str, label: str,
    ) -> tuple[Decimal, Decimal]:
        """Convert a reduce-side base quantity (exit leg / close) to contracts.

        Converts through the net-position mirror's effective anchor
        (venue contracts over the base reported to the core), so the
        proportions the engine computes in base land on the same
        proportions in contracts. A request covering the whole mirrored
        base snaps onto the venue's exact contract count — this is what
        makes a core full-close leave zero residue even after the
        reversal auto-flip repriced part of the position. With no known
        position (defensive orders on a flat book) the last-price entry
        anchor applies and the venue's reduce-only handling is the
        backstop.

        :return: ``(contracts, anchor)`` — the anchor is recorded per coid
            so the fills convert back to exactly the requested base.
        """
        net_c = abs(self._inverse_net_contracts)
        net_b = abs(self._inverse_net_base)
        if net_c > 0.0 and net_b > 0.0:
            anchor = Decimal(str(net_c)) / Decimal(str(net_b))
            if qty >= net_b * (1.0 - 1e-9):
                contracts = quantize_qty(net_c, market.qty_step_str)
            else:
                contracts = min(
                    base_to_contracts(qty, anchor, market.qty_step_str),
                    quantize_qty(net_c, market.qty_step_str),
                )
        else:
            anchor = await self._inverse_ref_price(market, None)
            contracts = base_to_contracts(qty, anchor, market.qty_step_str)
        if contracts <= 0:
            raise OrderSkippedByPlugin(
                f"Skipping {label}: size {qty} converts to zero contracts "
                f"at {anchor} on the {market.symbol} grid "
                f"({market.qty_step_str}). No order sent.",
                intent_key=intent_key, reason="below_min_size",
                context={'symbol': market.symbol, 'qty': qty,
                         'anchor': float(anchor),
                         'qty_step': market.qty_step_str},
            )
        return contracts, anchor

    def _record_anchor(self, coid: str, anchor: Decimal | None,
                       extras: dict) -> dict:
        """Remember a dispatch's conversion anchor (map + row extras)."""
        if anchor is None:
            return extras
        self._wire_anchor[coid] = anchor
        return {**extras, 'anchor': format_decimal(anchor)}

    def _core_qty(self, qty: Decimal, anchor: Decimal | None) -> float:
        """The core-facing (base) value of a wire quantity."""
        if anchor is None:
            return float(qty)
        return contracts_to_base(qty, anchor)

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
        """Open or add to the position/inventory (MARKET / LIMIT / STOP entry)."""
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} {intent.side.upper()} entry "
                 f"id={intent.pine_id!r}")
        if market.is_inverse:
            anchor = await self._inverse_ref_price(
                market, self._entry_anchor_price(intent, market),
            )
            qty = self._inverse_entry_contracts(
                market, intent.qty, anchor,
                intent_key=intent.intent_key, label=label,
            )
            return await self._place_entry_order(
                envelope, intent, market, qty, anchor,
            )
        qty = self._quantize_or_skip(
            market, intent.qty, intent_key=intent.intent_key, label=label,
        )
        return await self._place_entry_order(envelope, intent, market, qty)

    @staticmethod
    def _entry_anchor_price(
            intent: EntryIntent, market: InstrumentInfo,
    ) -> Decimal | None:
        """The entry order's own price level (the expected fill price)."""
        if intent.order_type is OrderType.LIMIT and intent.limit is not None:
            return round_price(intent.limit, market.tick_size_str)
        if intent.order_type is OrderType.STOP and intent.stop is not None:
            return round_price(intent.stop, market.tick_size_str)
        return None

    def _stop_limit_dormancy_trigger(
            self, intent: EntryIntent, market: InstrumentInfo,
    ) -> Decimal | None:
        """Trigger price that keeps a both-set stop-limit entry dormant.

        A Pine ``strategy.entry(limit=, stop=)`` reaches this plugin as a
        ``LIMIT`` entry that still carries the ``stop`` price; the engine arms
        a software price-watch on the stop side. The native LIMIT leg is only
        a safe resting order while it is *non-marketable* (the OCO pullback
        leg: a buy limit below the market, a sell limit above it). When the
        limit is on the marketable side — a genuine stop-limit whose limit
        sits at or beyond the current price — a plain resting limit would fill
        immediately, before the stop is ever crossed, opening the position at
        the wrong time.

        Return the rounded stop level so the caller can place the limit as a
        native conditional (trigger) order that stays dormant until the stop
        is crossed. Return ``None`` when the limit rests safely on its own (or
        the current price is unknown), leaving the plain-limit path untouched.
        """
        if intent.stop is None or intent.limit is None:
            return None
        last = self._last_price
        if last is None or last <= 0:
            return None
        limit = float(intent.limit)
        marketable = limit >= last if intent.side == 'buy' else limit <= last
        if not marketable:
            return None
        return round_price(intent.stop, market.tick_size_str)

    async def _place_entry_order(
            self, envelope: DispatchEnvelope, intent: EntryIntent,
            market: InstrumentInfo, qty: Decimal,
            anchor: Decimal | None = None,
    ) -> list[ExchangeOrder]:
        """Build, persist and POST one entry order of ``qty`` (wire units).

        Shared by :meth:`execute_entry` and the hedge-emulation
        ``place_leg`` primitive (which sizes the residual leg itself).
        Category shape: spot sends ``isLeverage=0`` + base-coin market
        units and uses ``orderFilter=StopOrder`` conditionals; the
        derivatives send plain trigger orders (``triggerPrice`` +
        ``triggerDirection``) and, on a hedge account, stamp the
        ``positionIdx`` of the intent side. On inverse ``qty`` arrives in
        contracts with its conversion ``anchor``; the core-facing return
        value converts back to base at the same anchor.
        """
        coid = envelope.client_order_id(
            KIND_ENTRY_STOP if intent.stop_fired_market else KIND_ENTRY,
        )
        label = (f"{market.symbol} {intent.side.upper()} entry "
                 f"id={intent.pine_id!r}")
        is_spot = market.category == CATEGORY_SPOT

        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'side': _SIDE_WIRE[intent.side],
            'qty': format_decimal(qty),
            'orderLinkId': coid,
        }
        if is_spot:
            body['isLeverage'] = 0
        elif self._position_mode == POSITION_MODE_HEDGE:
            body['positionIdx'] = (HEDGE_IDX_BUY if intent.side == 'buy'
                                   else HEDGE_IDX_SELL)
        price: Decimal | None = None
        is_market = intent.order_type is not OrderType.LIMIT
        if intent.order_type is OrderType.MARKET:
            body['orderType'] = 'Market'
            if is_spot:
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
            # Both-set stop-limit dormancy: a marketable limit would fill
            # instantly as a plain resting order, before the stop trigger.
            # Gate it behind a native conditional trigger at the stop level so
            # it cannot execute until the stop is crossed. The engine's
            # software STOP watch still owns the OCA cascade; its cancel
            # disposition gate arbitrates the cross exactly as for a plain
            # resting limit, so no double-fill race is introduced.
            dormancy_trigger = self._stop_limit_dormancy_trigger(intent, market)
            if dormancy_trigger is not None:
                body['triggerPrice'] = format_decimal(dormancy_trigger)
                if is_spot:
                    body['orderFilter'] = 'StopOrder'
                else:
                    # A buy stop-limit triggers on a rise to the stop, a sell
                    # stop-limit on a fall to it.
                    body['triggerDirection'] = (TRIGGER_DIRECTION_RISE
                                                if intent.side == 'buy'
                                                else TRIGGER_DIRECTION_FALL)
        else:  # STOP — conditional market entry
            if intent.stop is None:
                raise ExchangeOrderRejectedError(
                    f"Bybit STOP entry needs a stop price (id={intent.pine_id!r})"
                )
            trigger = round_price(intent.stop, market.tick_size_str)
            body['orderType'] = 'Market'
            if is_spot:
                body['marketUnit'] = 'baseCoin'
                body['orderFilter'] = 'StopOrder'
            else:
                # A buy stop sits above the market (triggers on a rise),
                # a sell stop below (triggers on a fall).
                body['triggerDirection'] = (TRIGGER_DIRECTION_RISE
                                            if intent.side == 'buy'
                                            else TRIGGER_DIRECTION_FALL)
            body['triggerPrice'] = format_decimal(trigger)
        self._preflight_order(
            market, qty, is_market=is_market, price=price,
            intent_key=intent.intent_key, label=label,
        )

        entry_kind = (ENTRY_KIND_POSITION
                      if intent.order_type is OrderType.MARKET
                      else ENTRY_KIND_WORKING)
        extras = self._record_anchor(coid, anchor, {
            'kind': entry_kind, 'order_type': intent.order_type.value,
        })
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
            if anchor is not None:
                # The conversion anchor must survive a restart; merge it
                # into the row extras right after the insert.
                self.store_ctx.upsert_order(coid, extras=extras)
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
        self._confirm_row(coid, order_id, extras)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'entry_dispatched', client_order_id=coid,
                exchange_order_id=order_id, intent_key=intent.intent_key,
            )
        core_qty = self._core_qty(qty, anchor)
        return [ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=core_qty,
            filled_qty=0.0,
            remaining_qty=core_qty,
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
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} exit id={intent.pine_id!r} "
                 f"from={intent.from_entry!r}")
        anchor: Decimal | None = None
        if market.is_inverse:
            qty, anchor = await self._inverse_reduce_contracts(
                market, intent.qty, intent_key=intent.intent_key, label=label,
            )
        else:
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
                    anchor=anchor,
                ))
            if intent.sl_price is not None:
                legs.append(await self._place_exit_leg(
                    envelope, intent, market, qty,
                    leg='sl',
                    price=round_price(intent.sl_price, market.tick_size_str),
                    anchor=anchor,
                ))
        except ClientOrderIdSpentError as spent:
            # A leg id was consumed by a now-dead order (a cancel+recreate
            # bracket modify re-sent the pinned leg id the cancel just
            # spent). This is NOT an unprotected-position emergency — the
            # engine re-anchors and re-dispatches the whole bracket under
            # fresh ids — but the re-dispatch re-places EVERY leg, so a
            # sibling this attempt already placed must be rolled back
            # first or the fresh bracket would double it. The spent error
            # contract requires a VERIFIED cleanup before propagating: if
            # the sibling's disposition cannot be pinned down (an
            # ambiguous rollback cancel that a readback plus one retry
            # could not resolve, or a sibling that already executed),
            # letting any other error replace the spent signal would park
            # the whole dispatch on the sibling's id — pending
            # verification could then adopt that lone leg as the complete
            # bracket, leaving the position without its stop. Escalate to
            # the defensive-flatten path instead: it closes the exposure
            # and its residual enumeration owns the still-live sibling
            # row.
            for placed in legs:
                rolled_back, executed = await self._rollback_spent_sibling(
                    market, placed.client_order_id)
                if not rolled_back:
                    # Size the defensive close to the confirmed residual:
                    # a fill that raced the rollback already reduced the
                    # position (booked by the event stream), and the core
                    # forwards this qty verbatim into the synthesized
                    # CloseIntent — on spot that order carries no
                    # reduceOnly cap, so the pre-fill bracket size would
                    # oversell the inventory (or reject the whole close
                    # for insufficient balance). With the executed amount
                    # unmeasurable (unreadable readback / unresolvable
                    # cancel — the venue API is already failing in those
                    # branches, a snapshot read would ride the same
                    # broken transport) the full size stays the
                    # conservative default and the venue-side backstops
                    # bound the damage.
                    residual = (qty if executed is None
                                else max(qty - executed, Decimal(0)))
                    raise BracketAttachAfterFillRejectedError(
                        f"Bybit bracket rollback unresolved after a spent "
                        f"leg id (exit={intent.pine_id!r}, "
                        f"from_entry={intent.from_entry!r}): sibling "
                        f"{placed.client_order_id} "
                        + (f"executed {executed} of {qty}"
                           if executed is not None
                           else "disposition unknown"),
                        position_coid=self._entry_coid_for(intent.from_entry)
                        or f"__pyne_orphan__{intent.symbol}__{intent.from_entry}",
                        symbol=intent.symbol,
                        position_side='buy' if intent.side == 'sell' else 'sell',
                        qty=self._core_qty(residual, anchor),
                        from_entry=intent.from_entry,
                        exit_id=intent.pine_id,
                    ) from spent
                if self.store_ctx is not None:
                    self.store_ctx.close_order(placed.client_order_id)
            raise
        except ExchangeOrderRejectedError as exc:
            # The parent entry has already filled by the time an exit
            # dispatches, so a definitive leg reject leaves the inventory
            # OPEN AND UNPROTECTED (possibly with the sibling leg already
            # resting). Surface it distinctly so the sync engine flattens
            # with a defensive market close instead of halting; the
            # already-placed sibling is enumerated by
            # :meth:`get_residual_orders_after_bracket_attach_reject`.
            #
            # Exception: the venue rejected the reduce-only leg because the
            # position is ALREADY FLAT (retCode 110017 "current position is
            # zero"). On a netting venue a bracket re-emission can race a
            # close that already flattened the shared position — the exit is
            # moot, not an unprotected-position emergency. Signal proven-flat
            # (``qty=0``): the engine skips the defensive close (which would
            # itself reject reduce-only against the flat position and halt)
            # and runs only the OCA / residual cleanup.
            cause = exc.__cause__
            proven_flat = (
                isinstance(cause, BybitAPIError)
                and is_reduce_only_zero_position_reject(cause)
            )
            raise BracketAttachAfterFillRejectedError(
                f"Bybit exit leg rejected after entry fill "
                f"(exit={intent.pine_id!r}, from_entry={intent.from_entry!r}): "
                f"{exc}",
                position_coid=self._entry_coid_for(intent.from_entry)
                or f"__pyne_orphan__{intent.symbol}__{intent.from_entry}",
                symbol=intent.symbol,
                position_side='buy' if intent.side == 'sell' else 'sell',
                qty=0.0 if proven_flat else self._core_qty(qty, anchor),
                from_entry=intent.from_entry,
                exit_id=intent.pine_id,
                error_code=(str(cause.ret_code)
                            if isinstance(cause, BybitAPIError) else None),
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
            leg: str, price: Decimal, anchor: Decimal | None = None,
    ) -> ExchangeOrder:
        """Place one bracket leg: plain limit (TP) or conditional market (SL).

        On the derivatives both legs carry the native ``reduceOnly``
        flag; the SL is a plain trigger order (an exit stop triggers
        against the position: a sell SL on a fall, a buy SL on a rise).
        On inverse ``qty`` arrives in contracts with its reduce-side
        conversion ``anchor``.
        """
        coid = envelope.client_order_id(KIND_EXIT_TP if leg == 'tp' else KIND_EXIT_SL)
        is_spot = market.category == CATEGORY_SPOT
        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'side': _SIDE_WIRE[intent.side],
            'qty': format_decimal(qty),
            'orderLinkId': coid,
        }
        if is_spot:
            body['isLeverage'] = 0
        else:
            body['reduceOnly'] = True
        if leg == 'tp':
            body['orderType'] = 'Limit'
            body['price'] = format_decimal(price)
            body['timeInForce'] = 'GTC'
        else:
            body['orderType'] = 'Market'
            if is_spot:
                body['marketUnit'] = 'baseCoin'
                body['orderFilter'] = 'StopOrder'
            else:
                body['triggerDirection'] = (TRIGGER_DIRECTION_FALL
                                            if intent.side == 'sell'
                                            else TRIGGER_DIRECTION_RISE)
            body['triggerPrice'] = format_decimal(price)
        leg_type = LegType.TAKE_PROFIT if leg == 'tp' else LegType.STOP_LOSS
        self._record_identity(coid, pine_id=intent.pine_id,
                              from_entry=intent.from_entry, leg_type=leg_type,
                              qty=float(qty))
        extras = self._record_anchor(coid, anchor, {
            'kind': 'exit_leg', 'leg': leg, 'exit_id': intent.pine_id,
        })
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
        core_qty = self._core_qty(qty, anchor)
        return ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.LIMIT if leg == 'tp' else OrderType.STOP,
            qty=core_qty,
            filled_qty=0.0,
            remaining_qty=core_qty,
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
        """Reduce the position/inventory with a market order.

        Spot sells the held base inventory (or covers with a buy); linear
        sends a native ``reduceOnly`` market order against the one-way
        position (a hedge account routes closes through the emulator's
        ``close_leg`` primitive instead of this path).
        """
        await self._ensure_broker_started()
        intent = envelope.intent
        assert isinstance(intent, CloseIntent)
        market = await asyncio.to_thread(self._broker_market)
        coid = envelope.client_order_id(KIND_CLOSE)
        label = f"{market.symbol} close id={intent.pine_id!r}"
        anchor: Decimal | None = None
        if market.is_inverse:
            qty, anchor = await self._inverse_reduce_contracts(
                market, intent.qty, intent_key=intent.intent_key, label=label,
            )
        else:
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
            'qty': format_decimal(qty),
            'orderLinkId': coid,
        }
        if market.category == CATEGORY_SPOT:
            body['marketUnit'] = 'baseCoin'
            body['isLeverage'] = 0
        else:
            body['reduceOnly'] = True
        self._record_identity(coid, pine_id=None,
                              from_entry=intent.pine_id, leg_type=LegType.CLOSE,
                              qty=float(qty))
        extras = self._record_anchor(coid, anchor, {
            'kind': 'close', 'close_of_entry': intent.pine_id,
        })
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
        core_qty = self._core_qty(qty, anchor)
        return ExchangeOrder(
            id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            order_type=OrderType.MARKET,
            qty=core_qty,
            filled_qty=0.0,
            remaining_qty=core_qty,
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
        market = await asyncio.to_thread(self._broker_market)
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

    async def _rollback_spent_sibling(
            self, market: InstrumentInfo, coid: str,
    ) -> tuple[bool, Decimal | None]:
        """Roll back one already-placed bracket leg after a spent sibling id.

        ``(True, 0)`` only when the leg is verifiably dead with nothing
        executed — the state in which the engine may safely re-dispatch
        the whole bracket under fresh ids. An ambiguous cancel is
        resolved locally instead of leaking upward (which would replace
        the spent signal and park the whole dispatch on this leg's id):
        the order is read back by its deterministic ``orderLinkId`` and
        the cancel is retried once. A NOMINAL cancel answer is no proof
        by itself either: a fill can land right before the cancel (the
        leg dies as ``PartiallyFilledCanceled`` with executed quantity)
        and the benign "order not found" mapping may hide a fully
        ``Filled`` leg — so every accepted cancel is followed by a
        readback that must show the leg dead with zero fills. A leg
        that executed — fully or partially — can never be rolled back
        (its fills are already booked by the event stream), so it
        reports ``(False, executed)`` and the caller escalates to the
        defensive-flatten path rather than re-dispatching a doubled
        exit — the executed WIRE quantity lets the caller size the
        defensive close to the confirmed residual instead of the
        pre-fill bracket size. The executed amount is ``None`` when it
        could not be measured (unreadable readback or an unresolvable
        cancel).
        """
        try:
            await self._cancel_by_coid(market, coid)
        except OrderDispositionUnknownError:
            verdict, executed = await self._rollback_readback(coid)
            if verdict is not None:
                return verdict, executed
            try:
                await self._cancel_by_coid(market, coid)
            except OrderDispositionUnknownError:
                return False, None
        verdict, executed = await self._rollback_readback(coid)
        if verdict is None:
            # A nominal cancel whose confirming readback stayed
            # undecided (leg shows alive, or both lookup endpoints
            # unreadable): the zero-fill invariant is unproven and any
            # executed number seen would not be final — escalate with
            # the fill state unknown.
            return False, None
        return verdict, executed

    async def _rollback_readback(
            self, coid: str) -> tuple[bool | None, Decimal | None]:
        """Classify a rollback leg's disposition by readback.

        Returns ``(verdict, executed)`` where ``executed`` is the leg's
        cumulative executed WIRE quantity when it could be read (base
        units on spot/linear, contracts on inverse), else ``None``.
        Verdict ``True`` — the leg is dead with zero executed quantity
        (the bracket may be re-dispatched under fresh ids); ``False`` —
        the leg executed or its fill state is unparseable (never safe
        to re-dispatch); ``None`` — undecided (still alive, or both
        lookup endpoints unreadable), the caller may retry the cancel
        once.
        """
        existing = await self._lookup_order_by_coid(coid)
        if existing is None:
            return None, None
        try:
            executed = Decimal(str(existing.get('cumExecQty') or '0'))
        except (InvalidOperation, TypeError, ValueError):
            return False, None
        if executed > 0:
            return False, executed
        if str(existing.get('orderStatus') or '') in _DEAD_ORDER_STATUSES:
            return True, Decimal(0)
        return None, None

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
        market = await asyncio.to_thread(self._broker_market)
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
            if status in _DEAD_ORDER_STATUSES:
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
        market = await asyncio.to_thread(self._broker_market)
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
        market = await asyncio.to_thread(self._broker_market)
        # Arm the engine's expected-cancel set BEFORE the venue round-trip so the
        # follow-up ``CANCELLED`` pushes for the orders this endpoint removes are
        # routed as the engine's own cancels rather than tripping the
        # ``on_unexpected_cancel`` quarantine. The marker rides the engine event
        # queue, FIFO-ordered ahead of those pushes.
        if self.native_cancel_all_expected_sink is not None:
            self.native_cancel_all_expected_sink(market.symbol)
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
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} {intent.side.upper()} entry amend "
                 f"id={intent.pine_id!r}")
        anchor: Decimal | None = None
        if market.is_inverse:
            # The dispatched anchor is pinned for the coid's whole
            # lifetime: a fill can land while the amend request is in
            # flight and it converts at the recorded anchor, so
            # installing a different anchor afterwards would mix two
            # conversion rates in one cumulative accounting (fills,
            # mirror and the order's total). Re-anchoring is only safe
            # through cancel+recreate, which ends up on a fresh coid:
            # Bybit refuses the recreate under the cancelled order's id
            # (never allows client-id reuse), ``_order_post`` raises
            # :class:`ClientOrderIdSpentError`, and the engine re-anchors
            # the envelope and re-dispatches under a bumped ``retry_seq``.
            anchor = self._inverse_anchor_for(old_coid)
            if anchor is None:
                # No recorded anchor (restart crash window) — resolve
                # through the base cancel+recreate, which re-anchors
                # the recreated dispatch cleanly.
                return await super().modify_entry(old, new)
            qty = self._inverse_entry_contracts(
                market, intent.qty, anchor,
                intent_key=intent.intent_key, label=label,
            )
        else:
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
            # Keep a both-set stop-limit's dormancy trigger in step with the
            # amended stop. If the order flipped between conditional and plain
            # (e.g. the limit is no longer marketable), the amend endpoint
            # rejects the mismatched field and ``_amend_or_none`` falls back to
            # cancel+recreate, which re-evaluates dormancy cleanly.
            dormancy_trigger = self._stop_limit_dormancy_trigger(intent, market)
            if dormancy_trigger is not None:
                body['triggerPrice'] = format_decimal(dormancy_trigger)
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
        self._dispatch_qty[old_coid] = float(qty)
        if self.store_ctx is not None:
            if anchor is not None:
                row = self.store_ctx.get_order(old_coid)
                self.store_ctx.upsert_order(
                    old_coid, qty=float(qty),
                    extras=self._record_anchor(
                        old_coid, anchor,
                        dict(row.extras or {}) if row is not None else {},
                    ),
                )
            else:
                self.store_ctx.upsert_order(old_coid, qty=float(qty))
            self.store_ctx.log_event(
                'entry_amended', client_order_id=old_coid,
                intent_key=intent.intent_key,
                payload={'qty': float(qty),
                         'price': float(price) if price is not None else None},
            )
        elif anchor is not None:
            self._wire_anchor[old_coid] = anchor
        core_qty = self._core_qty(qty, anchor)
        return [ExchangeOrder(
            id=str(result.get('orderId') or ''),
            symbol=intent.symbol,
            side=intent.side,
            order_type=intent.order_type,
            qty=core_qty,
            filled_qty=0.0,
            remaining_qty=core_qty,
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
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} exit amend id={new_intent.pine_id!r} "
                 f"from={new_intent.from_entry!r}")
        anchor: Decimal | None = None
        if market.is_inverse:
            # Both legs share the dispatched anchor and it is pinned
            # for their whole lifetime: a leg fill can land while an
            # amend request is in flight (converting at the recorded
            # anchor), so installing a different anchor afterwards
            # would mix two conversion rates in one cumulative
            # accounting. Re-anchoring is only safe through
            # cancel+recreate, which mints fresh coids.
            for kind in (KIND_EXIT_TP, KIND_EXIT_SL):
                anchor = self._inverse_anchor_for(old.client_order_id(kind))
                if anchor is not None:
                    break
            if anchor is None:
                # No recorded anchor (restart crash window) — resolve
                # through the base cancel+recreate.
                return await super().modify_exit(old, new)
            qty = self._inverse_entry_contracts(
                market, new_intent.qty, anchor,
                intent_key=new_intent.intent_key, label=label,
            )
        else:
            qty = self._quantize_or_skip(
                market, new_intent.qty, intent_key=new_intent.intent_key,
                label=label,
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
            self._dispatch_qty[old_coid] = float(qty)
            if self.store_ctx is not None:
                if anchor is not None:
                    row = self.store_ctx.get_order(old_coid)
                    self.store_ctx.upsert_order(
                        old_coid, qty=float(qty),
                        tp_level=float(price) if leg == 'tp' else None,
                        sl_level=float(price) if leg == 'sl' else None,
                        extras=self._record_anchor(
                            old_coid, anchor,
                            dict(row.extras or {}) if row is not None else {},
                        ),
                    )
                else:
                    self.store_ctx.upsert_order(
                        old_coid, qty=float(qty),
                        tp_level=float(price) if leg == 'tp' else None,
                        sl_level=float(price) if leg == 'sl' else None,
                    )
            elif anchor is not None:
                self._wire_anchor[old_coid] = anchor
            core_qty = self._core_qty(qty, anchor)
            legs.append(ExchangeOrder(
                id=str(result.get('orderId') or ''),
                symbol=new_intent.symbol,
                side=new_intent.side,
                order_type=OrderType.LIMIT if leg == 'tp' else OrderType.STOP,
                qty=core_qty,
                filled_qty=0.0,
                remaining_qty=core_qty,
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
                    predecessor_cancel_ids=(),
                ) from e
            raise
        except BybitError as e:
            raise OrderDispositionUnknownError(
                f"Bybit amend transport failure for {coid}; disposition unknown",
                client_order_id=coid, cause=e,
                predecessor_cancel_ids=(),
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
