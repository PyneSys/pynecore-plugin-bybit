"""Broker state-query mix-in for the Bybit plugin (spot M2, linear M3,
inverse M4).

Implements the read side of :class:`~pynecore.core.plugin.broker.BrokerPlugin`
plus the broker lifecycle glue:

- :meth:`get_capabilities` — the per-category capability profile.
- :attr:`account_id` — lazily latched from ``GET /v5/user/query-api``.
  Bybit's whole data-provider path is public, so unlike Capital.com /
  cTrader there is no earlier authenticated moment on the warmup path;
  the first read of the property (the startup contract probe) performs
  the one signed identity call and caches it.
- :meth:`_ensure_broker_started` — the per-category one-shot startup
  (spot: fail-closed
  :class:`~pynecore.core.broker.spot_inventory.SpotInventoryManager`
  startup; derivatives: position-mode detection + hedge-mode opt-in to
  the core one-way emulation), awaited by every broker entry point so it
  always precedes the engine's startup reconcile (whose first
  ``get_position`` read lands here).
- :meth:`get_open_orders` / :meth:`get_position` / :meth:`get_balance`.
"""
import asyncio
import logging
from dataclasses import replace

from pynecore.core.broker.exceptions import (
    ExchangeCapabilityError,
    ExchangeConnectionError,
)
from pynecore.core.broker.models import (
    CapabilityLevel,
    ExchangeCapabilities,
    ExchangeOrder,
    ExchangePosition,
    OrderStatus,
    OrderType,
)
from pynecore.core.plugin import override

from ._base import _BybitBase
from .exceptions import (
    BybitAPIError,
    BybitConnectionError,
    BybitError,
    map_broker_error,
)
from .helpers import (
    ACCOUNT_TYPE_UNIFIED,
    CATEGORY_SPOT,
    OPEN_ORDERS_PAGE_LIMIT,
)
from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: ``GET /v5/order/realtime`` ``orderStatus`` -> PyneCore order status.
#: Only statuses the endpoint actually serves for open/recent orders are
#: mapped; anything unknown is reported as OPEN (conservative: the engine
#: keeps tracking it instead of dropping a live order).
_ORDER_STATUS_MAP = {
    'New': OrderStatus.OPEN,
    'PartiallyFilled': OrderStatus.PARTIALLY_FILLED,
    'Untriggered': OrderStatus.OPEN,
    'Triggered': OrderStatus.OPEN,
    'Filled': OrderStatus.FILLED,
    'Cancelled': OrderStatus.CANCELLED,
    'PartiallyFilledCanceled': OrderStatus.CANCELLED,
    'Rejected': OrderStatus.REJECTED,
    'Deactivated': OrderStatus.CANCELLED,
}


def _order_type_of(entry: dict) -> OrderType:
    """Map one realtime/WS order payload to the PyneCore order type."""
    if str(entry.get('triggerPrice') or ''):
        return OrderType.STOP
    return (OrderType.LIMIT if str(entry.get('orderType') or '') == 'Limit'
            else OrderType.MARKET)


def parse_exchange_order(entry: dict) -> ExchangeOrder:
    """Build an :class:`ExchangeOrder` from one order payload.

    Shared by :meth:`_StateMixin.get_open_orders` and the private-stream
    ``order`` topic — the REST ``realtime`` rows and the WS pushes carry
    the same field names.
    """
    qty = float(entry.get('qty') or 0.0)
    filled = float(entry.get('cumExecQty') or 0.0)
    price = float(entry.get('price') or 0.0)
    trigger = float(entry.get('triggerPrice') or 0.0)
    avg = float(entry.get('avgPrice') or 0.0)
    created_ms = float(entry.get('createdTime') or 0.0)
    return ExchangeOrder(
        id=str(entry.get('orderId') or ''),
        symbol=str(entry.get('symbol') or ''),
        side=str(entry.get('side') or '').lower(),
        order_type=_order_type_of(entry),
        qty=qty,
        filled_qty=filled,
        remaining_qty=max(0.0, qty - filled),
        price=price or None,
        stop_price=trigger or None,
        average_fill_price=avg or None,
        status=_ORDER_STATUS_MAP.get(
            str(entry.get('orderStatus') or ''), OrderStatus.OPEN,
        ),
        timestamp=created_ms / 1000.0,
        fee=float(entry.get('cumExecFee') or 0.0),
        fee_currency='',
        reduce_only=bool(entry.get('reduceOnly', False)),
        client_order_id=str(entry.get('orderLinkId') or '') or None,
    )


class _StateMixin(_BybitBase):
    """Broker state queries, capability declaration and startup glue."""

    # --- account identity ---------------------------------------------------

    @property
    def account_id(self) -> str:
        """Plugin-qualified account identifier, latched lazily.

        Bybit's provider path is fully public, so no authenticated call
        precedes the startup contract probe that reads this property.
        The first read with credentials configured performs one signed
        ``GET /v5/user/query-api`` and caches
        ``bybit-{demo|live}-{userID}``; without credentials the base
        ``"default"`` sentinel is returned (data-only paths never
        authenticate).

        :raises pynecore.core.broker.exceptions.AuthenticationError: When
            credentials are configured but rejected (including the
            ~90-day key expiry) — a broker run must fail loudly here,
            not trade on a half-working key.
        """
        if self._account_id is None and self.config.api_key:
            result = self('/v5/user/query-api', auth=True)
            uid = str(result.get('userID') or '')
            if not uid:
                raise BybitConnectionError(
                    "Bybit query-api returned no userID — cannot derive "
                    "the account identity"
                )
            env = 'demo' if self.config.demo else 'live'
            self._account_id = f"bybit-{env}-{uid}"
        return self._account_id or "default"

    # --- capabilities ---------------------------------------------------------

    def _spot_market(self) -> InstrumentInfo:
        """Return the chart instrument, refusing non-spot categories.

        For the spot-only code paths (the inventory port construction);
        the shared broker paths gate through :meth:`_broker_market`.
        """
        market = self._broker_market()
        if market.category != CATEGORY_SPOT:
            raise ExchangeCapabilityError(
                f"Bybit spot inventory path invoked on a {market.category} "
                f"instrument ({market.symbol!r}) — this is a plugin bug"
            )
        return market

    def _broker_market(self) -> InstrumentInfo:
        """Return the chart instrument for the broker paths.

        The broker surface covers every category the resolver serves:
        spot (M2), linear (M3) and inverse (M4). Kept as the semantic
        chokepoint every broker entry point routes through, so a future
        category gap has exactly one place to gate.
        """
        return self._get_market()

    @override
    def get_capabilities(self) -> ExchangeCapabilities:
        """Declare the chart instrument's category capability profile.

        **Spot** — verified live on the global demo (2026-07-16):
        conditional ``StopOrder`` placement, ``/v5/order/amend`` price
        amend on a live spot order, ``orderLinkId`` duplicate rejection
        (retCode 170141) and ``/v5/order/cancel-all``. Deliberately
        conservative where the venue has no primitive:

        - ``tp_sl_bracket`` SOFTWARE — the plugin places a plain limit TP
          leg and a conditional stop SL leg; the engine owns the OCA
          cascade and partial-fill amends.
        - ``trailing_stop`` UNSUPPORTED — spot has no server-side
          trailing, and the core engine has no software-trailing driver
          for full-row exits; declaring SOFTWARE would promise semantics
          nobody upholds. Scripts using ``trail_*`` are rejected at
          startup.
        - ``reduce_only`` SOFTWARE — spot has no reduce-only flag; the
          semantics are upheld structurally (a sell cannot exceed the
          held base inventory, so an exit can never flip the book) plus
          the engine's projected-position gate on the short side.
        - ``short_selling`` UNSUPPORTED — the spot ledger models
          long-only exposure (mutually exclusive with the inventory
          port by core contract).

        **Linear / inverse** — the position-based profile:
        ``reduce_only`` / ``fetch_position`` / ``short_selling`` NATIVE
        (explicit ``reduceOnly`` flag, ``/v5/position/list``, shorting is
        natural). The exit bracket stays the engine-driven SOFTWARE pair
        of reduce-only order legs (the ``trading-stop`` position attach
        is assessed on the demo before any raise, per the plan), so
        ``tp_sl_bracket`` / ``oca_cancel`` match spot. ``trailing_stop``
        UNSUPPORTED until the ``trading-stop`` trailing activation
        semantics are verified live — same no-false-promise rule as spot.
        Inverse shares the linear profile: the venue primitives are the
        same endpoints; only the qty denomination differs (whole USD
        contracts — mapped in the execution/positions mix-ins).
        """
        market = self._broker_market()
        if market.category == CATEGORY_SPOT:
            return ExchangeCapabilities(
                stop_order=CapabilityLevel.NATIVE,
                trailing_stop=CapabilityLevel.UNSUPPORTED,
                tp_sl_bracket=CapabilityLevel.SOFTWARE,
                partial_qty_bracket_exit=CapabilityLevel.SOFTWARE,
                partial_qty_bracket_exit_pyramiding=CapabilityLevel.SOFTWARE,
                oca_cancel=CapabilityLevel.SOFTWARE,
                amend_order=CapabilityLevel.PARTIAL_NATIVE,
                cancel_all=CapabilityLevel.NATIVE,
                reduce_only=CapabilityLevel.SOFTWARE,
                watch_orders=CapabilityLevel.NATIVE,
                fetch_position=CapabilityLevel.SOFTWARE,
                idempotency=CapabilityLevel.NATIVE,
                short_selling=CapabilityLevel.UNSUPPORTED,
            )
        return ExchangeCapabilities(
            stop_order=CapabilityLevel.NATIVE,
            trailing_stop=CapabilityLevel.UNSUPPORTED,
            tp_sl_bracket=CapabilityLevel.SOFTWARE,
            partial_qty_bracket_exit=CapabilityLevel.SOFTWARE,
            partial_qty_bracket_exit_pyramiding=CapabilityLevel.SOFTWARE,
            oca_cancel=CapabilityLevel.SOFTWARE,
            amend_order=CapabilityLevel.PARTIAL_NATIVE,
            cancel_all=CapabilityLevel.NATIVE,
            reduce_only=CapabilityLevel.NATIVE,
            watch_orders=CapabilityLevel.NATIVE,
            fetch_position=CapabilityLevel.NATIVE,
            idempotency=CapabilityLevel.NATIVE,
            short_selling=CapabilityLevel.NATIVE,
        )

    # --- broker startup -------------------------------------------------------

    async def _ensure_broker_started(self) -> None:
        """Run the category's one-shot broker startup sequence.

        Awaited by every broker entry point (state reads, dispatches and
        the ``watch_orders`` loop), so whichever the engine drives first
        — in production the startup reconcile's ``get_position`` — runs
        it before any dispatch.

        Spot constructs + fail-closed-starts the core spot inventory
        manager; without persistence (``store_ctx is None``: unit tests,
        one-shot paths) the manager stays ``None`` and the plugin serves
        venue state directly.

        The derivative categories detect the account's position mode
        once. A HEDGE account
        opts into the core one-way emulation by publishing the
        :class:`~pynecore.core.plugin.broker.PositionPort` surface
        (``position_port = self``, the cTrader HEDGED precedent); a
        one-way account keeps the cheaper netting-native ``execute_*``
        path. The engine reads ``position_port`` per dispatch, so setting
        it here — before the first dispatch — is race-free.
        """
        if self._broker_started:
            return
        async with self._broker_start_lock:
            if self._broker_started:
                return
            market = await asyncio.to_thread(self._broker_market)
            if market.category != CATEGORY_SPOT:
                from .positions import POSITION_MODE_HEDGE
                mode = await self._detect_position_mode(market)
                self._position_mode = mode
                if mode == POSITION_MODE_HEDGE:
                    if market.is_inverse:
                        # The PositionPort volume contract is price-blind
                        # and cannot carry the inverse base->contract
                        # conversion; the account setting is the user's.
                        raise ExchangeCapabilityError(
                            f"Bybit inverse trading requires one-way "
                            f"position mode; the account holds "
                            f"{market.symbol!r} legs in hedge mode. Switch "
                            f"the contract to one-way mode on Bybit and "
                            f"restart."
                        )
                    self.position_port = self
                if market.is_inverse:
                    self._inverse_seed_net(
                        await self._fetch_position_rows(market),
                    )
                logger.info(
                    "Bybit %s broker ready: %s position mode%s",
                    market.category, mode.replace('_', '-'),
                    " (core one-way emulation active)"
                    if mode == POSITION_MODE_HEDGE else "",
                )
                self._broker_started = True
                return
            if self.store_ctx is not None:
                from pynecore.core.broker.spot_inventory import SpotInventoryManager
                from .inventory import spot_port_for
                port = spot_port_for(self, market)
                # Exposed for the startup contract probe's port-surface
                # check and for operator introspection.
                self.spot_inventory_port = port
                manager = SpotInventoryManager(
                    self.store_ctx,
                    port,
                    account_id=self.account_id,
                    symbol=self.symbol or market.symbol,
                    request_quarantine=self.quarantine_sink,
                    on_inventory_conflict=self.on_inventory_conflict,
                )
                result = await manager.startup()
                self._spot_manager = manager
                self._spot_port = port
                if result.quarantined:
                    logger.error(
                        "Bybit spot inventory startup quarantined: %s",
                        result.reason,
                    )
                else:
                    logger.info(
                        "Bybit spot inventory ready: net_base=%s fill_count=%d "
                        "(recovered=%d, adopted=%d)",
                        result.fold.net_base, result.fold.fill_count,
                        result.recovered_fills, result.adopted_fills,
                    )
            self._broker_started = True

    # --- state queries ---------------------------------------------------------

    @override
    async def get_open_orders(self, symbol: str | None = None) -> list[ExchangeOrder]:
        """Fetch the account's open orders via ``GET /v5/order/realtime``.

        Cursor-paged; covers plain and conditional (trigger) orders — the
        realtime endpoint returns both for every category. ``symbol``
        defaults to the chart instrument. Inverse rows arrive
        contract-denominated and convert to the core's base view at each
        order's recorded anchor.
        """
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._broker_market)
        native_symbol = market.symbol
        if symbol is not None and symbol not in (self.symbol, native_symbol):
            # Single-instrument plugin: an unknown symbol has no orders
            # rather than leaking another instrument's book.
            return []
        out: list[ExchangeOrder] = []
        cursor: str | None = None
        while True:
            try:
                result = await self._call('/v5/order/realtime', {
                    'category': market.category,
                    'symbol': native_symbol,
                    'limit': OPEN_ORDERS_PAGE_LIMIT,
                    'cursor': cursor,
                }, auth=True)
            except BybitError as e:
                raise self._classify_read_error(e) from e
            out.extend(parse_exchange_order(entry)
                       for entry in result.get('list') or [])
            cursor = result.get('nextPageCursor') or None
            if not cursor:
                break
        if market.is_inverse:
            out = [self._inverse_order_to_base(order) for order in out]
        return out

    def _inverse_order_to_base(self, order: ExchangeOrder) -> ExchangeOrder:
        """Convert one wire (contract-denominated) order row to base units.

        The dispatch's recorded anchor when resolvable, else the order's
        own price level (limit / trigger / average fill), else the last
        trade — an order with no price context at all stays in the wire
        view rather than being dropped.
        """
        fallback = (order.price or order.stop_price
                    or order.average_fill_price or self._last_price)
        anchor = self._inverse_anchor_for(
            order.client_order_id or '', fallback=fallback,
        )
        if anchor is None or anchor <= 0:
            return order
        factor = float(anchor)
        return replace(
            order,
            qty=order.qty / factor,
            filled_qty=order.filled_qty / factor,
            remaining_qty=order.remaining_qty / factor,
        )

    @override
    async def get_position(self, symbol: str) -> ExchangePosition | None:
        """Return the chart symbol's position (category-specific source).

        **Linear / inverse** reads the venue's native position object: a
        one-way account serves the single ``/v5/position/list`` row
        directly; a hedge account aggregates its raw legs through the
        core emulator's netting view (the cTrader HEDGED pattern).
        Inverse rows are contract-denominated and are reported back in
        base units (see ``positions.py``).

        **Spot** synthesizes from the core inventory ledger.
        ``None`` is an authoritative flat by engine contract. Sub-grid
        dust counts as flat: Bybit charges the buy-side fee in the BASE
        coin, so a full buy→sell round trip leaves a residue below
        ``basePrecision`` that can never be sold — reporting it as a live
        micro-long would keep ``strategy.position_size`` non-zero forever
        and block flat-gated re-entries. The exact ledger (and the
        balance invariant) keeps carrying the dust; only the engine-facing
        view snaps to flat. The residue is self-draining: each round trip
        sells the floor of the fee-adjusted inventory, so the dust stays
        bounded below one quantity-grid step.

        Without a store-backed inventory manager (persistence off) there
        is no ledger to synthesize from, so the plugin reports flat —
        matching the pre-persistence test paths of the other plugins.
        """
        await self._ensure_broker_started()
        market = await asyncio.to_thread(self._broker_market)
        if market.category != CATEGORY_SPOT:
            from .positions import POSITION_MODE_HEDGE
            try:
                if self._position_mode == POSITION_MODE_HEDGE:
                    from pynecore.core.broker.emulator import aggregate_positions
                    return aggregate_positions(
                        symbol, await self.fetch_raw_positions(symbol),
                    )
                return await self._fetch_deriv_position(market)
            except BybitError as e:
                raise self._classify_read_error(e) from e
        manager = self._spot_manager
        if manager is None:
            return None
        mark = self._last_price
        if mark is None:
            # No live price seen yet (startup reconcile runs before the
            # first WS push) — fall back to the ledger VWAP so the
            # position is adopted with zero unrealized PnL rather than a
            # bogus mark.
            vwap = manager.fold.vwap
            mark = float(vwap) if vwap is not None else 0.0
        position = manager.synthesize_position(mark)
        if position is not None \
                and market.qty_step > 0 and position.size < market.qty_step:
            return None
        return position

    @override
    async def get_balance(self) -> dict[str, float]:
        """Return the unified wallet's per-coin total balances."""
        await self._ensure_broker_started()
        try:
            result = await self._call('/v5/account/wallet-balance', {
                'accountType': ACCOUNT_TYPE_UNIFIED,
            }, auth=True)
        except BybitError as e:
            raise self._classify_read_error(e) from e
        balances: dict[str, float] = {}
        for account in result.get('list') or []:
            for coin in account.get('coin') or []:
                name = str(coin.get('coin') or '')
                if not name:
                    continue
                try:
                    balances[name] = float(coin.get('walletBalance') or 0.0)
                except (TypeError, ValueError):
                    continue
        return balances

    async def _fetch_wallet_coin(self, coin: str) -> dict:
        """Return the raw wallet-balance record of one coin (empty if absent)."""
        result = await self._call('/v5/account/wallet-balance', {
            'accountType': ACCOUNT_TYPE_UNIFIED,
            'coin': coin,
        }, auth=True)
        for account in result.get('list') or []:
            for entry in account.get('coin') or []:
                if str(entry.get('coin') or '') == coin:
                    return entry
        return {}

    @staticmethod
    def _classify_read_error(e: BybitError) -> Exception:
        """Map a REST failure on a state READ into the broker taxonomy.

        Reads are idempotent, so everything transient collapses to
        :class:`ExchangeConnectionError` (the engine parks the cycle and
        retries next bar); credential/rate-limit rejects keep their
        specific classes via :func:`map_broker_error`.
        """
        if isinstance(e, BybitAPIError):
            mapped = map_broker_error(e)
            if mapped is not None:
                return mapped
        return ExchangeConnectionError(str(e))
