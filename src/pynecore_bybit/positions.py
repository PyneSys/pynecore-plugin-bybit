"""Linear-derivative position mix-in for the Bybit plugin (M3).

Implements the venue position path of the linear category:

- Position-mode detection (one-way vs hedge). Measured on the global demo
  (2026-07-16): a symbol-filtered ``GET /v5/position/list`` returns one row
  with ``positionIdx=0`` on a one-way account and two rows with
  ``positionIdx=[1, 2]`` on a hedge account — zero-size rows included, so
  the probe works on a flat account.
- :meth:`get_position` source for one-way accounts (the netting-native
  fast path) and the raw-leg read behind the core one-way emulation.
- The :class:`~pynecore.core.plugin.broker.PositionPort` transport
  primitives. On a HEDGE account ``_ensure_broker_started`` sets
  ``self.position_port = self`` (the cTrader HEDGED precedent) and the
  core :class:`~pynecore.core.broker.one_way_emulator.OneWayEmulator`
  drives close / reversal / bracket through these — each method sends or
  reads exactly ONE broker entity; all netting / FIFO logic lives in core.
  A hedge account holds at most two aggregate legs per symbol (the Buy leg
  ``positionIdx=1`` and the Sell leg ``positionIdx=2``), a degenerate case
  of the emulator's general multi-leg model, addressed by the index.
- The last-known net-size cache the event stream's entry-row flat sweep
  keys off (fed by the private WS ``position`` topic and the periodic
  reconcile snapshot).

The hedge bracket primitive (:meth:`amend_bracket`) maps to
``POST /v5/position/trading-stop`` — a position attribute Bybit overwrites
wholesale, so an all-``None`` amend clears it, mirroring cTrader.
"""
import asyncio
import logging
from decimal import ROUND_DOWN, Decimal
from typing import Callable

from pynecore.core.broker.exceptions import ExchangeOrderRejectedError
from pynecore.core.broker.models import (
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExchangePosition,
    LegType,
    OrderType,
    PositionLeg,
)

from ._base import _BybitBase
from .exceptions import (
    AMBIGUOUS_DISPOSITION_CODES,
    BybitAPIError,
    BybitError,
    is_benign_trading_stop_reject,
    map_broker_error,
    reject_error,
)
from .helpers import format_decimal, round_price, wire_link_id
from .models import InstrumentInfo

logger = logging.getLogger(__name__)

POSITION_MODE_ONE_WAY = 'one_way'
POSITION_MODE_HEDGE = 'hedge'

#: ``positionIdx`` of the two aggregate hedge legs.
HEDGE_IDX_BUY = 1
HEDGE_IDX_SELL = 2

#: ``tradeMode`` -> engine ``margin_mode`` wording.
_MARGIN_MODE = {0: 'cross', 1: 'isolated'}


class _PositionsMixin(_BybitBase):
    """Linear position path: mode detection, venue reads, PositionPort."""

    # --- mode detection ------------------------------------------------------

    async def _detect_position_mode(self, market: InstrumentInfo) -> str:
        """Detect the account's position mode for the chart symbol.

        A hedge account serves the two aggregate legs (``positionIdx``
        1 and 2) even at zero size, a one-way account the single
        ``positionIdx=0`` row — measured on the global demo, see the
        module docstring.
        """
        rows = await self._fetch_position_rows(market)
        for row in rows:
            if int(row.get('positionIdx') or 0) in (HEDGE_IDX_BUY, HEDGE_IDX_SELL):
                return POSITION_MODE_HEDGE
        return POSITION_MODE_ONE_WAY

    # --- venue reads -----------------------------------------------------------

    async def _fetch_position_rows(self, market: InstrumentInfo) -> list[dict]:
        """Return the raw ``/v5/position/list`` rows of the chart symbol."""
        result = await self._call('/v5/position/list', {
            'category': market.category,
            'symbol': market.symbol,
        }, auth=True)
        return list(result.get('list') or [])

    @staticmethod
    def _position_row_size(row: dict) -> float:
        """Parse one position row's open size (0.0 when flat/unparsable)."""
        try:
            return float(row.get('size') or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _ingest_position_sizes(self, rows: list[dict]) -> None:
        """Update the net-size cache from position rows (WS push or REST).

        The cache only drives the entry-row flat sweep between reconcile
        snapshots; the engine-facing reads stay REST-authoritative.
        """
        sizes = self._linear_sizes
        if sizes is None:
            sizes = {}
            self._linear_sizes = sizes
        for row in rows:
            idx = int(row.get('positionIdx') or 0)
            sizes[idx] = self._position_row_size(row)

    def _linear_is_flat(self) -> bool:
        """Whether the last-known venue position of the symbol is flat.

        ``False`` while no position snapshot has been seen yet — the sweep
        must never close entry rows on ignorance.
        """
        sizes = self._linear_sizes
        if sizes is None:
            return False
        return all(size <= 0.0 for size in sizes.values())

    async def _fetch_linear_position(
            self, market: InstrumentInfo,
    ) -> ExchangePosition | None:
        """Read the one-way position from the venue (``None`` = flat).

        ``None`` is an authoritative flat by engine contract; a zero-size
        row (Bybit serves those for symbol queries) reports flat.
        """
        rows = await self._fetch_position_rows(market)
        self._ingest_position_sizes(rows)
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            return ExchangePosition(
                symbol=self.symbol or market.symbol,
                side='long' if side == 'buy' else 'short',
                size=size,
                entry_price=float(row.get('avgPrice') or 0.0),
                unrealized_pnl=float(row.get('unrealisedPnl') or 0.0),
                liquidation_price=float(row.get('liqPrice') or 0.0) or None,
                leverage=float(row.get('leverage') or 0.0),
                margin_mode=_MARGIN_MODE.get(
                    int(row.get('tradeMode') or 0), 'cross',
                ),
            )
        return None

    # --- PositionPort transport surface (core one-way emulation) ---------------
    #
    # Only wired on a HEDGE account (``position_port = self``); a one-way
    # account keeps the cheaper netting-native ``execute_*`` path.

    async def fetch_raw_positions(self, symbol: str) -> list[PositionLeg]:
        """Return every open hedge leg of ``symbol``, oldest first.

        One :class:`PositionLeg` per non-flat ``positionIdx`` row, ZERO
        aggregation — the core emulator owns netting and leg selection.
        The leg id is the ``positionIdx`` (the address ``close_leg`` and
        ``amend_bracket`` need); ``open_time`` comes from the broker's
        ``createdTime`` so the FIFO order is replay-stable.
        """
        market = await asyncio.to_thread(self._broker_market)
        if symbol not in (self.symbol, market.symbol):
            return []
        rows = await self._fetch_position_rows(market)
        self._ingest_position_sizes(rows)
        legs: list[PositionLeg] = []
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            if side not in ('buy', 'sell'):
                continue
            legs.append(PositionLeg(
                leg_id=str(int(row.get('positionIdx') or 0)),
                symbol=symbol,
                side=side,
                qty=size,
                entry_price=float(row.get('avgPrice') or 0.0),
                open_time=float(row.get('createdTime') or 0.0) / 1000.0,
                unrealized_pnl=float(row.get('unrealisedPnl') or 0.0),
            ))
        legs.sort(key=lambda leg: leg.open_time)
        return legs

    async def get_volume_quantizer(self, symbol: str) -> Callable[[float], int]:
        """Return a sync Pine-units -> qty-grid-step-count quantizer.

        The broker-grid integer is the number of ``qtyStep`` units — the
        closure captures the immutable step so the emulator can snap
        per-leg volumes without an await per call; ``close_leg`` converts
        the step count back to the wire quantity with the same step.
        """
        market = await asyncio.to_thread(self._broker_market)
        step = Decimal(market.qty_step_str)
        if step <= 0:
            raise ExchangeOrderRejectedError(
                f"Bybit instrument {market.symbol!r} reports no usable "
                f"qtyStep ({market.qty_step_str!r})"
            )
        return lambda units: int(
            (Decimal(str(units)) / step).to_integral_value(ROUND_DOWN)
        )

    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None:
        """Reduce ONE hedge leg by ``volume`` grid steps under ``coid``.

        A reduce-only market order addressed to the leg's ``positionIdx``;
        the resulting fill arrives on the regular ``execution`` push. The
        emulator composes ``coid`` as ``{parent_coid}:{leg_id}`` — the
        colon is outside Bybit's ``orderLinkId`` charset, so the wire
        carries its deterministic :func:`~pynecore_bybit.helpers.wire_link_id`
        form (identity, lookup and the duplicate-reject adoption all key
        on the same mapped id).
        """
        market = await asyncio.to_thread(self._broker_market)
        idx = int(leg_id)
        qty = Decimal(volume) * Decimal(market.qty_step_str)
        side = 'Sell' if idx == HEDGE_IDX_BUY else 'Buy'
        link_id = wire_link_id(coid)
        self._record_identity(link_id, pine_id=None, from_entry=None,
                              leg_type=LegType.CLOSE, qty=float(qty))
        await self._order_post('/v5/order/create', {
            'category': market.category,
            'symbol': market.symbol,
            'side': side,
            'orderType': 'Market',
            'qty': format_decimal(qty),
            'orderLinkId': link_id,
            'reduceOnly': True,
            'positionIdx': idx,
        }, coid=link_id, context="close leg")

    async def reject_out_of_range(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> None:
        """Raise the non-halting volume-bounds skip when ``qty`` is out of range."""
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} {intent.side.upper()} reversal residual "
                 f"id={intent.pine_id!r}")
        quantized = self._quantize_or_skip(
            market, qty, intent_key=intent.intent_key, label=label,
        )
        self._preflight_order(
            market, quantized, is_market=intent.order_type is not OrderType.LIMIT,
            price=None, intent_key=intent.intent_key, label=label,
        )

    async def place_leg(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> list[ExchangeOrder]:
        """Open ONE order of ``qty`` Pine units for the envelope's entry intent.

        The residual leg of a reversal or a plain add — delegates to the
        shared entry-order builder, which stamps the hedge ``positionIdx``
        from the intent side.
        """
        intent = envelope.intent
        assert isinstance(intent, EntryIntent)
        market = await asyncio.to_thread(self._broker_market)
        label = (f"{market.symbol} {intent.side.upper()} leg "
                 f"id={intent.pine_id!r}")
        quantized = self._quantize_or_skip(
            market, qty, intent_key=intent.intent_key, label=label,
        )
        return await self._place_entry_order(envelope, intent, market, quantized)

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            coid: str,
    ) -> None:
        """Replicate (or, all-``None``, clear) the bracket on ONE hedge leg.

        ``POST /v5/position/trading-stop`` sets the position-attribute
        TP / SL / trailing of the addressed ``positionIdx``; Bybit clears a
        field on the literal ``"0"``, so an all-``None`` amend wipes the
        bracket wholesale. ``side`` is unused — Bybit needs no anchor seed
        for a trailing distance (``trailingStop`` activates immediately
        without an ``activePrice``). A leg that vanished between the
        emulator's fetch and this amend rejects with the measured
        zero-position response, an idempotent re-amend with "not modified"
        — both benign no-ops (see
        :func:`~pynecore_bybit.exceptions.is_benign_trading_stop_reject`).
        """
        del side  # Bybit derives the protective side from the leg itself.
        market = await asyncio.to_thread(self._broker_market)
        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'positionIdx': int(leg_id),
            'tpslMode': 'Full',
            'takeProfit': (format_decimal(round_price(tp_price, market.tick_size_str))
                           if tp_price is not None else '0'),
            'stopLoss': (format_decimal(round_price(sl_price, market.tick_size_str))
                         if sl_price is not None else '0'),
            'trailingStop': (format_decimal(round_price(trail_offset,
                                                        market.tick_size_str))
                             if trail_offset is not None else '0'),
        }
        try:
            await self._call('/v5/position/trading-stop', method='post',
                             body=body, auth=True)
        except BybitAPIError as e:
            if is_benign_trading_stop_reject(e):
                return
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                # Server-side failure — surface as a rejection so the
                # emulator's attach path runs its defensive flatten instead
                # of trusting an unprotected leg.
                raise ExchangeOrderRejectedError(
                    f"Bybit trading-stop server-side failure on leg {leg_id} "
                    f"(retCode={e.ret_code})"
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BybitError as e:
            raise ExchangeOrderRejectedError(
                f"Bybit trading-stop transport failure on leg {leg_id}: {e}"
            ) from e
