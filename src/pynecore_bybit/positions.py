"""Derivative position mix-in for the Bybit plugin (linear M3, inverse M4).

Implements the venue position path of the derivative categories:

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
  reconcile snapshot). Wire units (contracts on inverse) — the sweep only
  asks "flat or not".
- The inverse net-position mirror (venue contracts + the base reported to
  the core), seeded from the venue at startup and folded from this
  strategy's own fills — the reduce-side base->contract conversions in
  ``execution.py`` run through its effective anchor.

The hedge bracket primitive (:meth:`amend_bracket`) maps to
``POST /v5/position/trading-stop`` — a position attribute Bybit overwrites
wholesale, so an all-``None`` amend clears it, mirroring cTrader. The
``PositionPort`` surface is linear-only: Bybit supports hedge mode on USDT
perpetuals and inverse futures only, and the port's price-blind volume
contract cannot carry the inverse base->contract conversion, so an
inverse hedge account is refused at broker startup with instructions to
switch to one-way mode.
"""
import asyncio
import logging
import math
from abc import ABC
from decimal import ROUND_DOWN, Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING, Callable

from pynecore.core.broker.exceptions import (
    ExchangeOrderRejectedError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.models import (
    DispatchEnvelope,
    EntryIntent,
    ExchangeOrder,
    ExchangePosition,
    LegType,
    OrderType,
    PositionLeg,
)
from pynecore.core.broker.one_way_emulator import close_leg_row_client_order_id
from pynecore.core.broker.protection_guard import ProtectionLevels
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    LEDGER_ROW_STATES,
    PENDING_DISPATCH_STATES,
    iter_active_bracket_ownerships,
)
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _BybitBase
from .exceptions import (
    AMBIGUOUS_DISPOSITION_CODES,
    BybitAdoptionBaselineError,
    BybitAPIError,
    BybitError,
    BybitPositionDataError,
    classify_read_error,
    is_benign_trading_stop_reject,
    map_broker_error,
    reject_error,
)
from .helpers import (
    CATEGORY_SPOT,
    CORE_CLIENT_ORDER_ID_EXTRA,
    ENTRY_CANCEL_CUM_EXEC_EXTRA,
    ENTRY_EXECUTION_HOLD_EXTRA,
    ENTRY_REMAINDER_CANCELLED_EXTRA,
    EXECUTION_SINCE_SKEW_MS,
    contracts_to_base,
    execution_sort_key,
    format_decimal,
    round_price,
    trading_stop_key,
    trading_stop_levels,
    venue_filled_qty,
    wire_link_id,
)
from .models import InstrumentInfo

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

POSITION_MODE_ONE_WAY = 'one_way'
POSITION_MODE_HEDGE = 'hedge'

#: ``positionIdx`` of the two aggregate hedge legs.
HEDGE_IDX_BUY = 1
HEDGE_IDX_SELL = 2

#: ``tradeMode`` -> engine ``margin_mode`` wording.
_MARGIN_MODE = {0: 'cross', 1: 'isolated'}

#: Max snapshot-stability attempts of the adoption baseline. A fill racing
#: the baseline's reads changes the position snapshot and forces another
#: pass; startup fills are rare, so the loop converges almost always on the
#: first pass. When it does not, the baseline is NOT latched and the read
#: raises :class:`BybitAdoptionBaselineError` (retryable) so the engine's
#: one-shot startup adoption never runs on an unbaselined snapshot.
_ADOPTION_STABLE_ATTEMPTS = 3

#: Full-fill slack of the baseline's terminal close, matching ``events.py``'s
#: ``_FILL_EPS`` (the cursor and ``qty`` both round-trip exact decimal
#: strings through float; the slack only absorbs that round-trip).
_FILL_EPS = 1e-9


def _wire_float(value: object, *, field: str) -> float:
    """Parse a finite JSON scalar as ``float``; empty values map to zero."""
    if value is None or value == '':
        return 0.0
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise BybitPositionDataError(
            f"Bybit position field {field!r} must be numeric, got "
            f"{type(value).__name__}"
        )
    try:
        number = float(value)
    except ValueError as exc:
        raise BybitPositionDataError(
            f"Bybit position field {field!r} is not numeric: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise BybitPositionDataError(
            f"Bybit position field {field!r} must be finite, got {value!r}"
        )
    return number


def _wire_int(value: object, *, field: str) -> int:
    """Parse an integral JSON scalar; empty values map to zero."""
    if value is None or value == '':
        return 0
    if isinstance(value, bool):
        raise BybitPositionDataError(
            f"Bybit position field {field!r} must be an integer, got bool"
        )
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise BybitPositionDataError(
            f"Bybit position field {field!r} must be an integer, got "
            f"{type(value).__name__}"
        )
    try:
        return int(value)
    except ValueError as exc:
        raise BybitPositionDataError(
            f"Bybit position field {field!r} is not an integer: {value!r}"
        ) from exc


class _PositionsMixin(_BybitBase, ABC):
    """Derivative position path: mode detection, venue reads, PositionPort."""

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
            if self._position_row_index(row) in (HEDGE_IDX_BUY, HEDGE_IDX_SELL):
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
        """Parse one position row's finite, non-negative open size."""
        size = _wire_float(row.get('size'), field='size')
        if size < 0.0:
            raise BybitPositionDataError(
                f"Bybit position field 'size' must be non-negative, got {size!r}"
            )
        return size

    @staticmethod
    def _position_row_index(row: dict) -> int:
        """Parse one position row's position index."""
        return _wire_int(row.get('positionIdx'), field='positionIdx')

    @staticmethod
    def _position_row_updated_ms(row: dict) -> int:
        """Parse one position row's non-negative ``updatedTime``."""
        updated_ms = _wire_int(row.get('updatedTime'), field='updatedTime')
        if updated_ms < 0:
            raise BybitPositionDataError(
                "Bybit position field 'updatedTime' must be non-negative, "
                f"got {updated_ms!r}"
            )
        return updated_ms

    def _ingest_position_sizes(self, rows: list[dict]) -> None:
        """Update the net-size cache from position rows (WS push or REST).

        The cache only drives the entry-row flat sweep between reconcile
        snapshots; the engine-facing reads stay REST-authoritative. The
        snapshot's freshness watermark advances alongside the sizes: the
        flat sweep compares it against the last own fill so a stale-flat
        reading (a ``position`` push that has not yet caught up with the
        bot's own fill) never retires a live entry row.

        A row older (by ``seq``) than a view of the same leg already seen is
        a delayed push: its size and time are history and touch neither the
        cache nor the watermark, so an old flat reading is never paired with
        a newer view's timestamp.
        """
        parsed: list[tuple[int, float, int, dict]] = []
        for row in rows:
            parsed.append((
                self._position_row_index(row),
                self._position_row_size(row),
                self._position_row_updated_ms(row),
                row,
            ))
        if not rows:
            return
        self._record_position_views(rows)
        current = [(idx, size, updated_ms) for idx, size, updated_ms, row in parsed
                   if not self._position_view_superseded(idx, row)]
        if not current:
            return
        sizes = self._deriv_sizes
        if sizes is None:
            sizes = {}
            self._deriv_sizes = sizes
        snapshot_ms = 0
        for idx, size, updated_ms in current:
            sizes[idx] = size
            snapshot_ms = max(snapshot_ms, updated_ms)
        # A snapshot just received reflects the venue state as of now;
        # fall back to the local clock when the rows carry no
        # ``updatedTime`` (a degenerate feed) so the freshness gate is
        # never starved of a timestamp.
        self._deriv_snapshot_ms = max(
            self._deriv_snapshot_ms,
            snapshot_ms or int(epoch_time() * 1000),
        )

    def _deriv_is_flat(self) -> bool:
        """Whether the last-known venue position of the symbol is flat.

        ``False`` while no position snapshot has been seen yet — the sweep
        must never close entry rows on ignorance. A flat reading is trusted
        only when the snapshot that produced it is at least as fresh as this
        strategy's most recent own fill: the bot's opening ``execution`` push
        can be handled before the ``position`` push refreshes the cache, and
        acting on that stale-flat view would close a freshly filled entry row
        while the venue position is still open.
        """
        sizes = self._deriv_sizes
        if sizes is None:
            return False
        if self._position_mode == POSITION_MODE_HEDGE and self.store_ctx is not None:
            return (
                abs(self._run_owned_signed_size()) <= _FILL_EPS
                and self._deriv_snapshot_ms >= self._last_own_fill_ms
            )
        if not all(size <= 0.0 for size in sizes.values()):
            return False
        return self._deriv_snapshot_ms >= self._last_own_fill_ms

    def _run_owned_signed_size(self) -> float:
        """Return this run's signed exposure from its durable entry rows.

        Bybit exposes one aggregate position per leg for the whole account.
        Those legs can include positions opened by another bot run or
        manually, so they are not an ownership boundary. The run's live entry
        rows are: each one owns what it filled, net of the exposure the
        run's reducing fills consumed from it (:meth:`_reduce_entry_ownership`
        nets them at fill time). An exit leg's or a close's own fill cursor is
        therefore never counted on top — it is already netted out of the
        entry rows. Startup-adopted foreign rows are deliberately excluded.
        """
        if self.store_ctx is None:
            return 0.0
        owned = 0.0
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            if extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
                continue
            if extras.get(ADOPTED_STARTUP_EXTRA_KEY):
                continue
            filled = float(row.filled_qty)
            if filled <= 0.0:
                continue
            owned += filled if row.side == 'buy' else -filled
        return owned

    def _held_entry_exposure(self) -> dict[str, float]:
        """The execution of the run's held entries that no journal row booked, per side.

        The engine owes its book the execution of these orders
        (:meth:`~pynecore.core.broker.storage.RunContext.held_execution_order_ids`,
        read fresh on every pass): an entry whose order ended at the venue
        while the terminal event did not report how much executed, and the
        orders a venue close the engine sent over such an entry took the
        execution of, until that close is retired.
        Its row's fill cursor never saw that execution, so the run-owned size
        (:meth:`_run_owned_signed_size`) leaves out a leg the held order may
        have opened. What the row keeps beyond its cursor is that execution:
        the venue's terminal ``cumExecQty`` once known
        (:data:`~pynecore_bybit.helpers.ENTRY_CANCEL_CUM_EXEC_EXTRA`, the row's
        ``qty`` normalized to it), otherwise the room the ended order left
        (:data:`~pynecore_bybit.helpers.ENTRY_REMAINDER_CANCELLED_EXTRA`, an
        upper bound the caller caps at the physical leg), or the row's
        whole unbooked quantity once the plugin handed the ended order to the
        engine (:data:`~pynecore_bybit.helpers.ENTRY_EXECUTION_HOLD_EXTRA`).
        A held entry whose
        order the plugin has not seen end (a cancel still parked) is left out:
        its executions arrive on the stream as usual.

        :return: Entry side (``'buy'`` / ``'sell'``) -> wire-domain quantity.
        """
        if self.store_ctx is None:
            return {}
        held_order_ids = self.store_ctx.held_execution_order_ids()
        if not held_order_ids:
            return {}
        exposure: dict[str, float] = {}
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            if extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
                continue
            if extras.get(ADOPTED_STARTUP_EXTRA_KEY):
                continue
            if not (extras.get(ENTRY_REMAINDER_CANCELLED_EXTRA)
                    or extras.get(ENTRY_EXECUTION_HOLD_EXTRA)
                    or ENTRY_CANCEL_CUM_EXEC_EXTRA in extras):
                continue
            order_id = row.exchange_order_id or str(extras.get('order_id') or '')
            if order_id not in held_order_ids:
                continue
            unbooked = float(row.qty) - float(row.filled_qty)
            if unbooked > _FILL_EPS:
                exposure[row.side] = exposure.get(row.side, 0.0) + unbooked
        return exposure

    def _scope_hedge_rows_to_run(self, rows: list[dict]) -> list[dict]:
        """Project account hedge rows onto the exposure owned by this run.

        A fresh run must see no tradable leg even when the account already
        carries a foreign hedge position. A restarted run sees only its own
        signed slice, bounded by the matching physical Bybit leg so stale
        journal state can never authorize an oversized reduction. Store-less
        unit paths retain the raw account view because they have no ownership
        ledger to consult.

        A held entry's unbooked execution (:meth:`_held_entry_exposure`) is
        the run's too: it is added to the leg of its side, bounded by the
        physical leg like the rest. Without it the leg the held order opened
        stays invisible, and the engine's close over that entry
        (``held_execution_close``, fanned out over these legs by the core
        one-way emulation) would leave the unbooked execution open.
        """
        if self.store_ctx is None:
            return rows
        owned = self._run_owned_signed_size()
        held = self._held_entry_exposure()
        shares = {
            HEDGE_IDX_BUY: max(owned, 0.0) + held.get('buy', 0.0),
            HEDGE_IDX_SELL: max(-owned, 0.0) + held.get('sell', 0.0),
        }
        scoped_rows: list[dict] = []
        for row in rows:
            share = shares.pop(self._position_row_index(row), 0.0)
            if share <= _FILL_EPS:
                continue
            physical = self._position_row_size(row)
            scoped = min(share, physical)
            if scoped <= _FILL_EPS:
                continue
            projected = dict(row)
            projected['size'] = format_decimal(Decimal(str(scoped)))
            scoped_rows.append(projected)
        return scoped_rows

    async def _fetch_deriv_position(
            self, market: InstrumentInfo,
    ) -> ExchangePosition | None:
        """Read the one-way position from the venue (``None`` = flat).

        ``None`` is an authoritative flat by engine contract; a zero-size
        row (Bybit serves those for symbol queries) reports flat. Inverse
        rows are contract-denominated; the core-facing size converts to
        base at the position's average entry price (the adoption anchor —
        the startup mirror seed uses the same conversion, so the core's
        adopted base and the reduce-side dispatches stay consistent).
        """
        rows = await self._fetch_position_rows(market)
        self._ingest_position_sizes(rows)
        rows = await self._apply_adoption_baseline(market, rows)
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            if side not in ('buy', 'sell'):
                raise BybitPositionDataError(
                    f"Bybit position field 'side' is invalid for open exposure: {side!r}"
                )
            entry_price = _wire_float(row.get('avgPrice'), field='avgPrice')
            unrealized = _wire_float(
                row.get('unrealisedPnl'), field='unrealisedPnl',
            )
            if market.is_inverse and entry_price > 0.0:
                size = self._inverse_core_size(side, size, entry_price)
                # Inverse unrealised PnL arrives in the settle coin; the
                # core's openprofit is quote-denominated — convert at the
                # mark price (falling back to the last trade, then the
                # entry price, so the number is never left settle-coined).
                mark = (_wire_float(row.get('markPrice'), field='markPrice')
                        or self._last_price or entry_price)
                unrealized *= mark
                if not math.isfinite(unrealized):
                    raise BybitPositionDataError(
                        "Bybit inverse unrealized PnL conversion is non-finite"
                    )
            liquidation = _wire_float(row.get('liqPrice'), field='liqPrice')
            leverage = _wire_float(row.get('leverage'), field='leverage')
            return ExchangePosition(
                symbol=self.symbol or market.symbol,
                side='long' if side == 'buy' else 'short',
                size=size,
                entry_price=entry_price,
                unrealized_pnl=unrealized,
                liquidation_price=liquidation or None,
                leverage=leverage,
                margin_mode=_MARGIN_MODE.get(
                    _wire_int(row.get('tradeMode'), field='tradeMode'), 'cross',
                ),
            )
        return None

    async def _venue_exposure_to_close(
            self, market: InstrumentInfo, close_side: str,
    ) -> tuple[Decimal, float] | None:
        """Read the one-way venue position a close of ``close_side`` reduces, fresh.

        The sizing source of a ``held_execution_close``
        (:meth:`~_ExecutionMixin.execute_close`): that close is sized against
        the venue, which holds an execution no journal row booked, so neither
        the journal nor the inverse net-position mirror can size it. The
        core-facing base is the figure :meth:`_fetch_deriv_position` reports
        for the same row (:meth:`_inverse_core_size` on inverse), so it is
        comparable with the close's ``qty``.

        :param market: The chart instrument (a derivative category).
        :param close_side: The close order's side (``'sell'`` closes a long).
        :return: ``(wire size, core-facing base size)`` of the position the
            close reduces: the exact wire size (base on linear, contracts on
            inverse) and the base the core is told it holds. ``None`` when the
            venue is flat or holds the other side.
        :raises ExchangeConnectionError: Or another broker-taxonomy error
            (:func:`~pynecore_bybit.exceptions.classify_read_error`) when the
            read fails; nothing was sent.
        """
        position_side = 'buy' if close_side == 'sell' else 'sell'
        try:
            rows = await self._fetch_position_rows(market)
            for row in rows:
                size = self._position_row_size(row)
                if size <= 0.0:
                    continue
                if str(row.get('side') or '').lower() != position_side:
                    return None
                wire = Decimal(str(row.get('size')))
                if not market.is_inverse:
                    return wire, size
                avg = _wire_float(row.get('avgPrice'), field='avgPrice')
                if avg <= 0.0:
                    raise BybitPositionDataError(
                        f"Bybit inverse position of {market.symbol} carries no "
                        f"usable avgPrice ({row.get('avgPrice')!r})"
                    )
                return wire, self._inverse_core_size(position_side, size, avg)
        except BybitError as e:
            raise classify_read_error(e) from e
        return None

    # --- startup adoption baseline (double-count barrier) -----------------------

    def _parked_adoption_executions(
            self, market: InstrumentInfo, order_id: str, floor_ms: int,
            known_ids: set[str],
    ) -> tuple[set[str], float, list[dict]]:
        """Return exact private execution slices parked behind the adoption gate.

        A fill can legitimately reach the startup window through BOTH the
        REST walk and a parked push, so every ``execId`` in ``known_ids``
        (the walk's) is skipped — its quantity is already in the walk's sum.

        :param known_ids: ``execId`` set the REST walk already returned.
        :return: Their ``execId`` set, summed wire-domain quantity and rows,
            all disjoint from ``known_ids``.
        """
        ids: set[str] = set()
        quantity = 0.0
        entries: list[dict] = []
        for frame in self._pre_adoption_frames:
            if str(frame.get('topic') or '') != 'execution':
                continue
            for entry in frame.get('data') or ():
                if str(entry.get('symbol') or '') != market.symbol:
                    continue
                if str(entry.get('category') or market.category) != market.category:
                    continue
                if str(entry.get('execType') or 'Trade') != 'Trade':
                    continue
                if str(entry.get('orderId') or '') != order_id:
                    continue
                exec_time = _wire_int(entry.get('execTime'), field='execTime')
                if exec_time > floor_ms:
                    continue
                exec_id = str(entry.get('execId') or '')
                if not exec_id or exec_id in ids or exec_id in known_ids:
                    continue
                ids.add(exec_id)
                quantity += _wire_float(entry.get('execQty'), field='execQty')
                entries.append(entry)
        return ids, quantity, entries

    async def _venue_time_ms(self) -> int:
        """Read the venue server clock (``/v5/market/time``) in epoch-ms."""
        result = await self._call('/v5/market/time')
        nano = result.get('timeNano')
        try:
            if nano not in (None, ''):
                return _wire_int(nano, field='timeNano') // 1_000_000
            return _wire_int(
                result.get('timeSecond'), field='timeSecond',
            ) * 1000
        except BybitPositionDataError as exc:
            raise BybitAdoptionBaselineError(
                "adoption baseline: venue time response is invalid"
            ) from exc

    @staticmethod
    def _position_rows_signature(rows: list[dict]) -> tuple:
        """Order-independent identity of a position snapshot.

        Two snapshots with the same signature bracket a window in which no
        execution changed the symbol's exposure: any fill moves ``size`` /
        ``avgPrice`` and stamps ``updatedTime``, so equality proves the
        reads between them saw a quiescent venue.
        """
        return tuple(sorted(
            (_wire_int(row.get('positionIdx'), field='positionIdx'),
             str(row.get('side') or ''),
             str(row.get('size') or ''),
             str(row.get('avgPrice') or ''),
             str(row.get('updatedTime') or ''))
            for row in rows
        ))

    async def _apply_adoption_baseline(
            self, market: InstrumentInfo, rows: list[dict],
    ) -> list[dict]:
        """Baseline every live row's fill cursor from per-order venue truth.

        The engine's startup ``reconcile`` adopts the venue's NET position
        size UNCONDITIONALLY and deal-independently: the FIRST engine-facing
        ``get_position`` read (routed here via :meth:`_fetch_deriv_position`
        or :meth:`fetch_raw_positions`) folds every pre-restart fill into
        ``BrokerPosition.size``. The private ``execution`` stream and the F4
        reconnect backfill emit fills incrementally against each row's durable
        ``filled_qty`` cursor; a cursor still lagging the venue (a fill that
        reached the venue — and so the adopted size — but whose WS push never
        persisted before the crash) would let that same slice be re-applied
        on top of the already-adopted size (``execId`` dedup lives in the
        in-memory :attr:`_seen_exec_ids`, which restart empties). So, exactly
        once, on the adoption snapshot, seed each live row's fills from the
        venue's own per-order execution history — and emit nothing; adoption
        owns the size. After this barrier only genuinely new fills move a
        cursor.

        Attribution is PER ORDER, never distributed from the aggregate net:
        each live row with a broker ``orderId`` gets its cursor from the
        summed ``execQty`` of its OWN ``/v5/execution/list`` rows (the walk
        in :meth:`_recovery_fill_ids`), and those same ``execId`` values are
        seeded into the de-dup frontier, so cursor and de-dup can never
        disagree. Bybit matches working orders by price / trigger, not by
        creation order, so any heuristic split of the aggregate net across
        rows can hand one order's fill to another; the per-order read is the
        only attribution the venue itself vouches for. This covers exit /
        bracket legs too — their pre-restart fills already reduced the
        adopted net, so their cursors must equally reflect venue truth or a
        later backfill / reconcile pass would re-book (or forever await)
        those slices. The run's own position trading stops that fired while
        the bot was down have no row of their own and no ``orderLinkId``:
        they are read from the symbol's execution history down to the floor
        (:meth:`_unbooked_venue_stop_executions`), netted from the entry rows
        like an unbooked exit fill, and the fired stop is no longer guarded.
        A NON-ENTRY row (exit / bracket / close leg) whose
        seeded cursor covers its full size is CLOSED on commit, mirroring
        the live path's full-fill terminal (``_fill_event``): that terminal
        can never fire for it afterwards — its executions are seeded into
        the de-dup frontier, so neither the PUSH replay nor the F4 backfill
        ever reaches the event builder — and leaving it live would strand
        the row (and its envelope / client id) forever. ENTRY rows stay
        live for the flat sweep, exactly like the live path. Rows in a
        :data:`PENDING_DISPATCH_STATES` state are
        EXEMPT — F1 in-flight recovery left those parked precisely because it
        could not confirm their dispatch, and it owns their resolution. A
        missing ``orderId`` falls back to the row's ``orderLinkId`` in the
        CONCLUSIVE lookup (:meth:`_confirm_lookup` — a transport failure
        raises instead of masquerading as not-found). The lookup ALSO
        decides terminality: a live order ALWAYS appears in
        ``/v5/order/realtime``, so a conclusive not-found proves the order
        is dead (aged out of the < 24h history retention, or it never
        landed) and can never fill again — REGARDLESS of any retained
        stale exchange handle, which is still walked first so the dead
        order's historical executions seed the de-dup frontier and the
        cursor. A dead row is RETIRED at commit (row + intent envelope,
        ``adoption_baseline_unattributable`` audit): neither runtime owner
        could ever conclude it — a handle-less row maps to an empty
        presence set, and the disappearance confirmation maps the same
        not-found to INCONCLUSIVE forever — so leaving it live would
        strand its envelope / spent client id. The one exception: a dead
        ENTRY row carrying fills stays live — its fills are real
        components of the adopted position, and the position-lifecycle
        owners (the positions-namespace tracking and the flat sweep)
        retire it with the position. Both of those owners predicate on a
        FULL fill, so a PARTIALLY filled dead entry has its ``qty``
        normalized down to the terminal filled amount (the residual can
        never fill again) — otherwise the row would be owned by neither.

        The per-order fill recovery targets an earlier process left open
        (:meth:`~_ReconcileMixin._durable_fill_recovery_targets`) are settled
        here too. A target of a row an earlier process CLOSED names an order
        no other path reads again (the restart re-homes only the live rows):
        its executions are walked by ``orderId`` like a live row's, under the
        same order-truth gate against the target's terminal ``cumExecQty``,
        and seeded into the de-dup frontier; a close / exit leg's executions
        its cursor lacks net the entry rows like any other unbooked reduce,
        in one transaction with the target's done record, so a later restart
        never nets them twice. Nothing is emitted: the adopted position holds
        them. A target of a re-homed row is done once the seeded cursor
        covers it, and queued for the per-order recovery otherwise.

        Completeness of the seed (the F8 order-truth gate): a successfully
        paginated ``/v5/execution/list`` walk is NOT proof the venue's
        execution index has caught up with the position — a fill can sit in
        both stable position snapshots while its execution row is not
        indexed yet. So when the conclusive lookup FOUND the order, the
        walked ``execQty`` sum must cover its authoritative ``cumExecQty``
        (same wire domain), or the complete snapshot is re-read within the
        bounded stability pass. Persistent indexing lag raises without
        committing a partial seed. A not-found order skips the gate — it aged
        out of the order history, so its fills are old and long indexed. The walk's end is
        additionally clamped to at least the venue-clock floor, so a
        lagging LOCAL clock can never truncate the seed below the floor.

        Race safety (the adoption floor): the venue clock is read FIRST, the
        per-order walks run after the caller's position snapshot, and a
        SECOND position snapshot must match the first
        (:meth:`_position_rows_signature`) before anything is committed — a
        fill racing any of these reads changes the snapshot and forces
        another pass. On a stable pass every execution at or before the
        adoption is provably seeded (live rows) or belongs to rows whose
        persisted state already accounts it, so the F4 backfill floors its
        watermark at the pre-read venue clock: below it only owned history
        remains, while a post-adoption fill's ``execTime`` necessarily lands
        at or above it (both timestamps are venue-clocked). If the snapshot
        never stabilizes, or any read fails, NOTHING is committed, the guard
        stays unlatched, and the read raises
        :class:`BybitAdoptionBaselineError` (retryable): the engine's
        ONE-SHOT startup adoption must never consume an unbaselined
        snapshot — it would fold every pre-restart fill into the adopted
        size while the cursors still lag, and a LATER successful baseline
        would then seed any post-adoption fill's ``execId`` into the de-dup
        frontier without the engine ever booking it (the periodic reconcile
        deliberately ignores size increases), silently losing the fill.
        Startup fails loud exactly like a failed position read; the
        periodic reconcile parks the cycle and retries; the F4 backfill
        stays deferred behind the guard.

        Wire domain: the store rows' ``qty`` / ``filled_qty`` and the
        execution rows' ``execQty`` are ALL in the wire domain (base units on
        linear, whole USD contracts on inverse), so the cursor seed is
        contract-for-contract with NO anchor conversion.

        :return: The position snapshot the caller must report from — the
            verified re-read of the committed pass (identical content to the
            caller's read).
        :raises BybitAdoptionBaselineError: When any baseline read fails or
            the position snapshot never stabilizes — nothing was committed.
        """
        if self.store_ctx is None or self._adoption_baselined:
            return rows
        try:
            floor_ms = await self._venue_time_ms()
        except BybitError as exc:
            raise BybitAdoptionBaselineError(
                "adoption baseline: venue time read failed",
            ) from exc
        if floor_ms <= 0:
            # A malformed venue-time response must not become a zero floor:
            # the F4 backfill anchors its first watermark on the floor and
            # would walk 7-day windows all the way from the epoch.
            raise BybitAdoptionBaselineError(
                "adoption baseline: venue time read returned no timestamp",
            )
        last_index_lag: str | None = None
        restored = self._durable_fill_recovery_targets()
        for _ in range(_ADOPTION_STABLE_ATTEMPTS):
            seeds: list[tuple] = []
            closed_seeds: list[tuple] = []
            pass_index_lag: str | None = None
            for row in self.store_ctx.iter_live_orders():
                if row.state in PENDING_DISPATCH_STATES or row.state in LEDGER_ROW_STATES:
                    continue
                # The conclusive lookup runs for EVERY walked row: it is the
                # ``orderId`` fallback for a handle-less row, the
                # authoritative order truth (``cumExecQty``) the execution
                # seed must cover before the baseline may commit, and the
                # TERMINALITY signal — a conclusive not-found proves the
                # order is dead regardless of any retained stale handle.
                lookup, conclusive = await self._confirm_lookup(
                    market, row.client_order_id,
                )
                if not conclusive:
                    raise BybitAdoptionBaselineError(
                        "adoption baseline: orderLinkId lookup failed "
                        f"for {row.client_order_id}",
                    )
                order_id = row.exchange_order_id \
                    or str((lookup or {}).get('orderId') or '')
                ids: set[str] = set()
                qty_sum = 0.0
                executions: list[dict] = []
                if order_id:
                    # A retained handle is still walked even for a dead
                    # order — its historical executions must seed the
                    # de-dup frontier and the cursor before any terminal
                    # decision is applied.
                    from_ms = row.created_ts_ms - EXECUTION_SINCE_SKEW_MS
                    ids, qty_sum, seeded, executions = await self._recovery_fill_ids(
                        market, order_id, from_ms, until_ms=floor_ms,
                    )
                    if not seeded:
                        raise BybitAdoptionBaselineError(
                            "adoption baseline: execution seed read failed "
                            f"for order {order_id}",
                        )
                    parked_ids, parked_qty, parked = self._parked_adoption_executions(
                        market,
                        order_id,
                        floor_ms,
                        ids,
                    )
                    ids.update(parked_ids)
                    qty_sum += parked_qty
                    executions = [*executions, *parked]
                if lookup is not None:
                    # F8 order-truth gate: the position snapshot can reflect
                    # a fill whose execution row is not indexed yet — a
                    # clean-but-incomplete walk must not commit an
                    # understated cursor / de-dup seed. ``cumExecQty`` and
                    # the walked ``execQty`` sum share the wire domain.
                    cum_exec = _wire_float(
                        lookup.get('cumExecQty'), field='cumExecQty',
                    )
                    if qty_sum < cum_exec - _FILL_EPS:
                        pass_index_lag = (
                            f"order {order_id} ({qty_sum} < {cum_exec})"
                        )
                        last_index_lag = pass_index_lag
                seeds.append((row, order_id, ids, qty_sum, lookup is None, executions))
            for coid, (order_id, cum_exec) in restored.items():
                # A recovery target of a row an earlier process closed: the
                # restart re-homed only the live rows, so no other path reads
                # this order again. Its executions all ran before the
                # restart (the order had ended), so the adopted position
                # holds them; walk them like a live row's, under the same
                # order-truth gate (the target is the venue's terminal
                # ``cumExecQty``).
                closed = self.store_ctx.get_order_for_run_id(coid)
                if closed is None or closed.closed_ts_ms is None:
                    continue
                from_ms = closed.created_ts_ms - EXECUTION_SINCE_SKEW_MS
                ids, qty_sum, seeded, executions = await self._recovery_fill_ids(
                    market, order_id, from_ms, until_ms=floor_ms,
                )
                if not seeded:
                    raise BybitAdoptionBaselineError(
                        "adoption baseline: execution seed read failed "
                        f"for closed order {order_id}",
                    )
                parked_ids, parked_qty, parked = self._parked_adoption_executions(
                    market, order_id, floor_ms, ids,
                )
                ids.update(parked_ids)
                qty_sum += parked_qty
                executions = [*executions, *parked]
                if qty_sum < float(cum_exec) - _FILL_EPS:
                    pass_index_lag = f"closed order {order_id} ({qty_sum} < {cum_exec})"
                    last_index_lag = pass_index_lag
                closed_seeds.append((closed, order_id, ids, qty_sum, executions, cum_exec))
            # The run's own trading stops that fired while the bot was down:
            # no row's order walk reads them.
            stop_executions = await self._unbooked_venue_stop_executions(market, floor_ms)
            closeouts = await self._unbooked_venue_closeouts(market, floor_ms)
            try:
                verify = await self._fetch_position_rows(market)
            except BybitError as exc:
                raise BybitAdoptionBaselineError(
                    "adoption baseline: verify snapshot read failed",
                ) from exc
            self._ingest_position_sizes(verify)
            if (self._position_rows_signature(verify)
                    != self._position_rows_signature(rows)):
                # A fill raced the reads — the walks and the caller snapshot
                # may disagree. Re-run against the fresh snapshot.
                rows = verify
                continue
            if pass_index_lag is not None:
                # Order truth can lead the execution index briefly after a
                # fill. Re-run the complete bounded snapshot instead of
                # failing the first startup read; no partial seed is committed.
                rows = verify
                continue
            # Every write of the pass commits as one: credits taken, entry
            # rows netted and recovery targets marked done never land without
            # each other, and the latch, the de-dup frontier and the queue
            # change only once they have. A failed write leaves the baseline
            # unlatched for the retry, against the journal as it was.
            seen: set[str] = set()
            requeue: dict[str, tuple[str, Decimal]] = {}
            inherited: list[tuple[str, list[dict]]] = []
            with self._journal_transaction():
                unbooked_reduces: list[tuple[str, float, list[dict]]] = []
                for row, order_id, ids, qty_sum, dead, executions in seeds:
                    seen.update(ids)
                    # ``qty_sum`` is the order's venue-domain cumulative; the
                    # row's cursor is net of what reducing fills consumed.
                    netted = venue_filled_qty(row) - float(row.filled_qty)
                    cumulative: float = min(
                        float(row.qty), max(float(row.filled_qty), qty_sum - netted),
                    )
                    if ((row.extras or {}).get('kind') in ('exit_leg', 'close')
                            and cumulative > row.filled_qty + _FILL_EPS):
                        unbooked_reduces.append(
                            (row.side, cumulative - float(row.filled_qty), executions))
                    if cumulative > row.filled_qty:
                        self.store_ctx.set_filled(row.client_order_id, cumulative)
                        self.store_ctx.log_event(
                            'adoption_baseline_applied',
                            client_order_id=row.client_order_id,
                            exchange_order_id=order_id,
                            intent_key=row.intent_key,
                            payload={'from_filled_qty': row.filled_qty,
                                     'filled_qty': cumulative,
                                     'exec_count': len(ids)},
                        )
                    is_entry = ((row.extras or {}).get('kind')
                                in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING))
                    if is_entry and executions:
                        inherited.append((row.client_order_id, executions))
                    if dead and is_entry and cumulative > _FILL_EPS:
                        # Conclusively dead ENTRY carrying fills: the fills are
                        # a real component of the adopted position, so the row
                        # stays live and the position lifecycle
                        # (positions-namespace tracking + flat sweep) owns its
                        # retirement. BOTH owners predicate on a FULL fill, so
                        # a partially filled dead entry must have its qty
                        # normalized down to the terminal filled amount — the
                        # residual can never fill again (the order is dead) and
                        # an un-normalized row would be owned by neither
                        # (disappearance maps its not-found to INCONCLUSIVE
                        # forever).
                        if cumulative < row.qty - _FILL_EPS:
                            self.store_ctx.upsert_order(
                                row.client_order_id, qty=cumulative,
                            )
                        self.store_ctx.log_event(
                            'adoption_baseline_unattributable',
                            client_order_id=row.client_order_id,
                            intent_key=row.intent_key,
                            payload={'state': row.state, 'kept': True,
                                     'qty': cumulative, 'from_qty': row.qty},
                        )
                        continue
                    if dead:
                        # Conclusively dead with no kept-alive exemption (a
                        # zero-fill row, or a non-entry leg whose residual can
                        # never fill again): retire the row AND its intent
                        # envelope. Its walked executions are already in the
                        # de-dup frontier, so nothing re-books them; without
                        # ``record_complete`` the persisted envelope would
                        # replay on the first sync and hand a re-emitted intent
                        # the same spent ``client_order_id``.
                        self.store_ctx.record_unpark(row.client_order_id)
                        self.store_ctx.close_order(row.client_order_id)
                        if row.intent_key:
                            self.store_ctx.record_complete(row.intent_key)
                        self.store_ctx.log_event(
                            'adoption_baseline_unattributable',
                            client_order_id=row.client_order_id,
                            intent_key=row.intent_key,
                            payload={'state': row.state, 'retired': True},
                        )
                        continue
                    if ((row.extras or {}).get('kind') in ('exit_leg', 'close')
                            and cumulative >= row.qty - _FILL_EPS):
                        # Fully covered non-entry leg: the live full-fill
                        # terminal (``_fill_event``) can never fire for it —
                        # every one of its executions is in the de-dup
                        # frontier — so apply the FULL terminal here (unpark +
                        # close + intent completion; ENTRY rows stay live for
                        # the flat sweep). ``record_complete`` matters: on the
                        # live path the engine retires the intent envelope when
                        # the fill event reaches it (``_drop_envelope``), but
                        # the baseline suppresses that event — without the
                        # completion the persisted envelope would replay on the
                        # first sync and hand a re-emitted intent the same
                        # spent ``client_order_id``. The sibling / OCA legs of
                        # this exit still carry their exchange handles, so the
                        # runtime disappearance pass owns their retirement.
                        self.store_ctx.record_unpark(row.client_order_id)
                        self.store_ctx.close_order(row.client_order_id)
                        if row.intent_key:
                            self.store_ctx.record_complete(row.intent_key)
                        self.store_ctx.log_event(
                            'adoption_baseline_closed',
                            client_order_id=row.client_order_id,
                            exchange_order_id=order_id,
                            intent_key=row.intent_key,
                            payload={'filled_qty': cumulative,
                                     'kind': (row.extras or {}).get('kind')},
                        )
                # Position in ``unbooked_reduces`` -> the closed row's target its
                # netting settles; the done record is written with the netting.
                closed_reduces: dict[int, tuple[str, str, Decimal]] = {}
                for closed, order_id, ids, qty_sum, executions, cum_exec in closed_seeds:
                    seen.update(ids)
                    netted = venue_filled_qty(closed) - float(closed.filled_qty)
                    cumulative = min(
                        float(closed.qty), max(float(closed.filled_qty), qty_sum - netted),
                    )
                    unbooked = cumulative - float(closed.filled_qty)
                    if ((closed.extras or {}).get('kind') in ('exit_leg', 'close')
                            and unbooked > _FILL_EPS):
                        # The reduce never reached the journal: it consumed
                        # entry exposure the entry rows still own.
                        closed_reduces[len(unbooked_reduces)] = (
                            closed.client_order_id, order_id, cum_exec)
                        unbooked_reduces.append((closed.side, unbooked, executions))
                        continue
                    self._journal_fill_recovery_done(
                        closed.client_order_id, order_id, cum_exec,
                        source='adoption_baseline', netted_qty=0.0,
                    )
                order_reduces = len(unbooked_reduces)
                for entry in stop_executions:
                    seen.add(str(entry.get('execId') or ''))
                    unbooked_reduces.append((str(entry.get('side') or '').lower(),
                                             float(entry.get('execQty') or 0.0), [entry]))
                tolerance = market.qty_step / 2 if market.qty_step > 0 else _FILL_EPS
                credited = self._take_persisted_ledger_credits(unbooked_reduces, tolerance)
                for position, (side, quantity) in enumerate(credited):
                    # A reducing fill the run never booked (it landed while the
                    # bot was down) consumed entry exposure exactly like a booked
                    # one: net it out of the entry rows before the venue clamp,
                    # which only sees the excess over the whole venue leg and
                    # misses it when foreign exposure shares the leg. What an
                    # earlier process's position ledger already netted for it is
                    # not netted again.
                    if position < order_reduces:
                        settled = closed_reduces.get(position)
                        if settled is None:
                            self._reduce_entry_ownership(side, quantity)
                            continue
                        # The done record, in the same commit, keeps a later
                        # restart from netting the same reduce again.
                        self._reduce_entry_ownership(side, quantity)
                        self._journal_fill_recovery_done(
                            *settled, source='adoption_baseline', netted_qty=quantity,
                        )
                        continue
                    _, executed, (entry,) = unbooked_reduces[position]
                    owned = self._entry_side_exposure(side)
                    self._reduce_entry_ownership(side, quantity)
                    self._log_adopted_venue_stop(
                        entry, side,
                        claim=owned - self._entry_side_exposure(side) + executed - quantity,
                    )
                for entry in closeouts:
                    # The venue closed the run's side while the bot was down; the
                    # execution does not say how much of it was the run's. The
                    # run's share on that side is taken as gone — someone else
                    # who holds the leg now keeps theirs.
                    side = str(entry.get('side') or '').lower()
                    seen.add(str(entry.get('execId') or ''))
                    owned = self._entry_side_exposure(side)
                    self._reduce_entry_ownership(side, owned)
                    self._log_venue_closeout_while_down(entry, side, retired=owned)
                self._clamp_entry_ownership_to_venue(verify)
                for coid, target in restored.items():
                    # A target of a row this instance re-homed: the walk above
                    # seeded its cursor and de-dup frontier from the same
                    # per-order truth. A cursor still short of the target goes
                    # back to the per-order recovery, whose reads now meet the
                    # seeded frontier; a pending-dispatch or ledger row the walk
                    # exempted keeps its durable target for its own resolution.
                    live = self.store_ctx.get_order(coid)
                    if live is None:
                        continue
                    if venue_filled_qty(live) >= float(target[1]) - _FILL_EPS:
                        self._journal_fill_recovery_done(
                            coid, *target, source='adoption_baseline')
                    elif (live.state not in PENDING_DISPATCH_STATES
                          and live.state not in LEDGER_ROW_STATES):
                        requeue[coid] = target
            self._adoption_baselined = True
            self._deriv_exec_floor_ms = floor_ms
            self._seen_exec_ids.update(seen)
            self._unbooked_fill_orders.update(requeue)
            self._queue_inherited_entry_fills(inherited, market)
            self._queue_inherited_hold_closes(market)
            for side in {str(entry.get('side') or '').lower()
                         for entry in (*stop_executions, *closeouts)}:
                # The venue clears a fired stop with the position it closed.
                self._forget_trading_stop_if_flat(side)
            # The journal holds the run's share as of the verified snapshot:
            # the position ledgers anchor on that very view.
            self._note_share_held_since(floor_ms, market)
            self._ledger_advance(market)
            return verify
        if last_index_lag is not None:
            raise BybitAdoptionBaselineError(
                "adoption baseline: execution index remained behind order "
                f"truth across {_ADOPTION_STABLE_ATTEMPTS} attempts: "
                f"{last_index_lag}",
            )
        raise BybitAdoptionBaselineError(
            "adoption baseline: position snapshot kept changing across "
            f"{_ADOPTION_STABLE_ATTEMPTS} attempts",
        )

    def _entry_side_exposure(self, side: str) -> float:
        """The owned exposure of the live entry rows a reduce on ``side`` nets."""
        assert self.store_ctx is not None
        entry_side = 'buy' if side == 'sell' else 'sell'
        return sum(
            float(row.filled_qty) for row in self.store_ctx.iter_live_orders()
            if row.side == entry_side
            and (row.extras or {}).get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
            and row.filled_qty > _FILL_EPS
        )

    def _queue_inherited_entry_fills(
            self, inherited: list[tuple[str, list[dict]]], market: InstrumentInfo,
    ) -> None:
        """Queue the entry fills the baseline seeded and the engine never took.

        The baseline put every walked execution of a live entry row into the
        de-dup frontier, so no stream or backfill path delivers it again. An
        execution whose fill the engine did not take
        (:meth:`~pynecore.core.broker.storage.RunContext.fill_delivered` is
        ``False``: the earlier process journaled it, and the engine had not
        booked it when that process ended) is delivered once by the next
        backfill pass, flagged ``journaled_before_restart``
        (:meth:`~_BybitBase._inherited_fill_event`): the engine records its
        receipt and books its fee onto the entry's open trades. The fee of an
        exit or close fill has no such channel: the engine does not recover
        the earlier process's trade it belonged to.

        :param inherited: Per live entry row: its client order id and walked
            execution rows.
        :param market: The chart instrument.
        """
        if self.store_ctx is None or market.category == CATEGORY_SPOT:
            return
        for coid, executions in inherited:
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                continue
            for entry in sorted(executions, key=execution_sort_key):
                fill_id = str(entry.get('execId') or '')
                if not fill_id or self.store_ctx.fill_delivered(fill_id):
                    continue
                event = self._inherited_fill_event(row, entry, market)
                if event is not None:
                    self._inherited_fill_events.append(event)

    def _log_venue_closeout_while_down(self, entry: dict, side: str, *, retired: float) -> None:
        """Journal one venue close of the run's side the adoption baseline retired.

        Logged ERROR: the run's share is assumed, not measured. The audit's
        ``exec_id`` keeps a later restart from retiring a newer share for the
        same execution (:meth:`_unbooked_venue_closeouts`).
        """
        assert self.store_ctx is not None
        logger.error(
            "Bybit adoption baseline: the venue closed the bot's %s side while the bot "
            "was down (%s %s, execution %s, %s): the run's share there (%s) is taken "
            "as closed — check the venue history",
            'long' if side == 'sell' else 'short', entry.get('execType') or 'Trade',
            entry.get('createType') or '-', entry.get('execId'), entry.get('execQty'),
            retired,
        )
        self.store_ctx.log_event(
            'venue_closeout_while_down',
            exchange_order_id=str(entry.get('orderId') or '') or None,
            payload={'exec_id': str(entry.get('execId') or ''),
                     'exec_type': str(entry.get('execType') or 'Trade'),
                     'create_type': str(entry.get('createType') or ''),
                     'side': side, 'exec_qty': entry.get('execQty'),
                     'exec_time': entry.get('execTime'), 'retired_qty': retired},
        )

    def _log_adopted_venue_stop(self, entry: dict, side: str, *, claim: float) -> None:
        """Journal one own trading stop the adoption baseline booked as a reduce.

        Recorded as the live attribution would record it, on the newest
        live ownership row of the stop's side, so a later execution of the
        same stop order and a later restart see the claim
        (:meth:`_venue_stop_claimed`, :meth:`_unbooked_venue_stop_executions`).
        """
        assert self.store_ctx is not None
        owners = sorted(
            (row for row in iter_active_bracket_ownerships(self.store_ctx) if row.side == side),
            key=lambda row: row.created_ts_ms,
        )
        owner = owners[-1] if owners else None
        logger.warning(
            "Bybit adoption baseline: own %s %s (%s %s) fired while the bot was "
            "down — netting %s of the run's share",
            entry.get('stopOrderType'), entry.get('execId'), entry.get('side'),
            entry.get('execQty'), claim,
        )
        self.store_ctx.log_event(
            'venue_bracket_fill_attributed',
            client_order_id=owner.client_order_id if owner is not None else None,
            exchange_order_id=str(entry.get('orderId') or '') or None,
            payload={
                'exec_id': str(entry.get('execId') or ''),
                'stop_order_type': str(entry.get('stopOrderType') or ''),
                'exit_id': owner.pine_entry_id if owner is not None else None,
                'from_entry': owner.from_entry if owner is not None else None,
                'claim_qty': claim,
                'split': False,
                'source': 'adoption_baseline',
            },
        )

    def _clamp_entry_ownership_to_venue(self, rows: list[dict]) -> None:
        """Clamp the live entry rows' owned exposure to the venue position.

        The per-order baseline can only RAISE a cursor: an entry's own
        executions are venue truth for what it FILLED, not for what the
        run still OWNS. Closes net the entry rows at fill time
        (``_reduce_entry_ownership``), but a close that filled while the
        bot was down — or a row inherited from a run that predates the
        fill-time netting — leaves the durable cursors above the venue's
        live size, and the cycle-end book then owns closes the venue
        already executed (measured on the inverse lane, 2026-08-21:
        adopted book 305 contracts vs venue 1, zero trades in the cycle).
        The verified snapshot at this barrier is stable and authoritative,
        so any per-side excess of the summed entry cursors over the venue
        size is consumed FIFO, mirroring the fill-time netting. The venue
        exceeding the rows is NOT raised here — untracked-exposure
        adoption owns that surplus. Wire domain on both sides; no anchor
        conversion. Rows in a pending-dispatch state stay untouched —
        F1 recovery owns their resolution and their cursors are zero.

        Hedge legs are separate positions: each side is clamped to its own
        leg. A one-way account holds ONE signed position, and a folded
        reversal is booked as a single entry row on the new side whose
        closing part is never netted from the old side's rows: the rows of
        the two sides are folded against each other first, and the run's
        net is clamped to the venue's signed position. Clamped side by side,
        the reversal's closing part would be netted twice — once off the old
        side, once off the new — and foreign exposure on the venue's side
        would land in the run's share. The fold cannot tell a reversal from
        rows of both sides that predate the fill-time netting; it never owns
        more than the side-by-side clamp would.
        """
        if self.store_ctx is None:
            return
        venue: dict[str, float] = {'buy': 0.0, 'sell': 0.0}
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            if side in venue:
                venue[side] += size
        if self._position_mode != POSITION_MODE_HEDGE:
            net = venue['buy'] - venue['sell']
            venue = {'buy': max(net, 0.0), 'sell': max(-net, 0.0)}
            folded = min(sum(row.filled_qty for row in self._clampable_entry_rows(side))
                         for side in ('buy', 'sell'))
            if folded > _FILL_EPS:
                for side in ('buy', 'sell'):
                    self._net_entry_rows_fifo(self._clampable_entry_rows(side),
                                              folded, 'adoption_baseline_folded')
        for side in ('buy', 'sell'):
            live = self._clampable_entry_rows(side)
            excess = sum(row.filled_qty for row in live) - venue[side]
            if excess <= _FILL_EPS:
                continue
            logger.warning(
                "Bybit adoption baseline: %s entry rows own %s beyond the "
                "venue position — netting the excess (a close landed while "
                "the bot was down, or the rows predate fill-time netting)",
                side, excess,
            )
            self._net_entry_rows_fifo(live, excess, 'adoption_baseline_clamped')

    def _clampable_entry_rows(self, side: str) -> list['OrderRow']:
        """The settled entry rows of ``side`` that own exposure, oldest first."""
        assert self.store_ctx is not None
        return sorted(
            (
                row for row in self.store_ctx.iter_live_orders()
                if row.side == side
                and row.state not in PENDING_DISPATCH_STATES
                and (row.extras or {}).get('kind')
                in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                and row.filled_qty > _FILL_EPS
            ),
            key=lambda entry_row: entry_row.created_ts_ms,
        )

    def _net_entry_rows_fifo(
            self, rows: list['OrderRow'], quantity: float, kind: str,
    ) -> None:
        """Net ``quantity`` owned exposure out of ``rows`` in order, journalling each.

        Exactly the fill-time netting: only the OWNED (filled) exposure is
        consumed, a working entry keeps its unfilled remainder (it can still
        fill on the venue), and a row left with neither retires only the
        physical row.

        :param rows: Entry rows of one side, oldest first.
        :param quantity: The exposure to net (wire domain).
        :param kind: The audit event kind logged per row.
        """
        assert self.store_ctx is not None
        remaining = quantity
        for row in rows:
            if remaining <= _FILL_EPS:
                break
            consumed = min(remaining, row.filled_qty)
            self.store_ctx.log_event(
                kind,
                client_order_id=row.client_order_id,
                intent_key=row.intent_key,
                payload={'from_filled_qty': row.filled_qty,
                         'filled_qty': row.filled_qty - consumed,
                         'consumed': consumed},
            )
            self._net_entry_row(row, consumed)
            remaining -= consumed

    # --- inverse net-position mirror --------------------------------------------

    def _inverse_seed_net(self, rows: list[dict]) -> None:
        """Seed the inverse mirror from the venue's position rows.

        An adopted position anchors at its average entry price — the same
        conversion :meth:`_fetch_deriv_position` reports to the core, so
        a core full-close of the adopted base lands exactly back on the
        venue's contract count.
        """
        contracts = 0.0
        base = 0.0
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            avg = _wire_float(row.get('avgPrice'), field='avgPrice')
            if side not in ('buy', 'sell') or avg <= 0.0:
                continue
            sign = 1.0 if side == 'buy' else -1.0
            contracts += sign * size
            base += sign * contracts_to_base(size, avg)
        self._inverse_net_contracts = contracts
        self._inverse_net_base = base

    def _inverse_core_size(self, side: str, contracts: float, avg_price: float) -> float:
        """Report an inverse position in the base the core was told it holds.

        While the mirror holds the venue's exact contract count, its base is
        the sum of the fills the core booked, so the report matches the book
        exactly. Converting at the venue's average price instead differs from
        it by a fraction of a contract (each fill converts at its own anchor),
        which the core cannot tell from a manual reduction (measured live:
        bybit-inverse cycle 226, 3.38e-09 BTC on a 165-contract short adopted
        as an external close). A venue holding the mirror's contracts and
        more on the same side (someone else added to the shared position)
        reports the booked base plus the extra contracts at the average
        price: the core then sees its own exposure intact, never a phantom
        reduction. Fewer contracts, or the other side, is a real change and
        converts at the average price.
        """
        signed = contracts if side == 'buy' else -contracts
        mirror = self._inverse_net_contracts
        if signed == mirror:
            return abs(self._inverse_net_base)
        if signed * mirror > 0.0 and abs(signed) > abs(mirror):
            return (abs(self._inverse_net_base)
                    + contracts_to_base(abs(signed) - abs(mirror), avg_price))
        return contracts_to_base(contracts, avg_price)

    def _apply_inverse_fill(self, side: str, contracts: float, base: float) -> None:
        """Fold one own fill into the inverse net-position mirror.

        Contract counts are whole numbers, so their float sum is exact —
        when it reaches zero the base side (which does accumulate float
        noise across the division per fill) snaps to exactly flat.
        """
        sign = 1.0 if side == 'buy' else -1.0
        self._inverse_net_contracts += sign * contracts
        self._inverse_net_base += sign * base
        if self._inverse_net_contracts == 0.0:
            self._inverse_net_base = 0.0

    def _net_inverse_mirror(self, side: str, contracts: float) -> None:
        """Take ``contracts`` of the ``side`` exposure off the inverse mirror.

        The mirror folds the run's own fills; the run's contracts can also
        leave the leg without one: the position ledger books a foreign or
        venue close of the run's share, or a clamp to the venue view, by
        netting the journal (:meth:`_net_side_share`). The mirror follows the
        journal there, or it keeps contracts the venue no longer holds for
        the run: the next position's core-facing size then converts at the
        venue average instead of the base its fills booked (a phantom
        reduction the core adopts), and a core full close no longer snaps
        onto the venue's contract count (a base residue). The base moves pro
        rata, so the contracts left keep the average conversion the core was
        told. A mirror holding nothing has no conversion to keep and stays as
        it is.

        :param side: The side whose exposure is taken off (``'buy'`` moves
            the signed mirror down, ``'sell'`` up).
        :param contracts: Wire-domain contracts.
        """
        market = self._market
        if market is None or not market.is_inverse or contracts <= 0.0:
            return
        held = self._inverse_net_contracts
        if held == 0.0:
            return
        shifted = held + (contracts if side == 'sell' else -contracts)
        self._inverse_net_base *= shifted / held
        self._inverse_net_contracts = shifted
        if shifted == 0.0:
            self._inverse_net_base = 0.0

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
        ``createdTime`` so the FIFO order is replay-stable. A hedge leg
        aggregates every entry of its side, so no leg carries an
        ``entry_id``: each exit of the side protects it, and the core merges
        their levels into the one bracket the leg holds.

        A leg's ``qty`` is the run's own share of the venue position
        (:meth:`_scope_hedge_rows_to_run`). When the venue position is larger
        — it also holds exposure this run does not own — its size is reported
        as ``venue_qty``: the trading stop is an attribute of the whole
        position, so the core then keeps it off the leg and guards the run's
        share in software.

        A failed venue read raises the broker taxonomy
        (:func:`~pynecore_bybit.exceptions.classify_read_error`), never a raw
        plugin error: the core emulator calls this as the first step of a
        hedge dispatch, where the taxonomy's connection error parks the
        dispatch for a plain retry; an untranslated fault is parked as a
        write of unknown outcome under an entry block instead.
        """
        try:
            return await self._fetch_raw_position_legs(symbol)
        except BybitError as e:
            raise classify_read_error(e) from e

    async def _fetch_raw_position_legs(self, symbol: str) -> list[PositionLeg]:
        """Read and parse the hedge legs of ``symbol`` (untranslated errors)."""
        market = await asyncio.to_thread(self._broker_market)
        if symbol not in (self.symbol, market.symbol):
            return []
        rows = await self._fetch_position_rows(market)
        self._ingest_position_sizes(rows)
        # The baseline may report a newer, verified snapshot than the first
        # read: the venue size comes from the same rows the run's share does.
        rows = await self._apply_adoption_baseline(market, rows)
        venue_sizes = {
            self._position_row_index(row): self._position_row_size(row) for row in rows
        }
        rows = self._scope_hedge_rows_to_run(rows)
        legs: list[PositionLeg] = []
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            if side not in ('buy', 'sell'):
                continue
            venue_size = venue_sizes.get(self._position_row_index(row), size)
            legs.append(PositionLeg(
                leg_id=str(self._position_row_index(row)),
                symbol=symbol,
                side=side,
                qty=size,
                entry_price=_wire_float(row.get('avgPrice'), field='avgPrice'),
                open_time=_wire_float(
                    row.get('createdTime'), field='createdTime',
                ) / 1000.0,
                unrealized_pnl=_wire_float(
                    row.get('unrealisedPnl'), field='unrealisedPnl',
                ),
                venue_qty=venue_size if venue_size > size + _FILL_EPS else None,
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

        The leg gets a persist-first close row under the wire id, like a
        one-way close (:meth:`execute_close`): the disappearance pass tracks
        it and the per-order fill recovery books a fill the stream dropped
        and the execution list indexed past the backfill's read overlap. The
        row keeps ``coid`` itself
        (:data:`~pynecore_bybit.helpers.CORE_CLIENT_ORDER_ID_EXTRA`): every
        order and fill event of the leg reports it as its
        ``client_order_id``, so the core tells a netting close's fill from a
        strategy close's on every path, after a restart too.
        """
        market = await asyncio.to_thread(self._broker_market)
        idx = int(leg_id)
        qty = Decimal(volume) * Decimal(market.qty_step_str)
        side = 'Sell' if idx == HEDGE_IDX_BUY else 'Buy'
        link_id = wire_link_id(coid)
        self._record_identity(link_id, pine_id=None, from_entry=None,
                              leg_type=LegType.CLOSE, qty=float(qty))
        # Measured live (bybit cycle 335): a reversal's close leg filled while
        # the private stream was down and the execution list had not indexed
        # it a minute later; with no row of its own nothing proved the fill,
        # and the flat venue was cleared as an unexplained external close.
        emulator_row = (self.store_ctx.get_order(close_leg_row_client_order_id(coid))
                        if self.store_ctx is not None else None)
        extras: dict = {'kind': 'close', CORE_CLIENT_ORDER_ID_EXTRA: coid}
        self._persist_leg_row(
            link_id, market=market, side=side.lower(), qty=qty,
            intent_key=emulator_row.intent_key if emulator_row is not None else None,
            extras=extras,
        )
        try:
            result = await self._order_post('/v5/order/create', {
                'category': market.category,
                'symbol': market.symbol,
                'side': side,
                'orderType': 'Market',
                'qty': format_decimal(qty),
                'orderLinkId': link_id,
                'reduceOnly': True,
                'positionIdx': idx,
            }, coid=link_id, context="close leg")
        except Exception as exc:
            self._mark_dispatch_failure(link_id, exc)
            raise
        self._confirm_row(link_id, str(result.get('orderId') or ''), extras)

    async def reject_out_of_range(
            self, envelope: DispatchEnvelope, qty: float,
    ) -> None:
        """Raise the volume-bounds skip (the order is declined) when ``qty`` is out of range."""
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
            qty: float,
            coid: str,
    ) -> None:
        """Write the whole bracket of ONE hedge leg.

        ``POST /v5/position/trading-stop`` sets the position-attribute
        TP / SL / trailing of the addressed ``positionIdx``. Every write sends
        all three fields and Bybit clears a field on the literal ``"0"``, so
        the write is a full replacement: a level absent from the merged
        protection the core sends is removed, and an all-``None`` amend wipes
        the bracket wholesale. ``side`` is unused — Bybit needs no anchor seed
        for a trailing distance (``trailingStop`` activates immediately
        without an ``activePrice``). A leg that vanished between the
        emulator's fetch and this amend rejects with the measured
        zero-position response, an idempotent re-amend with "not modified"
        — both benign no-ops (see
        :func:`~pynecore_bybit.exceptions.is_benign_trading_stop_reject`).

        ``qty`` is unused: the trading stop is written ``tpslMode=Full`` and
        covers the whole venue position, which is all the core ever asks of
        this port (``sized_leg_bracket`` is declared ``SOFTWARE``, so a leg
        that needs less protected never reaches this call).

        A transport or server-side failure leaves the outcome unknown, for an
        attach as for a clear: the request may have reached the venue and may
        still take effect. It raises :class:`OrderDispositionUnknownError`;
        the emulator then writes the leg nothing else until
        :meth:`bracket_matches` tells it what the leg holds, and repeats the
        write when the leg does not show it.
        """
        del side, qty  # Bybit derives the protective side from the leg itself.
        market = await asyncio.to_thread(self._broker_market)
        idx = int(leg_id)
        tp = (round_price(tp_price, market.tick_size_str)
              if tp_price is not None else None)
        sl = (round_price(sl_price, market.tick_size_str)
              if sl_price is not None else None)
        trail = (round_price(trail_offset, market.tick_size_str)
                 if trail_offset is not None else None)
        body: dict = {
            'category': market.category,
            'symbol': market.symbol,
            'positionIdx': idx,
            'tpslMode': 'Full',
            'takeProfit': format_decimal(tp) if tp is not None else '0',
            'stopLoss': format_decimal(sl) if sl is not None else '0',
            'trailingStop': format_decimal(trail) if trail is not None else '0',
        }
        key = trading_stop_key(idx)
        levels = trading_stop_levels(
            float(tp) if tp is not None else None,
            float(sl) if sl is not None else None,
            float(trail) if trail is not None else None,
        )
        cleared = levels == ProtectionLevels()
        row_coid, _retiring = self._trading_stop_write_owner(leg_id, coid)
        write = self._expect_protection(key, levels, row_coid=row_coid)
        try:
            await self._call('/v5/position/trading-stop', method='post',
                             body=body, auth=True)
        except BybitAPIError as e:
            if e.ret_code == 34040:
                # "Not modified": the leg already carries these levels.
                self._settle_protection(key, levels, write=write)
            elif e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                self._settle_protection(key, None, write=write)
            else:
                self._settle_protection(key, None, rejected=levels, write=write)
            if is_benign_trading_stop_reject(e):
                if e.ret_code != 34040:
                    # The leg is flat: nothing left to guard.
                    self._forget_protection(key)
                elif cleared:
                    self._guard_cleared_trading_stop(key, idx)
                return
            if e.ret_code in AMBIGUOUS_DISPOSITION_CODES:
                raise OrderDispositionUnknownError(
                    f"Bybit trading-stop write on leg {leg_id}: server-side "
                    f"failure (retCode={e.ret_code}); disposition unknown",
                    client_order_id=coid, cause=e,
                ) from e
            mapped = map_broker_error(e)
            if mapped is not None:
                raise mapped from e
            raise reject_error(e) from e
        except BaseException as e:
            self._settle_protection(key, None, write=write)
            if isinstance(e, BybitError):
                raise OrderDispositionUnknownError(
                    f"Bybit trading-stop write on leg {leg_id}: transport "
                    f"failure; disposition unknown",
                    client_order_id=coid, cause=e,
                ) from e
            raise
        self._settle_protection(key, levels, write=write)
        if cleared:
            self._guard_cleared_trading_stop(key, idx)

    async def bracket_matches(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            qty: float,
    ) -> bool | None:
        """Whether hedge leg ``leg_id`` holds exactly the trading stop this amend writes.

        Reads the position row from the venue and compares its ``takeProfit``,
        ``stopLoss`` and ``trailingStop`` with the amend's levels after the
        same tick rounding :meth:`amend_bracket` applies. A trailing amend
        whose take-profit and distance the row holds, but whose ``stopLoss``
        differs, cannot be told apart: Bybit may have trailed the stop since
        the write, or the write may never have landed over an earlier stop
        with the same distance. That case answers ``None``; a differing
        take-profit or distance is ``False``. A flat or missing row holds
        nothing and matches no write.

        A failed read raises the broker taxonomy, like
        :meth:`fetch_raw_positions`.
        """
        del side, qty
        idx = int(leg_id)
        try:
            market = await asyncio.to_thread(self._broker_market)
            if symbol not in (self.symbol, market.symbol):
                return False
            rows = await self._fetch_position_rows(market)
            row = next(
                (row for row in rows
                 if self._position_row_index(row) == idx
                 and self._position_row_size(row) > 0.0),
                None,
            )
            if row is None:
                return False
            held = trading_stop_levels(
                _wire_float(row.get('takeProfit'), field='takeProfit') or None,
                _wire_float(row.get('stopLoss'), field='stopLoss') or None,
                _wire_float(row.get('trailingStop'), field='trailingStop') or None,
            )
        except BybitError as e:
            raise classify_read_error(e) from e

        def rounded(level: float | None) -> float | None:
            return (float(round_price(level, market.tick_size_str))
                    if level is not None else None)

        wanted = trading_stop_levels(
            rounded(tp_price), rounded(sl_price), rounded(trail_offset),
        )
        tolerance = float(market.tick_size_str) / 2.0
        if wanted.trailing:
            if not (held.trailing
                    and _same_level(held.limit, wanted.limit, tolerance)
                    and _same_level(held.trail_distance, wanted.trail_distance, tolerance)):
                return False
            return True if _same_level(held.stop, wanted.stop, tolerance) else None
        return (not held.trailing
                and _same_level(held.limit, wanted.limit, tolerance)
                and _same_level(held.stop, wanted.stop, tolerance))


def _same_level(held: float | None, wanted: float | None, tolerance: float) -> bool:
    """Whether two optional price levels are both absent or within ``tolerance``."""
    if held is None or wanted is None:
        return held is wanted
    return abs(held - wanted) <= tolerance

