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
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    PENDING_DISPATCH_STATES,
)
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _BybitBase
from .exceptions import (
    AMBIGUOUS_DISPOSITION_CODES,
    BybitAdoptionBaselineError,
    BybitAPIError,
    BybitError,
    BybitPositionDataError,
    is_benign_trading_stop_reject,
    map_broker_error,
    reject_error,
)
from .helpers import (
    EXECUTION_SINCE_SKEW_MS,
    contracts_to_base,
    format_decimal,
    round_price,
    wire_link_id,
)
from .models import InstrumentInfo

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
        """
        parsed: list[tuple[int, float, int]] = []
        snapshot_ms = 0
        for row in rows:
            updated_ms = self._position_row_updated_ms(row)
            parsed.append((
                self._position_row_index(row),
                self._position_row_size(row),
                updated_ms,
            ))
            snapshot_ms = max(snapshot_ms, updated_ms)
        if rows:
            sizes = self._deriv_sizes
            if sizes is None:
                sizes = {}
                self._deriv_sizes = sizes
            for idx, size, _ in parsed:
                sizes[idx] = size
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
                abs(self._run_owned_hedge_signed_size()) <= _FILL_EPS
                and self._deriv_snapshot_ms >= self._last_own_fill_ms
            )
        if not all(size <= 0.0 for size in sizes.values()):
            return False
        return self._deriv_snapshot_ms >= self._last_own_fill_ms

    def _run_owned_hedge_signed_size(self) -> float:
        """Return this run's signed hedge exposure from its durable fills.

        Bybit exposes one aggregate Buy row and one aggregate Sell row for the
        whole account. Those rows can include positions opened by another bot
        run or manually, so they are not an ownership boundary. The run's
        persisted fill cursors are the only attributable source: entry fills
        add exposure and filled close rows subtract it. Startup-adopted foreign
        rows are deliberately excluded.
        """
        if self.store_ctx is None:
            return 0.0
        owned = 0.0
        for row in self.store_ctx.iter_live_orders():
            if (row.extras or {}).get(ADOPTED_STARTUP_EXTRA_KEY):
                continue
            filled = float(row.filled_qty)
            if filled <= 0.0:
                continue
            owned += filled if row.side == 'buy' else -filled
        return owned

    def _scope_hedge_rows_to_run(self, rows: list[dict]) -> list[dict]:
        """Project account hedge rows onto the exposure owned by this run.

        A fresh run must see no tradable leg even when the account already
        carries a foreign hedge position. A restarted run sees only its own
        signed slice, bounded by the matching physical Bybit leg so stale
        journal state can never authorize an oversized reduction. Store-less
        unit paths retain the raw account view because they have no ownership
        ledger to consult.
        """
        if self.store_ctx is None:
            return rows
        owned = self._run_owned_hedge_signed_size()
        if abs(owned) <= _FILL_EPS:
            return []
        wanted_idx = HEDGE_IDX_BUY if owned > 0.0 else HEDGE_IDX_SELL
        for row in rows:
            if self._position_row_index(row) != wanted_idx:
                continue
            physical = self._position_row_size(row)
            scoped = min(abs(owned), physical)
            if scoped <= _FILL_EPS:
                return []
            projected = dict(row)
            projected['size'] = format_decimal(Decimal(str(scoped)))
            return [projected]
        return []

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
                size = contracts_to_base(size, entry_price)
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

    # --- startup adoption baseline (double-count barrier) -----------------------

    def _parked_adoption_executions(
            self, market: InstrumentInfo, order_id: str, floor_ms: int,
    ) -> tuple[set[str], float]:
        """Return exact private execution slices parked behind the adoption gate."""
        ids: set[str] = set()
        quantity = 0.0
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
                if not exec_id or exec_id in ids:
                    continue
                ids.add(exec_id)
                quantity += _wire_float(entry.get('execQty'), field='execQty')
        return ids, quantity

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
        those slices. A NON-ENTRY row (exit / bracket / close leg) whose
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
        for _ in range(_ADOPTION_STABLE_ATTEMPTS):
            seeds: list[tuple] = []
            pass_index_lag: str | None = None
            for row in self.store_ctx.iter_live_orders(symbol=market.symbol):
                if row.state in PENDING_DISPATCH_STATES:
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
                if order_id:
                    # A retained handle is still walked even for a dead
                    # order — its historical executions must seed the
                    # de-dup frontier and the cursor before any terminal
                    # decision is applied.
                    from_ms = row.created_ts_ms - EXECUTION_SINCE_SKEW_MS
                    ids, qty_sum, seeded = await self._recovery_fill_ids(
                        market, order_id, from_ms, until_ms=floor_ms,
                    )
                    if not seeded:
                        raise BybitAdoptionBaselineError(
                            "adoption baseline: execution seed read failed "
                            f"for order {order_id}",
                        )
                    parked_ids, parked_qty = self._parked_adoption_executions(
                        market,
                        order_id,
                        floor_ms,
                    )
                    ids.update(parked_ids)
                    qty_sum += parked_qty
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
                seeds.append((row, order_id, ids, qty_sum, lookup is None))
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
            self._adoption_baselined = True
            self._deriv_exec_floor_ms = floor_ms
            for row, order_id, ids, qty_sum, dead in seeds:
                self._seen_exec_ids.update(ids)
                cumulative: float = min(
                    float(row.qty), max(float(row.filled_qty), qty_sum),
                )
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
            self._clamp_entry_ownership_to_venue(market, verify)
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

    def _clamp_entry_ownership_to_venue(
            self, market: InstrumentInfo, rows: list[dict],
    ) -> None:
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
        for side in ('buy', 'sell'):
            live = sorted(
                (
                    row for row in self.store_ctx.iter_live_orders(
                        symbol=market.symbol)
                    if row.side == side
                    and row.state not in PENDING_DISPATCH_STATES
                    and (row.extras or {}).get('kind')
                    in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                    and row.filled_qty > _FILL_EPS
                ),
                key=lambda entry_row: entry_row.created_ts_ms,
            )
            excess = sum(row.filled_qty for row in live) - venue[side]
            if excess <= _FILL_EPS:
                continue
            logger.warning(
                "Bybit adoption baseline: %s entry rows own %s beyond the "
                "venue position — netting the excess (a close landed while "
                "the bot was down, or the rows predate fill-time netting)",
                side, excess,
            )
            for row in live:
                if excess <= _FILL_EPS:
                    break
                consumed = min(excess, row.filled_qty)
                owned = row.filled_qty - consumed
                # A working entry's unfilled residual can still fill on the
                # venue — the netting consumes only the OWNED (filled)
                # exposure and must keep the row alive for the resting
                # remainder; de-dup lives in the seeded exec frontier, so a
                # lowered cursor never re-books history.
                unfilled = max(0.0, row.qty - row.filled_qty)
                self.store_ctx.log_event(
                    'adoption_baseline_clamped',
                    client_order_id=row.client_order_id,
                    intent_key=row.intent_key,
                    payload={'from_filled_qty': row.filled_qty,
                             'filled_qty': owned, 'consumed': consumed},
                )
                if owned <= _FILL_EPS and unfilled <= _FILL_EPS:
                    # Retire only the physical row — the shared intent key
                    # may already own a fresh envelope, exactly like the
                    # fill-time netting.
                    self.store_ctx.close_order(row.client_order_id)
                else:
                    self.store_ctx.upsert_order(
                        row.client_order_id, qty=owned + unfilled,
                        filled_qty=owned,
                    )
                excess -= consumed

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
        rows = await self._apply_adoption_baseline(market, rows)
        rows = self._scope_hedge_rows_to_run(rows)
        legs: list[PositionLeg] = []
        for row in rows:
            size = self._position_row_size(row)
            if size <= 0.0:
                continue
            side = str(row.get('side') or '').lower()
            if side not in ('buy', 'sell'):
                continue
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
