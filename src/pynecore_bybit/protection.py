"""External-intervention detection mix-in for the Bybit plugin (derivatives).

Reports, through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`,
the two interventions only the plugin can see:

- **A moved or removed bot SL/TP.** One
  :class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` per
  plugin instance holds the levels the bot itself put on the venue:

  * an order-based exit leg (the spot / one-way SOFTWARE bracket: a limit
    TP leg, a conditional SL leg) under the leg row's ``orderLinkId``, fed
    by every private ``order`` push of that order;
  * a position trading stop (``/v5/position/trading-stop``: the one-way
    trailing attach, the hedge-mode bracket replication) under
    ``trading-stop:{positionIdx}``, fed by the private ``position`` push and
    the reconcile position snapshot.

  Every protection write is bracketed by ``expect`` / ``settle``
  (``execution.py`` / ``positions.py``), so the bot's own writes and their
  late echoes are never judged. Quantities are never compared: the venue
  shrinks a reduce-only leg on its own.
- **A manual close.** A closing execution of the run's exposure that no own
  order and no own trading stop explains, created by a user-side order type
  (:data:`MANUAL_CLOSE_CREATE_TYPES`). Venue mechanics (liquidation,
  takeover, ADL, settlement, admin close, a fired position TP/SL) are never
  reported, and a close of exposure that is not provably the run's is not
  either: the run's durable entry rows must own exposure on the closed leg
  and the venue leg must not carry foreign exposure on top of it.
"""
import logging
import math
from abc import ABC
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.idempotency import (
    parse_client_order_id,
    parse_wire_client_order_id,
)
from pynecore.core.broker.models import LegType
from pynecore.core.broker.protection_guard import (
    ProtectionLevelGuard,
    ProtectionLevels,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_BRACKET_OWN_ATTACH_COID,
    EXTRAS_KEY_BRACKET_OWN_CLEAR_COID,
    EXTRAS_KEY_BRACKET_OWN_LEG_ID,
    iter_active_bracket_ownerships,
)

from ._base import _BybitBase
from .exceptions import BybitPositionDataError
from .helpers import (
    CATEGORY_SPOT,
    exit_leg_levels,
    trading_stop_key,
    trading_stop_levels,
)
from .positions import HEDGE_IDX_BUY, HEDGE_IDX_SELL, POSITION_MODE_HEDGE

if TYPE_CHECKING:
    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: ``positionIdx`` of the one-way position.
_ONE_WAY_IDX = 0

#: ``createType`` values of an order a user or a user-run tool placed — the
#: only closes reported as a manual intervention. A venue mechanism
#: (``CreateByLiq``, ``CreateByTakeOver_PassThrough``,
#: ``CreateByAdl_PassThrough``, ``CreateByAdminClosing``, ``CreateBySettle``),
#: a fired position TP/SL (``CreateByTakeProfit`` & co., attributed through
#: the bracket-ownership rows) and any value Bybit adds later stay unreported.
MANUAL_CLOSE_CREATE_TYPES = frozenset({
    'CreateByUser',
    'CreateByClosing',
    'CreateByStopOrder',
    'CreateByMmRateClose',
    'CreateByTVSignal',
    'CreateByChaseOrder',
    'CreateByBboOrder',
    'CreateByTWAP',
    'CreateByIceBerg',
    'CreateByFGridBot',
    'CloseByFGridBot',
    'CreateByMartingaleBot',
    'CloseByMartingaleBot',
    'CreateByArbitrage',
})

#: ``stopOrderType`` values of a fired position TP/SL / trailing stop.
_POSITION_TPSL_STOP_TYPES = frozenset({
    'TakeProfit', 'PartialTakeProfit', 'StopLoss', 'PartialStopLoss',
    'TrailingStop',
})

_FILL_EPS = 1e-9

#: Position views kept per ``positionIdx`` to find the one preceding a late
#: execution (a backfill re-read can deliver it several views later).
_MAX_POSITION_VIEWS = 32


def _venue_level(raw: object) -> float | None:
    """Parse one venue price field; ``""`` / ``"0"`` mean absent.

    :raises ValueError: When the field is neither absent nor a finite
        non-negative number.
    """
    if raw is None or raw == '':
        return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        raise ValueError(f"not a price: {raw!r}")
    value = float(raw)
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"not a price: {raw!r}")
    return value or None


def _parse_seq(raw: object) -> int | None:
    """Parse a cross sequence (``seq``) of an execution or position row.

    Bybit stamps every fill and every position update of a symbol with one
    increasing sequence; the position update a fill causes carries the
    fill's ``seq``. ``None`` when the field is absent or unreadable.
    """
    if isinstance(raw, bool) or not isinstance(raw, (str, int)):
        return None
    try:
        seq = int(raw)
    except ValueError:
        return None
    return seq if seq > 0 else None


def _looks_like_pyne_link_id(link_id: str) -> bool:
    """Whether an ``orderLinkId`` has the shape of a PyneCore client id.

    Canonical and wire-form ids, and the one-way emulator's composed close-leg
    ids (``{coid}:{leg}`` mapped to ``{coid}_{leg}`` on the wire). Such an
    order may be this run's own close whose identity a restart lost.
    """
    if parse_wire_client_order_id(link_id) is not None:
        return True
    return parse_client_order_id(link_id.split('_', 1)[0]) is not None


class _ProtectionMixin(_BybitBase, ABC):
    """External-intervention detection: moved SL/TP and manual closes."""

    # --- guard plumbing -------------------------------------------------------

    def _protection_guard(self) -> ProtectionLevelGuard:
        """The plugin's protection guard, built once the journal is known."""
        guard = self._protection_guard_instance
        if guard is None or self._protection_guard_store is not self.store_ctx:
            guard = ProtectionLevelGuard(
                self._report_protection_change,
                store_ctx=self.store_ctx,
                clock=epoch_time,
            )
            self._protection_guard_instance = guard
            self._protection_guard_store = self.store_ctx
        return guard

    def _report_protection_change(self, reason: str, context: dict) -> None:
        """The guard's report hook: the configured intervention policy."""
        coid = context.get('client_order_id')
        self.report_external_intervention(
            reason, context,
            client_order_id=coid if isinstance(coid, str) else None,
        )

    def _expect_protection(
            self, key: str, levels: ProtectionLevels, *,
            row_coid: str | None, protects: str | None = None,
    ) -> int:
        """Record a protection write before it is sent.

        A trading stop moves between journal rows as exits replace each
        other; the guard moves its persisted levels along with ``row_coid``.

        :param key: The guard key.
        :param levels: The complete protection the write sets.
        :param row_coid: The journal row the levels persist on.
        :param protects: The position side (``'buy'`` / ``'sell'``) a
            trading stop protects; the venue clears the stop when that
            position closes or flips.
        :return: The write's id, for :meth:`_settle_protection`'s ``write``.
        """
        if protects is not None:
            self._protected_sides[key] = protects
        in_flight, _ = self._protection_writes.get(key, (0, 0.0))
        self._protection_writes[key] = (in_flight + 1, epoch_time())
        return self._protection_guard().expect(key, levels, row_coid=row_coid)

    def _settle_protection(
            self, key: str, confirmed: ProtectionLevels | None, *,
            rejected: ProtectionLevels | None = None, write: int | None = None,
    ) -> None:
        """Close one write recorded by :meth:`_expect_protection`.

        :param key: The guard key.
        :param confirmed: The levels the venue accepted; ``None`` when the
            write was refused or its outcome is unknown.
        :param rejected: The levels of a write the venue definitively
            refused (an answered reject, never a transport or server-side
            failure). They stop being acceptable at once; an unknown
            outcome leaves it ``None`` and the write stays acceptable until
            a venue observation resolves it.
        :param write: The id :meth:`_expect_protection` returned for this
            write, so the answer closes exactly that write even while other
            writes of the same levels are open.
        """
        in_flight = self._protection_writes.get(key)
        if in_flight is not None:
            self._protection_writes[key] = (max(0, in_flight[0] - 1), epoch_time())
        self._protection_guard().settle(key, confirmed, rejected=rejected, write=write)

    def _forget_protection(self, key: str) -> None:
        """Stop guarding ``key``: its order, or its position, is gone."""
        self._protected_sides.pop(key, None)
        self._protection_writes.pop(key, None)
        self._protection_guard().forget(key)

    def _view_postdates_own_writes(self, key: str, read_started_at: float | None) -> bool:
        """Whether a position view may retire ``key``'s protection.

        The same gate the guard applies to a judgement: no own write of the
        key in flight, and a snapshot read that started after the last one
        settled. A flat or flipped view older than the bot's own attach
        must not forget the stop the attach just put on the new position.
        """
        write = self._protection_writes.get(key)
        if write is None:
            return True
        in_flight, written_at = write
        if in_flight > 0:
            return False
        return read_started_at is None or read_started_at >= written_at

    def _forget_trading_stop_if_flat(self, side: str) -> None:
        """Forget the trading stop of a position the bot's own fill closed.

        The venue clears a position's TP/SL and trailing stop when the
        position goes flat. A position view of the flat leg forgets the key
        (:meth:`_observe_position_rows`), but the bot may re-enter the same
        side before such a view arrives, and the new position must not be
        judged against the old position's levels. Called after a reducing
        own fill has netted the run's entry rows: when no live entry row of
        the closed side carries exposure any more, the position the stop
        protected is gone. A partial reduce leaves exposure behind and keeps
        the key guarded.

        :param side: The reducing fill's side (``'buy'`` / ``'sell'``).
        """
        if self.store_ctx is None or side not in ('buy', 'sell'):
            return
        entry_side = 'sell' if side == 'buy' else 'buy'
        if self._position_mode == POSITION_MODE_HEDGE:
            idx = HEDGE_IDX_BUY if entry_side == 'buy' else HEDGE_IDX_SELL
        else:
            idx = _ONE_WAY_IDX
        key = trading_stop_key(idx)
        if not self._protection_guard().known(key):
            return
        if self._trading_stop_side(key, idx) not in (entry_side, None):
            return
        for row in self.store_ctx.iter_live_orders():
            if (row.side == entry_side
                    and (row.extras or {}).get('kind')
                    in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
                    and row.filled_qty > _FILL_EPS):
                return
        self._forget_protection(key)

    def _trading_stop_row_coid(self, leg_id: str, coid: str) -> str | None:
        """The live ownership row a trading-stop write on ``leg_id`` belongs to.

        The row whose attach or clear id is ``coid`` when there is one, else
        the newest live ownership row of the leg; ``None`` storeless or when
        the leg has no ownership row.
        """
        if self.store_ctx is None:
            return None
        newest: str | None = None
        for row in iter_active_bracket_ownerships(self.store_ctx):
            extras = row.extras or {}
            if str(extras.get(EXTRAS_KEY_BRACKET_OWN_LEG_ID)) != leg_id:
                continue
            if coid in (extras.get(EXTRAS_KEY_BRACKET_OWN_ATTACH_COID),
                        extras.get(EXTRAS_KEY_BRACKET_OWN_CLEAR_COID)):
                return row.client_order_id
            newest = row.client_order_id
        return newest

    def _trading_stop_side(self, key: str, position_idx: int) -> str | None:
        """The position side the trading stop on ``position_idx`` protects.

        A hedge leg's index fixes its side. A one-way stop's side is recorded
        at the write; after a restart it comes from the live ownership row
        (the exit side is the opposite of the protected position).
        """
        if position_idx == HEDGE_IDX_BUY:
            return 'buy'
        if position_idx == HEDGE_IDX_SELL:
            return 'sell'
        side = self._protected_sides.get(key)
        if side is not None or self.store_ctx is None:
            return side
        for row in iter_active_bracket_ownerships(self.store_ctx):
            if str((row.extras or {}).get(EXTRAS_KEY_BRACKET_OWN_LEG_ID)) \
                    != str(position_idx):
                continue
            if row.side in ('buy', 'sell'):
                side = 'sell' if row.side == 'buy' else 'buy'
                self._protected_sides[key] = side
                return side
        return None

    # --- observations ---------------------------------------------------------

    def _observe_exit_leg_push(
            self, entry: dict, coid: str, leg_type: LegType,
            market: 'InstrumentInfo',
    ) -> None:
        """Judge one ``New`` / ``Untriggered`` push of an order-based exit leg.

        Judged at once: the push is the venue's word on this exact order.
        Only the price the leg carries is compared, never its quantity (the
        venue shrinks a reduce-only leg by itself).
        """
        if not coid or leg_type not in (LegType.TAKE_PROFIT, LegType.STOP_LOSS):
            return
        guard = self._protection_guard()
        if not guard.known(coid):
            return
        if self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                self._forget_protection(coid)
                return
        field = 'price' if leg_type is LegType.TAKE_PROFIT else 'triggerPrice'
        try:
            price = _venue_level(entry.get(field))
        except ValueError:
            logger.warning("Bybit exit leg %s push carries an unreadable %s: %r",
                           coid, field, entry.get(field))
            return
        if price is None:
            return
        guard.observe(coid, exit_leg_levels(leg_type, price),
                      tick=market.tick_size, immediate=True)

    def _observe_position_rows(
            self, rows: list[dict], market: 'InstrumentInfo', *,
            read_started_at: float | None,
    ) -> None:
        """Judge the trading stops a position push or snapshot shows.

        A flat leg, or a one-way position that flipped side, lost its trading
        stop to the venue: the key is forgotten, never judged — unless the
        view may predate the bot's own write of the key (in flight, or a
        snapshot read that started before it settled). A row older (by
        ``seq``) than a view of the same leg already seen is a delayed push:
        it neither forgets nor is judged. A push is not specific to one
        change (the venue pushes on every order event), so a mismatch needs
        a second observation.

        :param rows: The symbol's position rows.
        :param read_started_at: Wall-clock start of a REST snapshot read;
            ``None`` for a push.
        """
        if market.category == CATEGORY_SPOT:
            return
        guard = self._protection_guard()
        for row in rows:
            if str(row.get('symbol') or '') != market.symbol:
                continue
            try:
                idx = self._position_row_index(row)
                size = self._position_row_size(row)
            except BybitPositionDataError:
                continue
            key = trading_stop_key(idx)
            if not guard.known(key):
                continue
            if self._position_view_superseded(idx, row):
                continue
            if size <= 0.0:
                if self._view_postdates_own_writes(key, read_started_at):
                    self._forget_protection(key)
                continue
            side = self._trading_stop_side(key, idx)
            if side is None:
                continue
            if str(row.get('side') or '').lower() != side:
                if self._view_postdates_own_writes(key, read_started_at):
                    self._forget_protection(key)
                continue
            try:
                trail = _venue_level(row.get('trailingStop'))
                venue = trading_stop_levels(
                    _venue_level(row.get('takeProfit')),
                    _venue_level(row.get('stopLoss')),
                    trail,
                )
            except ValueError:
                logger.warning(
                    "Bybit position %s row carries an unreadable TP/SL field: %r",
                    idx, row,
                )
                continue
            guard.observe(key, venue, tick=market.tick_size,
                          read_started_at=read_started_at)

    # --- manual close ---------------------------------------------------------

    def _note_external_execution(
            self, entry: dict, market: 'InstrumentInfo', *, ambiguous: bool,
    ) -> None:
        """Inspect one execution no own order or own trading stop explains.

        An opening part is remembered by its ``seq``: a position view older
        than it does not show that foreign exposure. A closing part is
        reported as a manual close only when it provably reduced the run's
        exposure.

        :param ambiguous: The bracket-ownership attribution refused to pick
            between several own rows — the close may be the bot's own.
        """
        if market.category == CATEGORY_SPOT:
            return
        try:
            closed = float(entry.get('closedSize') or 0.0)
            exec_qty = float(entry.get('execQty') or 0.0)
        except (TypeError, ValueError):
            return
        side = str(entry.get('side') or '').lower()
        if side not in ('buy', 'sell'):
            return
        hedge = self._position_mode == POSITION_MODE_HEDGE
        if exec_qty > closed + _FILL_EPS:
            opened_idx = ((HEDGE_IDX_BUY if side == 'buy' else HEDGE_IDX_SELL)
                          if hedge else _ONE_WAY_IDX)
            self._foreign_open_seqs.setdefault(opened_idx, []).append(
                _parse_seq(entry.get('seq')) or 0,
            )
        if closed <= 0.0:
            return
        exec_id = str(entry.get('execId') or '')
        if not exec_id or exec_id in self._reported_external_exec_ids:
            return
        reason = self._manual_close_blocker(entry, market, side=side,
                                            ambiguous=ambiguous)
        if reason is not None:
            logger.info("Bybit external close %s not reported as an intervention: %s",
                        exec_id, reason)
            return
        self._reported_external_exec_ids.add(exec_id)
        closed_idx = ((HEDGE_IDX_BUY if side == 'sell' else HEDGE_IDX_SELL)
                      if hedge else _ONE_WAY_IDX)
        order_id = str(entry.get('orderId') or '') or None
        create_type = str(entry.get('createType') or '')
        self.report_external_intervention(
            f"bot position on {market.symbol} closed on the venue by someone "
            f"else: {side} {closed} ({create_type}, order {order_id})",
            {
                'exec_id': exec_id,
                'order_id': order_id,
                'create_type': create_type,
                'closed_size': closed,
                'side': side,
                'position_idx': closed_idx,
            },
            exchange_order_id=order_id,
        )

    def _manual_close_blocker(
            self, entry: dict, market: 'InstrumentInfo', *,
            side: str, ambiguous: bool,
    ) -> str | None:
        """Why a foreign closing execution must not be reported, or ``None``."""
        if self.store_ctx is None:
            return "no journal to prove the run's exposure"
        if not self._adoption_baselined:
            return "the startup adoption baseline has not committed yet"
        if ambiguous:
            return "the bracket-ownership attribution was ambiguous"
        link_id = str(entry.get('orderLinkId') or '')
        if link_id and _looks_like_pyne_link_id(link_id):
            return f"orderLinkId {link_id!r} has the shape of a bot client id"
        if str(entry.get('stopOrderType') or '') in _POSITION_TPSL_STOP_TYPES:
            return "a position TP/SL fired (venue mechanism)"
        create_type = str(entry.get('createType') or '')
        if not create_type:
            logger.warning(
                "Bybit closing execution %s carries no createType; its origin "
                "is unknown", entry.get('execId'),
            )
            return "no createType"
        if create_type not in MANUAL_CLOSE_CREATE_TYPES:
            return f"createType {create_type!r} is not a user order"
        hedge = self._position_mode == POSITION_MODE_HEDGE
        idx = ((HEDGE_IDX_BUY if side == 'sell' else HEDGE_IDX_SELL)
               if hedge else _ONE_WAY_IDX)
        # The leg is judged as it stood right before this execution: a view
        # at or past its ``seq`` already shows the close (and possibly the
        # flat sweep that retired the run's rows on it).
        exec_seq = _parse_seq(entry.get('seq'))
        if exec_seq is None:
            return "no seq to order the execution against the position views"
        view = self._position_view_before(idx, exec_seq)
        if view is None:
            return "no position view of the leg precedes the execution"
        view_seq, physical, view_ms = view
        if view_ms < self._last_own_fill_ms:
            return "an own fill postdates the position view before the execution"
        if any(seq == 0 or view_seq < seq < exec_seq
               for seq in self._foreign_open_seqs.get(idx, ())):
            return "foreign exposure opened on the leg after the position view"
        swept = self._swept_ownership.get(idx)
        if swept is not None and exec_seq <= swept[1]:
            # The flat sweep retired the rows after this execution: the run's
            # exposure is the one the sweep saw.
            owned = swept[0]
        else:
            owned = self._run_owned_hedge_signed_size()
        owned_on_leg = owned if side == 'sell' else -owned
        if owned_on_leg <= _FILL_EPS:
            return "the run owns no exposure on the closed side"
        tolerance = market.qty_step / 2 if market.qty_step > 0 else _FILL_EPS
        if physical > owned_on_leg + tolerance:
            return (f"the leg held {physical}, more than the run's "
                    f"{owned_on_leg}: foreign exposure may have been closed")
        return None

    def _position_view_superseded(self, idx: int, row: dict) -> bool:
        """Whether a newer view of leg ``idx`` than ``row`` was already seen.

        The views are recorded before the rows are judged, so the row's own
        view is in the history; a strictly larger ``seq`` there means the
        row arrived late. A row without a ``seq`` cannot be ordered and is
        never superseded.
        """
        seq = _parse_seq(row.get('seq'))
        views = self._position_views.get(idx)
        return seq is not None and bool(views) and views[-1][0] > seq

    def _position_view_before(
            self, idx: int, exec_seq: int,
    ) -> tuple[int, float, int] | None:
        """The newest recorded view of leg ``idx`` older than ``exec_seq``."""
        for view in reversed(self._position_views.get(idx, ())):
            if view[0] < exec_seq:
                return view
        return None

    def _record_position_views(self, rows: list[dict]) -> None:
        """Remember the ``(seq, size, updatedTime)`` of each position row.

        Called for every position push and REST read, before the flat sweep
        can retire the rows of a position someone else closed. A row without
        a ``seq`` cannot be ordered against an execution and is skipped.
        """
        for row in rows:
            try:
                idx = self._position_row_index(row)
                size = self._position_row_size(row)
            except BybitPositionDataError:
                continue
            seq = _parse_seq(row.get('seq'))
            if seq is None:
                continue
            try:
                updated_ms = int(row.get('updatedTime') or 0)
            except (TypeError, ValueError):
                updated_ms = 0
            views = self._position_views.setdefault(idx, [])
            views[:] = [view for view in views if view[0] != seq]
            views.append((seq, size, updated_ms))
            views.sort(key=lambda view: view[0])
            del views[:-_MAX_POSITION_VIEWS]
            # An unsequenced foreign open is covered by any later view; one
            # older than every kept view can no longer fall between a view
            # and an execution.
            opens = self._foreign_open_seqs.get(idx)
            if opens:
                oldest = views[0][0]
                opens[:] = [open_seq for open_seq in opens if open_seq > oldest]
                if not opens:
                    del self._foreign_open_seqs[idx]

    def _remember_swept_ownership(self) -> None:
        """Keep the run's one-way exposure the flat sweep is about to retire.

        A manual close whose execution arrives after the flat view (a
        position push first, or the reconcile backfill) must still be judged
        against the exposure the run held. The record is scoped to the
        flat view's ``seq``: only an execution at or before it can have
        closed that exposure. Hedge mode never sweeps an owned leg.
        """
        if self.store_ctx is None or self._position_mode == POSITION_MODE_HEDGE:
            return
        owned = self._run_owned_hedge_signed_size()
        views = self._position_views.get(_ONE_WAY_IDX)
        if abs(owned) <= _FILL_EPS or not views:
            return
        self._swept_ownership[_ONE_WAY_IDX] = (owned, views[-1][0])
