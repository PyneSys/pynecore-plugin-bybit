"""External-intervention detection mix-in for the Bybit plugin.

Reports, through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`,
the interventions only the plugin can see:

- **A moved or removed bot SL/TP, an amended bot order.** One
  :class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` per
  plugin instance holds the levels the bot itself put on the venue:

  * an order-based exit leg (the spot / one-way SOFTWARE bracket: a limit
    TP leg, a conditional SL leg) under the leg row's ``orderLinkId``;
  * a resting entry order (a limit price, a stop trigger) under its
    ``orderLinkId``;
  * a position trading stop (``/v5/position/trading-stop``: the one-way
    trailing attach, the hedge-mode bracket replication) under
    ``trading-stop:{positionIdx}``.

  Order keys are fed by every private ``order`` push of the order (judged at
  once) and by the open-orders snapshot of the reconcile cadence (two
  consecutive observations), so an edit whose push was lost, or that landed
  while the bot's own amend of the same order was in flight, is still seen.
  Trading stops are fed by the private ``position`` push and the reconcile
  position snapshot. Every protection write is bracketed by ``expect`` /
  ``settle`` (``execution.py`` / ``positions.py``), so the bot's own writes
  and their late echoes are never judged.
- **A protection the bot never set added to its position.** A position the
  run holds a share of without a trading stop of the bot's (it protects it
  with order legs, with nothing, or cleared its own stop) is guarded with a
  bare baseline: a TP / SL / trailing stop someone else adds to it changes
  the bot's protection. The baseline lives on an entry row of the run that
  holds no other guard record, and moves on when reducing fills net that
  row away, so it survives a restart. A partial TP/SL order (``Partial*``
  ``stopOrderType``; the bot never places one) resting on the run's
  position is reported when it covers the run's share — once the evidence
  covers its creation: a position view taken after it, executions that
  explain the views, and the fill-report grace (the order push of a partial
  TP/SL attached to someone else's fill routinely arrives before the view
  of that fill). A view or an order that may predate the run's fill is
  never taken as an addition.

  Order quantities are judged apart from the levels: an order whose
  quantity grew past every quantity the bot sent, or a resting spot exit
  leg showing any other quantity, was amended by someone else. A
  derivative order cut below them is never reported — the venue trims a
  reduce-only leg to the position and reduces active orders to free margin
  for a close-on-trigger order, and the order view alone cannot tell that
  from a foreign amend — but a cut that leaves an exit leg short of its
  position, or any cut entry, is logged as an error with an audit event.
  The quantities sent are journalled before each request leaves, so an
  amend whose outcome stayed unknown is still the bot's own after a
  restart, and an operator resume takes the venue's quantity as accepted,
  like the levels.
- **A close of the run's exposure.** Any execution no own order explains is
  entered into the per-leg ledger (:mod:`.ledger`) with its ``seq``, next to
  the position views and the run's own fills. Once the executions of an
  interval provably account for its views (every view of the interval is
  stamped with the ``seq`` of a known execution and matches the executions
  before it; never while a private-stream outage is not re-read from the
  execution history yet), each foreign execution is judged by
  whether it left the leg below the run's own share: a user-side order type
  (:data:`MANUAL_CLOSE_CREATE_TYPES`) or user transaction
  (:data:`USER_EXEC_TYPES`) that did is reported; a close of foreign
  exposure, a same-direction add, a trade on the other hedge leg never is.
  Venue mechanics (liquidation, takeover, ADL, settlement, admin close) are
  never reported, and an execution whose origin cannot be proven is logged
  as an error with an audit event instead. A fired position TP/SL that no
  ownership row of any instance of the run ever placed on that position is
  a protection someone else set, judged like a manual close; one on a
  position the run once protected may be the bot's own released stop and
  stays unproven. Whatever took the run's share,
  the run's journal books the venue truth: the entry rows are netted down
  to what the leg still holds for the run (on inverse, the net-position
  mirror with them, so the core-facing size stays exact). The share never
  exceeds what the
  run's own journaled fills built; foreign exposure an own fill consumed
  (a one-way reversal over a foreign add) is logged, never charged. An
  interval whose executions stay missing past the grace is given up: the
  share is clamped to the venue view and what is left of it is held in
  doubt, so a later close of it is logged as an error, never reported,
  until the late executions complete that interval.
"""
import logging
import math
from abc import ABC
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.idempotency import (
    parse_client_order_id,
    parse_wire_client_order_id,
)
from pynecore.core.broker.models import LegType
from pynecore.core.broker.protection_guard import (
    PROTECTION_LEVELS_EXTRA,
    ProtectionLevelGuard,
    ProtectionLevels,
)
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_BRACKET_OWN_ATTACH_COID,
    EXTRAS_KEY_BRACKET_OWN_CLEAR_COID,
    EXTRAS_KEY_BRACKET_OWN_LEG_ID,
    STATE_BRACKET_OWN,
    iter_active_bracket_ownerships,
)
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _BybitBase
from .exceptions import BybitError, BybitPositionDataError, traceback_wanted
from .helpers import (
    CATEGORY_SPOT,
    ENTRY_NETTED_QTY_EXTRA,
    OWN_ORDER_QTYS_EXTRA,
    execution_sort_key,
    exit_leg_levels,
    trading_stop_key,
    trading_stop_levels,
)
from .ledger import (
    ORIGIN_AMBIGUOUS,
    ORIGIN_UNPROVEN,
    ORIGIN_USER,
    ORIGIN_VENUE,
    LegEvent,
    LegLedger,
    LegOutcome,
)
from .positions import HEDGE_IDX_BUY, HEDGE_IDX_SELL, POSITION_MODE_HEDGE

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: ``positionIdx`` of the one-way position.
_ONE_WAY_IDX = 0

#: ``createType`` values of an order a user or a user-run tool placed — the
#: only foreign executions reported as an intervention.
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
    'CreateByFutureSpread',
    'CreateByBlock_PassThrough',
    'CreateByBlockTradeMovePosition_PassThrough',
})

#: ``createType`` values of a venue mechanism: never reported.
_VENUE_CREATE_TYPES = frozenset({
    'CreateByLiq',
    'CreateByTakeOver_PassThrough',
    'CreateByAdl_PassThrough',
    'CreateByAdminClosing',
    'CreateBySettle',
})

#: ``execType`` values of a size-changing venue execution outside ``Trade``
#: (ADL, takeover liquidation, delivery / delisting close, inverse futures
#: settlement, stock-split fractional settlement). They are never booked;
#: the ledger takes them as venue mechanics.
VENUE_EXEC_TYPES = frozenset({
    'AdlTrade',
    'BustTrade',
    'Delivery',
    'Settle',
    'ForwardSplitSettle',
    'ReverseSplitSettle',
})

#: ``execType`` values of a size-changing execution outside ``Trade`` that
#: a user starts (a block trade, a position moved between accounts, a spread
#: leg). The bot places none of them: the ledger judges them like a manual
#: order. Every other non-``Trade`` type (``Funding``, ``Dividend``) is a
#: cashflow that moves no position; only its ``seq`` is recorded.
USER_EXEC_TYPES = frozenset({'BlockTrade', 'MovePosition', 'FutureSpread'})

#: ``stopOrderType`` values of a fired ``Full``-mode position TP/SL or
#: trailing stop. No live ownership row explains it; it is the bot's own
#: only when the run ever set a trading stop on that position.
_POSITION_TPSL_STOP_TYPES = frozenset({'TakeProfit', 'StopLoss', 'TrailingStop'})

#: ``stopOrderType`` values of a fired ``Partial``-mode position TP/SL. The
#: bot writes every trading stop with ``tpslMode=Full``: a Partial stop is
#: always one someone else placed.
_PARTIAL_TPSL_STOP_TYPES = frozenset({'PartialTakeProfit', 'PartialStopLoss'})

#: ``orderStatus`` values of a resting order whose quantity is still the one
#: the last amend set (no execution consumed it yet).
_UNFILLED_ORDER_STATUSES = frozenset({'New', 'Untriggered'})

#: ``orderStatus`` values of a conditional order that fired.
_FIRED_ORDER_STATUSES = frozenset({'Triggered', 'PartiallyFilled', 'Filled'})

#: Seconds a foreign partial TP/SL order waits for the evidence of its
#: verdict (a position view taken after it, executions explaining the views)
#: before it is logged as unproven.
_FOREIGN_TPSL_VERDICT_CEILING_S = 300.0

_FILL_EPS = 1e-9

#: Execution ids remembered for the ledger's once-only intake.
_MAX_LEDGER_EXEC_IDS = 4096

#: Own quantities remembered per guarded order.
_MAX_OWN_ORDER_QTYS = 8

#: Audit event holding a leg ledger's credits (the newest one per leg wins).
LEDGER_CREDITS_EVENT = 'position_ledger_credits'

#: Audit event of a judged foreign TP/SL order (one per verdict; an amend
#: of the order is judged again).
FOREIGN_TPSL_ORDER_EVENT = 'foreign_tpsl_order_judged'

#: Audit event of a trading stop the run's position carried, first seen with
#: the run's share on it, whose origin is not known.
TRADING_STOP_ORIGIN_UNPROVEN_EVENT = 'trading_stop_origin_unproven'


def _venue_level(raw: object) -> float | None:
    """Parse one venue price field; ``""`` / ``"0"`` mean absent.

    :raises ValueError: When the field is neither absent nor a finite
        non-negative number.
    """
    if raw is None or raw == '':
        return None
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        raise ValueError(f"not a price: a {type(raw).__name__}")
    value = float(raw)
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"not a price: {raw!r}")
    return value or None


def _venue_qty(raw: object) -> float | None:
    """Parse one venue quantity field; ``None`` when absent or unreadable."""
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)) or raw == '':
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if math.isfinite(value) and value > 0.0 else None


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


def _parse_ms(raw: object) -> int | None:
    """Parse a venue epoch-ms timestamp (``execTime``, ``createdTime``).

    ``None`` when the field is absent, unreadable or not positive.
    """
    if isinstance(raw, bool) or not isinstance(raw, (str, int)):
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


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
    """External-intervention detection: edited protection and orders, closes."""

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
        self._own_order_qtys.pop(key, None)
        self._qty_mismatches.pop(key, None)
        self._qty_cut_logged.pop(key, None)
        self._qty_rebaseline_pending.discard(key)
        self._protection_guard().forget(key)

    def _note_own_order_qty(self, coid: str, qty: float) -> None:
        """Remember a quantity the bot itself sent for order ``coid``.

        Journalled on the order's row before the request leaves, so the
        quantity stays the bot's own across a restart even when the
        request's outcome never became known.
        """
        sent = self._own_order_qtys.setdefault(coid, [])
        sent.append(qty)
        del sent[:-_MAX_OWN_ORDER_QTYS]
        self._write_own_order_qtys(coid, lambda saved: [*saved, qty])

    def _retract_own_order_qty(self, coid: str, qty: float) -> None:
        """Withdraw a quantity the venue definitively refused for order ``coid``.

        Undoes the latest :meth:`_note_own_order_qty` of ``qty`` in memory
        and in the journal: an answered reject proves the order never
        carried it, so a later venue view showing it is someone else's
        edit. An earlier note of the same quantity stays — that write may
        have landed.
        """

        def drop_latest(qtys: list[float]) -> list[float]:
            for index in range(len(qtys) - 1, -1, -1):
                if qtys[index] == qty:
                    return [*qtys[:index], *qtys[index + 1:]]
            return qtys

        sent = self._own_order_qtys.get(coid)
        if sent is not None:
            sent[:] = drop_latest(sent)
        self._write_own_order_qtys(coid, drop_latest)

    def _write_own_order_qtys(
            self, coid: str, update: Callable[[list[float]], list[float]],
    ) -> None:
        """Rewrite the journalled own quantities of order ``coid``.

        :param update: Maps the journalled list to the new one.
        """
        if self.store_ctx is None:
            return
        with self.store_ctx.transaction():
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                return
            extras = dict(row.extras or {})
            saved = extras.get(OWN_ORDER_QTYS_EXTRA)
            qtys = [float(qty) for qty in saved if isinstance(qty, (int, float))] \
                if isinstance(saved, list) else []
            extras[OWN_ORDER_QTYS_EXTRA] = update(qtys)[-_MAX_OWN_ORDER_QTYS:]
            self.store_ctx.upsert_order(coid, extras=extras)

    def _own_order_qty_candidates(self, coid: str) -> list[float]:
        """Every quantity of order ``coid`` that is the bot's own.

        The ones sent in this process, the journalled ones, and the journal
        row's own quantity (venue domain: what reducing fills netted off it
        added back).
        """
        own = list(self._own_order_qtys.get(coid, ()))
        if self.store_ctx is None:
            return own
        row = self.store_ctx.get_order(coid)
        if row is None:
            return own
        extras = row.extras or {}
        saved = extras.get(OWN_ORDER_QTYS_EXTRA)
        if isinstance(saved, list):
            own.extend(float(qty) for qty in saved if isinstance(qty, (int, float)))
        netted = extras.get(ENTRY_NETTED_QTY_EXTRA)
        own.append(float(row.qty) + (float(netted) if isinstance(netted, (int, float))
                                     else 0.0))
        return own

    def _protection_rebaseline_flagged(self, coid: str) -> bool:
        """Whether an operator resume left order ``coid``'s levels to be re-baselined."""
        if self.store_ctx is None:
            return False
        row = self.store_ctx.get_order(coid)
        saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA) if row is not None else None
        return isinstance(saved, dict) and bool(saved.get('rebaseline'))

    def _view_postdates_own_writes(self, key: str, read_started_at: float | None) -> bool:
        """Whether a venue view may judge or retire ``key``.

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
        """Forget the trading stop of a position the run no longer holds anything of.

        The venue clears a position's TP/SL and trailing stop when the
        position goes flat. A position view of the flat leg forgets the key
        (:meth:`_observe_position_rows`), but the bot may re-enter the same
        side before such a view arrives, and the new position must not be
        judged against the old position's levels. Called after a reducing
        own fill has netted the run's entry rows, and after the position
        ledger netted them (:meth:`_net_side_share`): when no live entry row
        of the closed side carries exposure any more, the run's position the
        stop protected is gone, even when someone else's exposure keeps the
        leg open. A partial reduce leaves exposure behind and keeps the key
        guarded, on a row that still lives (:meth:`_home_trading_stop`).

        :param side: The reducing side (``'buy'`` / ``'sell'``).
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
                # The netting may have closed the row the levels lived on.
                self._home_trading_stop(key, idx)
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
        at the write (or at the bare baseline); after a restart it comes from
        the live ownership row (the exit side is the opposite of the
        protected position), and without one — a bare baseline — from the
        run's own share of the position.
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
        side = self._run_side_on_leg(position_idx)
        if side is not None:
            self._protected_sides[key] = side
        return side

    def _run_side_on_leg(self, idx: int) -> str | None:
        """The side of the run's own share on leg ``idx``; ``None`` without one."""
        share = self._journal_leg_shares().get(idx, 0.0)
        market = self._market
        tolerance = self._qty_tolerance(market) if market is not None else _FILL_EPS
        if abs(share) <= tolerance:
            return None
        if idx == HEDGE_IDX_BUY:
            return 'buy'
        if idx == HEDGE_IDX_SELL:
            return 'sell'
        return 'buy' if share > 0.0 else 'sell'

    def _bare_baseline_row_coid(self, side: str, key: str) -> str | None:
        """The journal row a trading stop with no ownership row of its own persists on.

        The bare baseline (:meth:`_baseline_bare_trading_stop`), a stop the
        bot cleared (:meth:`_guard_cleared_trading_stop`), or one whose
        ownership row closed while the run still holds the position
        (:meth:`_home_trading_stop`) live on one of the run's own entry rows
        of the position side. A row holds one guard record only, so the row
        must carry no other: an entry still resting (an unfilled remainder)
        carries its own limit or trigger under its own key. Of the rows
        left, the one already holding ``key``, else the newest: reducing
        fills net the entry rows oldest first, so it lives the longest.

        :return: ``None`` when no such row exists: the levels stay where they
            are (in memory only until :meth:`_home_trading_stop` finds one).
        """
        rows = [row for row in self._owned_entry_rows()
                if row.side == side and self._bare_home_eligible(row, key)]
        for row in rows:
            saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA)
            if isinstance(saved, dict) and saved.get('key') == key:
                return row.client_order_id
        rows.sort(key=lambda item: item.created_ts_ms)
        return rows[-1].client_order_id if rows else None

    def _bare_home_eligible(self, row: 'OrderRow', key: str) -> bool:
        """Whether entry row ``row`` may hold the guard record of trading stop ``key``."""
        market = self._market
        tolerance = self._qty_tolerance(market) if market is not None else _FILL_EPS
        if float(row.qty) - float(row.filled_qty) > tolerance:
            return False
        saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA)
        if saved is not None and not (isinstance(saved, dict) and saved.get('key') == key):
            return False
        return not self._protection_guard().known(row.client_order_id)

    def _home_trading_stop(self, key: str, idx: int) -> None:
        """Keep the guarded levels of trading stop ``key`` on a live journal row.

        The guard persists the levels on one row and restores them from the
        live rows only. A row holding them can close while the run still
        holds the position: reducing fills net the oldest entry row away,
        and an attach that never landed leaves the levels on an ownership row
        that is released. A position the bot left bare, or holding a stop
        whose row closed, would then lose its guard at the next restart, and
        a stop someone else added meanwhile could no longer be reported.
        Levels found on no live ownership row or eligible entry row move to
        the row :meth:`_bare_baseline_row_coid` names.
        """
        if self.store_ctx is None:
            return
        guard = self._protection_guard()
        if not guard.known(key):
            return
        side = self._run_side_on_leg(idx)
        if side is None:
            return
        owned = {row.client_order_id for row in self._owned_entry_rows() if row.side == side}
        for row in self.store_ctx.iter_live_orders():
            saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA)
            if not (isinstance(saved, dict) and saved.get('key') == key):
                continue
            if row.state == STATE_BRACKET_OWN or (
                    row.client_order_id in owned and self._bare_home_eligible(row, key)):
                return
            break
        target = self._bare_baseline_row_coid(side, key)
        if target is None:
            return
        # A write the guard knows refused moves the levels, every write and
        # verdict state with them, onto the named row and changes nothing else.
        write = guard.expect(key, ProtectionLevels(), row_coid=target)
        guard.settle(key, None, rejected=ProtectionLevels(), write=write)

    def _note_share_start(
            self, entry: dict, before: dict[int, float], after: dict[int, float],
            market: 'InstrumentInfo',
    ) -> None:
        """Remember the own execution that gave the run its share on a leg.

        Called with the run's journal share per leg before and after the
        execution's booking. A position view older than that execution may
        show the leg before the run held anything on it, so it cannot set the
        bare trading-stop baseline (:meth:`_baseline_bare_trading_stop`); a
        foreign TP/SL order created before it was not added to the run's
        position (:meth:`_judge_tpsl_order`). Without a ``seq`` or
        an ``execTime`` the start is unknown, as after a restart.

        The view of the position the execution built may have arrived before
        it (the venue pushes ``position`` and ``execution`` as separate
        frames): the newest view of the leg not older than the start then
        sets the baseline now, so a stop someone else adds before the next
        view is judged against it. Without such a view the leg waits for its
        first one (:attr:`_share_unviewed`): the bot may attach its own stop
        before it arrives.
        """
        tolerance = self._qty_tolerance(market)
        seq = _parse_seq(entry.get('seq'))
        exec_ms = _parse_ms(entry.get('execTime'))
        for idx in set(before) | set(after):
            old = before.get(idx, 0.0)
            new = after.get(idx, 0.0)
            new_side = 0.0 if abs(new) <= tolerance else math.copysign(1.0, new)
            old_side = 0.0 if abs(old) <= tolerance else math.copysign(1.0, old)
            if new_side == old_side:
                continue
            self._share_held_since.pop(idx, None)
            self._share_unviewed.discard(idx)
            if not new_side or seq is None or exec_ms is None:
                self._share_start.pop(idx, None)
                continue
            self._share_start[idx] = (seq, exec_ms)
            self._baseline_bare_trading_stop_from_last_view(idx)
            if not self._protection_guard().known(trading_stop_key(idx)):
                self._share_unviewed.add(idx)

    def _baseline_bare_trading_stop_from_last_view(self, idx: int) -> None:
        """Set the bare baseline of leg ``idx`` from the newest view already seen."""
        row = self._leg_last_view.get(idx)
        key = trading_stop_key(idx)
        if row is None or self._protection_guard().known(key):
            return
        try:
            size = self._position_row_size(row)
        except BybitPositionDataError:
            return
        if size > 0.0:
            self._baseline_bare_trading_stop(idx, key, row)

    def _note_share_held_since(self, since_ms: int, market: 'InstrumentInfo') -> None:
        """Remember from when the run is proven to hold the share it holds now.

        Called when the adoption baseline commits: the journal then holds the
        run's share as of the venue time the baseline read
        (``since_ms``), every execution up to it walked. The own execution
        that started a share held across a restart is not known
        (:attr:`_share_start`), but a foreign TP/SL order created after
        ``since_ms`` was added to the position the run held
        (:meth:`_judge_tpsl_order`).
        """
        if market.category == CATEGORY_SPOT:
            return
        for idx in self._journal_leg_shares():
            if idx not in self._share_start and self._run_side_on_leg(idx) is not None:
                self._share_held_since[idx] = since_ms

    # --- order observations -----------------------------------------------------

    def _observe_exit_leg_push(
            self, entry: dict, coid: str, leg_type: LegType,
            market: 'InstrumentInfo',
    ) -> None:
        """Judge one ``New`` / ``Untriggered`` push of an order-based exit leg.

        Judged at once: the push is the venue's word on this exact order.
        """
        if not coid or leg_type not in (LegType.TAKE_PROFIT, LegType.STOP_LOSS):
            return
        if not self._protection_guard().known(coid):
            return
        if self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                self._forget_protection(coid)
                return
        self._judge_order_levels(entry, coid, leg_type, market,
                                 read_started_at=None)

    def _observe_entry_push(
            self, entry: dict, coid: str, market: 'InstrumentInfo',
    ) -> None:
        """Judge one ``New`` / ``Untriggered`` push of a resting entry order."""
        if not coid or not self._protection_guard().known(coid):
            return
        if self.store_ctx is not None:
            row = self.store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                self._forget_protection(coid)
                return
        self._judge_order_levels(entry, coid, LegType.ENTRY, market,
                                 read_started_at=None)

    def _observe_open_order_levels(
            self, orders: list[dict], market: 'InstrumentInfo', *,
            read_started_at: float,
    ) -> None:
        """Judge the guarded orders an open-orders snapshot shows.

        The private ``order`` push is the fast path; this read catches an
        edit whose push was lost, and one that landed while the bot's own
        amend of the same order was in flight (the push is skipped then).
        A polled mismatch needs a second snapshot.

        A foreign partial TP/SL order awaiting its verdict that the snapshot
        no longer shows is gone (:meth:`_withdraw_pending_tpsl_order`).

        :param orders: The ``/v5/order/realtime`` rows of the symbol (a
            complete read).
        :param read_started_at: Wall-clock start of the read.
        """
        guard = self._protection_guard()
        shown = {str(entry.get('orderId') or '') for entry in orders}
        for order_id, (_entry, first_seen) in list(self._pending_tpsl_orders.items()):
            # A read that started before the order was seen may predate it.
            if order_id not in shown and read_started_at >= first_seen:
                self._withdraw_pending_tpsl_order(order_id, None)
        for entry in orders:
            coid = str(entry.get('orderLinkId') or '')
            if not coid or not guard.known(coid):
                if not coid:
                    self._observe_foreign_tpsl_order(entry, market)
                continue
            if self.store_ctx is not None:
                row = self.store_ctx.get_order(coid)
                if row is None or row.closed_ts_ms is not None:
                    continue
            _, _, leg_type = self._resolve_identity(
                coid, str(entry.get('orderId') or '') or None,
            )
            if leg_type not in (LegType.ENTRY, LegType.TAKE_PROFIT, LegType.STOP_LOSS):
                continue
            self._judge_order_levels(entry, coid, leg_type, market,
                                     read_started_at=read_started_at)

    def _observe_foreign_tpsl_order(self, entry: dict, market: 'InstrumentInfo') -> None:
        """Take in one view of a partial TP/SL order someone else placed.

        Bybit keeps a position's partial TP/SL (``tpslMode=Partial``) as
        conditional orders with a ``Partial*`` ``stopOrderType`` and no
        ``orderLinkId``; the bot writes every trading stop in ``Full`` mode,
        so such an order is always someone else's. Fed by every private
        ``order`` push of such an order and by the open-orders snapshot.

        A resting one on the side that reduces the run's share waits for its
        verdict (:meth:`_judge_tpsl_order`); one that rests no more before it
        is judged is withdrawn (:meth:`_withdraw_pending_tpsl_order`). An
        order judged already is judged again when a later view shows it
        amended: its trigger moved or its quantity grew
        (:meth:`_tpsl_order_amended`). A reported one is never judged again.
        The ``Full``-mode TP/SL orders the venue keeps for a trading stop are
        judged from the position rows instead (the trading-stop guard).
        """
        if (market.category == CATEGORY_SPOT or self.store_ctx is None
                or not self._adoption_baselined):
            return
        if str(entry.get('symbol') or market.symbol) != market.symbol:
            return
        if str(entry.get('stopOrderType') or '') not in _PARTIAL_TPSL_STOP_TYPES:
            return
        order_id = str(entry.get('orderId') or '')
        if not order_id:
            return
        status = str(entry.get('orderStatus') or '')
        if status not in _UNFILLED_ORDER_STATUSES:
            self._withdraw_pending_tpsl_order(order_id, status)
            return
        side = str(entry.get('side') or '').lower()
        if side not in ('buy', 'sell'):
            return
        judged = self._foreign_tpsl_orders_judged()
        if order_id in judged and not self._tpsl_order_amended(judged[order_id], entry,
                                                               market):
            return
        pending = self._pending_tpsl_orders.get(order_id)
        first_seen = pending[1] if pending is not None else epoch_time()
        self._pending_tpsl_orders[order_id] = (entry, first_seen)
        self._judge_tpsl_order(order_id, market)

    def _tpsl_order_amended(
            self, terms: tuple[float | None, float | None] | None, entry: dict,
            market: 'InstrumentInfo',
    ) -> bool:
        """Whether a judged foreign partial TP/SL order was amended since its verdict.

        Its trigger moved, or its quantity grew: either may make it cover the
        run's share, or change a protection that does. A smaller quantity
        covers less than the order judged before, and is what the venue
        itself may leave of it. A view that does not show a value (absent or
        unreadable) shows no change of it.

        :param terms: ``(quantity, trigger price)`` the last verdict judged;
            ``None`` for a reported order (never judged again).
        """
        if terms is None:
            return False
        qty, trigger = terms
        new_qty, new_trigger = self._tpsl_order_terms(entry)
        if new_trigger is not None and (
                trigger is None or abs(new_trigger - trigger) > market.tick_size / 2):
            return True
        return new_qty is not None and (
            qty is None or new_qty > qty + self._qty_tolerance(market))

    @staticmethod
    def _tpsl_order_terms(entry: dict) -> tuple[float | None, float | None]:
        """``(quantity, trigger price)`` of one view of a partial TP/SL order."""
        try:
            trigger = _venue_level(entry.get('triggerPrice'))
        except ValueError:
            trigger = None
        return _venue_qty(entry.get('qty')), trigger

    def _judge_pending_tpsl_orders(self, market: 'InstrumentInfo') -> None:
        """Judge the foreign partial TP/SL orders whose evidence may be in now.

        Called whenever a position view or an execution moved the leg
        ledgers, and on the reconcile cadence through :meth:`_ledger_advance`.
        """
        for order_id in list(self._pending_tpsl_orders):
            if order_id in self._pending_tpsl_orders:
                self._judge_tpsl_order(order_id, market)

    def _judge_tpsl_order(self, order_id: str, market: 'InstrumentInfo') -> None:
        """Judge one foreign partial TP/SL order once the evidence allows.

        - Reported when it was added to a position the run holds a share of
          (created after the run's share started, :meth:`_note_share_start`,
          or after the adoption baseline proved the share held across a
          restart, :meth:`_note_share_held_since`) and covers that share:
          its quantity exceeds the foreign exposure the leg holds beside
          it. It changed the bot's protection.
        - Logged with an audit event when it covers the foreign exposure
          only (another party protecting its own), or was there before the
          run's fill merged into the position.
        - An amend of an order judged before (:meth:`_tpsl_order_amended`) is
          judged like an order created when it was amended (its
          ``updatedTime``).
        - Logged as an error with an audit event when the evidence cannot
          tell: the share's start, the quantity or the creation time is not
          known, it was created before the adoption baseline of a share
          held across a restart, the foreign exposure beside the share is
          in doubt (:attr:`~.ledger.LegLedger.doubt`), or the evidence did
          not come in before :data:`_FOREIGN_TPSL_VERDICT_CEILING_S`.

        The foreign exposure is read from the leg's ledger
        (:meth:`_tpsl_order_evidence`), never from the newest view alone:
        the order push of a partial TP/SL attached to someone else's fill,
        or added beside the run's own fill, routinely arrives before the
        position view that shows the fill, and the run's own fills move the
        journal before their view. Until then the order waits, unjudged.
        """
        entry, first_seen = self._pending_tpsl_orders[order_id]
        side = str(entry.get('side') or '').lower()
        idx = self._closed_leg(side)
        held = self._run_side_on_leg(idx)
        if held is None or held == side or self.store_ctx is None:
            # It reduces nothing the run holds.
            del self._pending_tpsl_orders[order_id]
            return
        now = epoch_time()
        qty, trigger = self._tpsl_order_terms(entry)
        amended = order_id in self._foreign_tpsl_orders_judged()
        created = _parse_ms(entry.get('updatedTime' if amended else 'createdTime'))
        start = self._share_start.get(idx)
        since = self._share_held_since.get(idx) if start is None else None
        tolerance = self._qty_tolerance(market)
        room: float | None = None
        doubt: float | None = None
        if qty is None or created is None or (start is None and since is None):
            verdict, level = 'unproven', logging.ERROR
        elif start is not None and created < start[1]:
            verdict, level = 'preexisting', logging.WARNING
        elif since is not None and created < since:
            verdict, level = 'unproven', logging.ERROR
        else:
            evidence = self._tpsl_order_evidence(idx, held, first_seen, now)
            if evidence is None:
                if now - first_seen < _FOREIGN_TPSL_VERDICT_CEILING_S:
                    return
                verdict, level = 'unproven', logging.ERROR
            else:
                room, doubt = evidence
                if qty <= room + tolerance:
                    verdict, level = 'foreign_only', logging.INFO
                elif qty > room + doubt + tolerance:
                    verdict, level = 'reported', logging.INFO
                else:
                    verdict, level = 'unproven', logging.ERROR
        del self._pending_tpsl_orders[order_id]
        self._foreign_tpsl_orders_judged()[order_id] = (
            None if verdict == 'reported' else (qty, trigger))
        stop_type = str(entry.get('stopOrderType') or '')
        context = {
            'order_id': order_id, 'stop_order_type': stop_type, 'side': side,
            'position_idx': idx, 'qty': qty, 'foreign_exposure': room,
            'foreign_exposure_in_doubt': doubt, 'created_ms': created,
            'amended': amended,
            'share_start_ms': start[1] if start else None, 'share_held_since_ms': since,
            'trigger_price': trigger,
        }
        self.store_ctx.log_event(FOREIGN_TPSL_ORDER_EVENT, exchange_order_id=order_id,
                                 payload={'verdict': verdict, **context})
        if verdict != 'reported':
            logger.log(
                level,
                "Bybit %s order %s (%s %s) on position %s holding this run's share: %s",
                stop_type, order_id, side, qty, idx,
                {'unproven': "whether it covers the run's share or was added or "
                             "amended on it is not known; not reported",
                 'preexisting': "it was there before the run's fill merged into the "
                                "position; not reported",
                 'foreign_only': "it covers only the foreign exposure beside the "
                                 "run's share"}[verdict],
            )
            return
        self.report_external_intervention(
            f"partial {stop_type} order {order_id} {'amended' if amended else 'added'} "
            f"on the venue by someone else on the bot position on {market.symbol}: "
            f"{side} {qty} covers the run's share ({room} foreign beside it)",
            context, exchange_order_id=order_id, changed_at_ms=created,
        )

    def _tpsl_order_evidence(
            self, idx: int, held: str, first_seen: float, now: float,
    ) -> tuple[float, float] | None:
        """The foreign exposure leg ``idx`` holds beside the run's share, once proven.

        Read from the leg ledger's cursor: the leg and the run's share at the
        newest view, which the known executions explain. Only when that
        evidence covers the order's creation:

        - the private stream's last outage is re-read
          (:attr:`_ledger_stream_opened_ms`) and no view of the leg waits
          for executions (:attr:`~.ledger.LegLedger.open`);
        - no execution is known beyond that view: an own fill the journal
          booked, or a foreign fill, whose view has not arrived yet;
        - a view of the leg was taken after the order was first seen
          (:attr:`_leg_view_at`), and :attr:`fill_report_grace_s` passed
          since: a fill the order was attached to has reached the views and
          the execution stream by then, even when a push older than the
          order was delivered after it.

        :param held: The side of the run's share on the leg.
        :param first_seen: Wall-clock time the order was first seen.
        :return: ``(foreign exposure, share in doubt)`` (magnitudes);
            ``None`` while the evidence is not in.
        """
        if self._ledger_stream_opened_ms is not None:
            return None
        grace = self.fill_report_grace_s
        if grace is not None and now - first_seen < grace:
            return None
        view_at = self._leg_view_at.get(idx)
        if view_at is None or view_at <= first_seen:
            return None
        ledger = self._leg_ledgers.get(idx)
        if ledger is None or ledger.cursor_seq is None or ledger.open:
            return None
        cursor = ledger.cursor_seq
        if any(event.seq > cursor for event in ledger.events.values()):
            return None
        direction = 1.0 if self._hedge() or held == 'buy' else -1.0
        leg = max(ledger.cursor_leg * direction, 0.0)
        share = max(ledger.share * direction, 0.0)
        return max(0.0, leg - share), ledger.doubt

    def _withdraw_pending_tpsl_order(self, order_id: str, status: str | None) -> None:
        """Drop a foreign partial TP/SL order that rests no more before its verdict.

        One that fired is the position ledger's to judge: its executions
        carry the ``Partial*`` ``stopOrderType`` (:meth:`_external_origin`).
        One cancelled, deactivated or rejected (or missing from a complete
        open-orders snapshot) was withdrawn before the evidence could tell
        whether it covered the run's share: logged as an error with an
        audit event, never reported.

        :param status: The ``orderStatus`` of the push; ``None`` when a
            complete snapshot no longer shows the order.
        """
        pending = self._pending_tpsl_orders.pop(order_id, None)
        if pending is None:
            return
        entry, _first_seen = pending
        if status in _FIRED_ORDER_STATUSES:
            logger.info(
                "Bybit %s order %s fired before its verdict; the position ledger "
                "judges its executions", entry.get('stopOrderType'), order_id,
            )
            return
        qty, trigger = self._tpsl_order_terms(entry)
        self._foreign_tpsl_orders_judged()[order_id] = (qty, trigger)
        side = str(entry.get('side') or '').lower()
        logger.error(
            "Bybit %s order %s (%s %s) on a position holding this run's share was "
            "withdrawn (%s) before the evidence could tell whether it covered the "
            "run's share; not reported", entry.get('stopOrderType'), order_id, side,
            entry.get('qty'), status or 'gone from the open orders',
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                FOREIGN_TPSL_ORDER_EVENT, exchange_order_id=order_id,
                payload={'verdict': 'unproven', 'order_id': order_id,
                         'stop_order_type': str(entry.get('stopOrderType') or ''),
                         'side': side, 'qty': qty, 'trigger_price': trigger,
                         'created_ms': _parse_ms(entry.get('createdTime')),
                         'withdrawn': status or 'gone'},
            )

    def _foreign_tpsl_orders_judged(self) -> dict[str, tuple[float | None, float | None] | None]:
        """The foreign TP/SL orders judged already, by every process instance of the run.

        :return: Venue order id -> ``(quantity, trigger price)`` the newest
            verdict judged; ``None`` once reported.
        """
        judged = self._foreign_tpsl_judged
        if judged is None:
            judged = {}
            if self.store_ctx is not None:
                for _ik, _coid, eoid, payload in \
                        self.store_ctx.iter_events_by_kind_for_run_id(FOREIGN_TPSL_ORDER_EVENT):
                    if not eoid or judged.get(eoid, ()) is None:
                        continue
                    if payload.get('verdict') == 'reported':
                        judged[eoid] = None
                        continue
                    judged[eoid] = self._tpsl_order_terms(
                        {'qty': payload.get('qty'), 'triggerPrice': payload.get('trigger_price')})
            self._foreign_tpsl_judged = judged
        return judged

    def _judge_order_levels(
            self, entry: dict, coid: str, leg_type: LegType,
            market: 'InstrumentInfo', *, read_started_at: float | None,
    ) -> None:
        """Judge one venue view of a guarded order: its levels, then its quantity.

        :param read_started_at: Wall-clock start of a snapshot read; ``None``
            for a push (judged at once).
        """
        status = str(entry.get('orderStatus') or '')
        if coid not in self._qty_rebaselined and self._protection_rebaseline_flagged(coid):
            # Read before the guard judges the levels: that judgement takes
            # the flag down, and the quantity follows the same resume.
            self._qty_rebaseline_pending.add(coid)
        levels = self._order_view_levels(entry, coid, leg_type, status)
        if levels is not None:
            reported = self._protection_guard().observe(
                coid, levels, tick=market.tick_size,
                read_started_at=read_started_at,
                immediate=read_started_at is None,
            )
            if reported:
                return
        if status in _UNFILLED_ORDER_STATUSES:
            self._judge_order_qty(entry, coid, leg_type, market,
                                  read_started_at=read_started_at)

    def _order_view_levels(
            self, entry: dict, coid: str, leg_type: LegType, status: str,
    ) -> ProtectionLevels | None:
        """The levels one venue view of a guarded order carries.

        An exit leg carries its limit (TP) or trigger (SL). A limit entry
        carries its limit price; a stop entry its trigger, read only while
        the order is still untriggered. ``None`` when the view does not show
        the level (or shows an unreadable one).
        """
        if leg_type is LegType.ENTRY:
            row = self.store_ctx.get_order(coid) if self.store_ctx is not None else None
            order_type = str((row.extras or {}).get('order_type') or '') if row else ''
            if order_type == 'stop':
                if status != 'Untriggered':
                    return None
                field = 'triggerPrice'
            else:
                field = 'price'
        else:
            field = 'price' if leg_type is LegType.TAKE_PROFIT else 'triggerPrice'
        try:
            price = _venue_level(entry.get(field))
        except ValueError:
            logger.warning("Bybit order %s view carries an unreadable %s: %r",
                           coid, field, entry.get(field))
            return None
        if price is None:
            return None
        if leg_type is LegType.ENTRY:
            return (ProtectionLevels(stop=price) if field == 'triggerPrice'
                    else ProtectionLevels(limit=price))
        return exit_leg_levels(leg_type, price)

    def _judge_order_qty(
            self, entry: dict, coid: str, leg_type: LegType,
            market: 'InstrumentInfo', *, read_started_at: float | None,
    ) -> None:
        """Report a guarded order whose quantity someone else amended.

        Every quantity the bot sent for the order is its own, and so is the
        journal row's (venue domain). An order that grew past all of them
        was amended by someone else: the venue never enlarges an order. So
        was a resting spot exit leg showing any other quantity (an
        untriggered conditional, a limit on the book).
        A derivative order cut below them may be the venue's own doing: it
        trims a reduce-only leg to the position left to reduce, and it
        reduces other active orders of the contract when a close-on-trigger
        order fires without the balance to execute (Bybit's
        ``closeOnTrigger`` documentation). The order view alone cannot tell
        that from a foreign amend: a cut is never reported
        (:meth:`_log_order_qty_cut`).
        A push is judged at once; a snapshot needs a second one. The first
        view judged after an operator resume is accepted instead: the
        operator took the venue as it stands, and only the quantities the
        bot sends from then on are its own besides it.
        """
        if coid in self._qty_reported or not self._view_postdates_own_writes(
                coid, read_started_at):
            return
        venue = _venue_qty(entry.get('qty'))
        if venue is None:
            return
        if coid in self._qty_rebaseline_pending:
            self._qty_rebaseline_pending.discard(coid)
            self._qty_rebaselined.add(coid)
            self._qty_mismatches.pop(coid, None)
            sent = list(self._own_order_qtys.get(coid, ()))
            self._write_own_order_qtys(coid, lambda _saved: [venue, *sent])
            return
        own = self._own_order_qty_candidates(coid)
        if not own:
            return
        tolerance = market.qty_step / 2 if market.qty_step > 0 else _FILL_EPS
        resting_spot_leg = market.category == CATEGORY_SPOT and (
            str(entry.get('orderStatus') or '') == 'Untriggered'
            or str(entry.get('orderType') or '') == 'Limit')
        changed = all(abs(venue - qty) > tolerance for qty in own)
        cut = changed and not resting_spot_leg and venue <= max(own) + tolerance
        if not changed or (cut and read_started_at is None):
            # A cut waits for the snapshot: the position view that explains
            # a reduce-only trim may still be in flight behind the push.
            if not changed:
                self._qty_mismatches.pop(coid, None)
            return
        if read_started_at is not None:
            seen = self._qty_mismatches.get(coid)
            count = seen[1] + 1 if seen is not None and abs(seen[0] - venue) <= tolerance else 1
            self._qty_mismatches[coid] = (venue, count)
            if count < 2:
                return
        if cut:
            self._qty_mismatches.pop(coid, None)
            self._log_order_qty_cut(entry, coid, leg_type, venue, max(own), market)
            return
        self._qty_reported.add(coid)
        self._qty_mismatches.pop(coid, None)
        self.report_external_intervention(
            f"bot order {coid!r} quantity changed on the venue by someone "
            f"else: expected {max(own)}, found {venue}",
            {'client_order_id': coid, 'expected_qty': max(own),
             'observed_qty': venue, 'leg_type': leg_type.value},
            client_order_id=coid,
        )

    def _log_order_qty_cut(
            self, entry: dict, coid: str, leg_type: LegType, venue: float,
            expected: float, market: 'InstrumentInfo',
    ) -> None:
        """Log a derivative order the venue shows cut below every own quantity.

        An exit leg that still covers what its position holds, up to the
        bot's own quantity, is what a reduce-only trim leaves: logged
        quietly. Anything else is logged as an error with a durable audit
        event, once per quantity: the bot's order no longer is what the bot
        sent, and nothing proves who changed it. Never reported.
        """
        tolerance = self._qty_tolerance(market)
        logged = self._qty_cut_logged.get(coid)
        if logged is not None and abs(logged - venue) <= tolerance:
            return
        self._qty_cut_logged[coid] = venue
        position: float | None = None
        if leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS):
            position = self._protected_position(str(entry.get('side') or '').lower())
            if position is not None and venue >= min(position, expected) - tolerance:
                logger.info(
                    "Bybit exit leg %s quantity cut from %s to %s; it still covers "
                    "the %s position: a reduce-only trim", coid, expected, venue,
                    position,
                )
                return
        logger.error(
            "Bybit order %s quantity was cut on the venue from %s to %s and "
            "nothing explains it (the venue reduces orders itself, so it is "
            "not reported as an intervention)", coid, expected, venue,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'order_qty_cut_unattributed', client_order_id=coid,
                exchange_order_id=str(entry.get('orderId') or '') or None,
                payload={'leg_type': leg_type.value, 'expected_qty': expected,
                         'observed_qty': venue, 'position': position},
            )

    def _protected_position(self, exit_side: str) -> float | None:
        """The position an exit order on ``exit_side`` reduces, by its newest view.

        ``None`` when no view of the leg is known.
        """
        if exit_side not in ('buy', 'sell'):
            return None
        ledger = self._leg_ledgers.get(self._closed_leg(exit_side))
        size = ledger.latest_size if ledger is not None else None
        if size is None:
            return None
        if not self._hedge() and exit_side == 'buy':
            size = -size
        return max(size, 0.0)

    async def _observe_resume_baseline(self, market: 'InstrumentInfo') -> None:
        """Take the venue as the operator left it right after a resume.

        An operator resume makes the next observation of every guarded key
        its new baseline. Waiting for the next push would let an edit made
        in between become that baseline; a snapshot read at stream start
        closes the window. A no-op unless a resume flagged a persisted
        protection; a failed read leaves the push / reconcile path to set
        the baseline.
        """
        if self.store_ctx is None:
            return
        flagged = False
        for row in self.store_ctx.iter_live_orders():
            saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA)
            if isinstance(saved, dict) and saved.get('rebaseline'):
                flagged = True
                break
        if not flagged:
            return
        read_started_at = epoch_time()
        try:
            orders, conclusive = await self._open_order_rows(market)
            if conclusive:
                self._observe_open_order_levels(orders, market,
                                                read_started_at=read_started_at)
            if market.category != CATEGORY_SPOT:
                rows = await self._fetch_position_rows(market)
                self._observe_position_rows(rows, market,
                                            read_started_at=read_started_at)
        except BybitError as exc:
            logger.warning(
                "Bybit resume baseline snapshot failed (the next view sets "
                "it): %s", exc, exc_info=traceback_wanted(exc),
            )

    # --- trading-stop observations ----------------------------------------------

    def _observe_position_rows(
            self, rows: list[dict], market: 'InstrumentInfo', *,
            read_started_at: float | None,
    ) -> None:
        """Judge the trading stops a position push or snapshot shows.

        A flat leg, or a one-way position that flipped side, lost its trading
        stop to the venue: the key is forgotten, never judged — unless the
        view may predate the bot's own write of the key (in flight, or a
        snapshot read that started before it settled). A row older (by
        ``seq``) than a view of the same leg already seen is a delayed push,
        and one older than the own execution that started the run's share
        shows the leg before the run held anything of it: neither forgets
        nor is judged. The first view of a share the bot attached its stop
        to before that view arrived shows the venue before the attach
        (:meth:`_precede_own_trading_stop`). A push is not specific to one
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
            if self._position_view_superseded(idx, row):
                continue
            self._leg_last_view[idx] = row
            self._note_leg_view(idx, row, read_started_at)
            if self._view_predates_share(idx, row):
                continue
            first_view = idx in self._share_unviewed
            self._share_unviewed.discard(idx)
            if guard.known(key):
                if size <= 0.0:
                    if self._view_postdates_own_writes(key, read_started_at):
                        self._forget_protection(key)
                    continue
                side = self._trading_stop_side(key, idx)
                if side is None:
                    continue
                if str(row.get('side') or '').lower() == side:
                    venue = self._trading_stop_view(idx, row)
                    if venue is not None:
                        if first_view:
                            self._precede_own_trading_stop(idx, key, venue, row, market)
                        guard.observe(key, venue, tick=market.tick_size,
                                      read_started_at=read_started_at)
                    self._home_trading_stop(key, idx)
                    continue
                if not self._view_postdates_own_writes(key, read_started_at):
                    continue
                self._forget_protection(key)
            if size > 0.0:
                self._baseline_bare_trading_stop(idx, key, row)
        self._judge_pending_tpsl_orders(market)

    def _view_predates_share(self, idx: int, row: dict) -> bool:
        """Whether position ``row`` is older than the run's current share on leg ``idx``.

        By ``seq``, against the own execution that started the share
        (:attr:`_share_start`): such a view shows the leg before the run
        held anything of it. ``False`` when either is not known.
        """
        start = self._share_start.get(idx)
        seq = _parse_seq(row.get('seq'))
        return start is not None and seq is not None and seq < start[0]

    def _precede_own_trading_stop(
            self, idx: int, key: str, venue: ProtectionLevels, row: dict,
            market: 'InstrumentInfo',
    ) -> None:
        """Take the first view of a new share as the venue before the bot's own attach.

        The own execution that started the run's share arrived before any
        view of the position it built, and the bot attached its trading
        stop before that view arrived (:attr:`_share_unviewed`). The venue
        pushes the position the fill built as soon as it fills, so the view
        may show the position before the attach landed: the bot's own older
        state, ranked below its writes (:meth:`ProtectionLevelGuard.precede`)
        and acceptable until the venue is seen carrying the attach. Bare is
        the venue's own state of a position the fill opened. A stop the bot
        did not write may have been on the position before the run's fill
        merged into it or been put there since: it is logged as an error with
        an audit event, as :meth:`_baseline_bare_trading_stop` does.
        """
        if not self._protection_guard().precede(key, venue, tick=market.tick_size):
            return
        if venue == ProtectionLevels():
            return
        seq = _parse_seq(row.get('seq'))
        side = str(row.get('side') or '').lower()
        logger.error(
            "Bybit position %s carries a trading stop (%s) the bot never set, seen "
            "first with the run's share on it after the bot's own attach: whether it "
            "was there before the attach or added since is not known; accepting it "
            "until the venue shows the attach",
            idx, venue,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                TRADING_STOP_ORIGIN_UNPROVEN_EVENT,
                payload={'position_idx': idx, 'seq': seq, 'side': side,
                         'levels': venue.as_list()},
            )

    def _note_leg_view(self, idx: int, row: dict, read_started_at: float | None) -> None:
        """Remember when the newest view of leg ``idx`` the ledger holds was taken.

        :param read_started_at: Wall-clock start of a REST snapshot read
            (its rows may show the venue as of then); ``None`` for a push.
        """
        seq = _parse_seq(row.get('seq'))
        ledger = self._leg_ledgers.get(idx)
        if seq is None or ledger is None or ledger.latest_view_seq != seq:
            return
        self._leg_view_at[idx] = read_started_at if read_started_at is not None else epoch_time()

    @staticmethod
    def _trading_stop_view(idx: int, row: dict) -> ProtectionLevels | None:
        """The trading stop one position row shows; ``None`` when unreadable."""
        try:
            return trading_stop_levels(
                _venue_level(row.get('takeProfit')),
                _venue_level(row.get('stopLoss')),
                _venue_level(row.get('trailingStop')),
            )
        except ValueError:
            logger.warning(
                "Bybit position %s row carries an unreadable TP/SL field: %r", idx, row,
            )
            return None

    def _baseline_bare_trading_stop(self, idx: int, key: str, row: dict) -> None:
        """Start guarding the trading stop of a run position the bot left without one.

        The bot's protection state covers a position it set no trading stop
        on (or cleared its own from): bare is its choice, and a TP / SL /
        trailing stop someone else adds to that position changes it. The
        first view of the position that postdates the run's share
        (:meth:`_note_share_start`) sets the baseline: bare, every later stop
        the bot did not write itself is judged like an edit of its own
        (two observations). A view that already shows a stop cannot tell
        whether it was there before the run's fill merged into the position
        or was added since: it is logged as an error with an audit event and
        becomes the baseline, so a later change of it is judged. The bot's
        own later writes go through ``expect`` / ``settle`` as usual.
        """
        if not self._adoption_baselined:
            # The journal does not hold the run's share yet.
            return
        side = str(row.get('side') or '').lower()
        if side not in ('buy', 'sell') or self._run_side_on_leg(idx) != side:
            return
        start = self._share_start.get(idx)
        seq = _parse_seq(row.get('seq'))
        if start is not None and (seq is None or seq < start[0]):
            return
        venue = self._trading_stop_view(idx, row)
        if venue is None:
            return
        if venue != ProtectionLevels():
            logger.error(
                "Bybit position %s carries a trading stop (%s) the bot never set, "
                "seen first with the run's share on it: whether it was there before "
                "the run's fill or added since is not known; guarding it from now on",
                idx, venue,
            )
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    TRADING_STOP_ORIGIN_UNPROVEN_EVENT,
                    payload={'position_idx': idx, 'seq': seq, 'side': side,
                             'levels': venue.as_list()},
                )
        self._protection_guard().adopt(key, venue,
                                       row_coid=self._bare_baseline_row_coid(side, key))
        self._protected_sides[key] = side

    def _guard_cleared_trading_stop(self, key: str, idx: int) -> None:
        """Keep guarding a trading stop the bot cleared while the run holds the position.

        The position is bare by the bot's own write now: a stop someone else
        adds to it is an edit of the bot's protection. The bare levels move
        to one of the run's entry rows (the cleared stop's ownership row is
        released, :meth:`_bare_baseline_row_coid`), so they survive a
        restart. Without a share of the run on the leg there is nothing left
        to guard.
        """
        side = self._run_side_on_leg(idx)
        if side is None:
            self._forget_protection(key)
            return
        guard = self._protection_guard()
        write = guard.expect(key, ProtectionLevels(),
                             row_coid=self._bare_baseline_row_coid(side, key))
        guard.settle(key, ProtectionLevels(), write=write)
        self._protected_sides[key] = side

    # --- share ledger: views and executions -------------------------------------

    def _leg_ledger(self, idx: int) -> LegLedger:
        """The ledger of leg ``idx``."""
        ledger = self._leg_ledgers.get(idx)
        if ledger is None:
            ledger = LegLedger()
            self._leg_ledgers[idx] = ledger
        return ledger

    @staticmethod
    def _qty_tolerance(market: 'InstrumentInfo') -> float:
        """Half a quantity step: two quantities closer than that are equal."""
        return market.qty_step / 2 if market.qty_step > 0 else _FILL_EPS

    def _hedge(self) -> bool:
        return self._position_mode == POSITION_MODE_HEDGE

    def _closed_leg(self, side: str) -> int:
        """The leg a reducing execution on ``side`` reduces."""
        if not self._hedge():
            return _ONE_WAY_IDX
        return HEDGE_IDX_BUY if side == 'sell' else HEDGE_IDX_SELL

    def _opened_leg(self, side: str) -> int:
        """The leg an opening execution on ``side`` grows."""
        if not self._hedge():
            return _ONE_WAY_IDX
        return HEDGE_IDX_BUY if side == 'buy' else HEDGE_IDX_SELL

    def _record_position_views(self, rows: list[dict]) -> None:
        """Enter every sequenced position row into its leg's ledger.

        Called for every position push and REST read, before the flat sweep
        can retire the rows of a position someone else closed. A hedge leg
        is entered by its size, the one-way leg by its signed net. A row
        without a ``seq`` cannot be ordered against an execution and is
        skipped; one without a readable ``updatedTime`` is never taken for
        an echo of the newest view.
        """
        for row in rows:
            seq = _parse_seq(row.get('seq'))
            if seq is None:
                continue
            try:
                idx = self._position_row_index(row)
                size = self._position_row_size(row)
            except BybitPositionDataError:
                continue
            try:
                updated_ms: int | None = self._position_row_updated_ms(row) or None
            except BybitPositionDataError:
                updated_ms = None
            if idx == _ONE_WAY_IDX and size > 0.0:
                side = str(row.get('side') or '').lower()
                if side not in ('buy', 'sell'):
                    continue
                size = size if side == 'buy' else -size
            self._leg_ledger(idx).add_view(seq, size, updated_ms)

    def _position_view_superseded(self, idx: int, row: dict) -> bool:
        """Whether a newer view of leg ``idx`` than ``row`` was already seen.

        The views are recorded before the rows are judged, so the row's own
        view is in the history; a strictly larger ``seq`` there means the
        row arrived late. A row without a ``seq`` cannot be ordered and is
        never superseded.
        """
        seq = _parse_seq(row.get('seq'))
        ledger = self._leg_ledgers.get(idx)
        return seq is not None and ledger is not None and ledger.latest_view_seq > seq

    def _journal_leg_shares(self) -> dict[int, float]:
        """The run's journal share per leg (hedge: per side; one-way: signed)."""
        if self.store_ctx is None:
            return {}
        if self._hedge():
            buy = sell = 0.0
            for row in self._owned_entry_rows():
                if row.side == 'buy':
                    buy += float(row.filled_qty)
                else:
                    sell += float(row.filled_qty)
            return {HEDGE_IDX_BUY: buy, HEDGE_IDX_SELL: sell}
        return {_ONE_WAY_IDX: self._run_owned_signed_size()}

    def _ledger_ready(self, market: 'InstrumentInfo') -> bool:
        """Whether the ledger may record and judge on this run."""
        return market.category != CATEGORY_SPOT and self.store_ctx is not None

    def _take_ledger_exec_id(self, exec_id: str) -> bool:
        """Claim ``exec_id`` for the ledger; ``False`` when it was entered already."""
        if exec_id in self._ledger_exec_ids:
            return False
        self._ledger_exec_ids[exec_id] = None
        if len(self._ledger_exec_ids) > _MAX_LEDGER_EXEC_IDS:
            del self._ledger_exec_ids[next(iter(self._ledger_exec_ids))]
        return True

    def _note_ledger_seq(self, entry: dict, market: 'InstrumentInfo') -> None:
        """Remember the ``seq`` of one execution of the symbol the ledgers know.

        A position view stamped with it may close an interval: the last
        transaction before the view is known (:meth:`LegLedger.advance`).
        Every leg shares the record — a view may carry the ``seq`` of an
        execution on the other hedge leg.
        """
        if not self._ledger_ready(market):
            return
        seq = _parse_seq(entry.get('seq'))
        if seq is None or seq in self._ledger_exec_seqs:
            return
        self._ledger_exec_seqs[seq] = None
        if len(self._ledger_exec_seqs) > _MAX_LEDGER_EXEC_IDS:
            del self._ledger_exec_seqs[next(iter(self._ledger_exec_seqs))]

    @contextmanager
    def _ledger_batch(self, market: 'InstrumentInfo') -> Iterator[None]:
        """Enter a batch of executions before the ledgers judge any of it.

        One push or one backfill window is entered whole: judged one
        execution at a time, a view the rest of the batch explains could
        close an interval while executions that cancel each other out are
        still to come. The ledgers judge once, when the batch is in.
        """
        self._ledger_hold += 1
        try:
            yield
        finally:
            self._ledger_hold -= 1
        self._ledger_advance(market)

    def _note_own_execution(
            self, entry: dict, market: 'InstrumentInfo', *,
            reducing: bool, share: dict[int, float], credit: float = 0.0,
    ) -> None:
        """Enter one execution this run booked into the ledger.

        :param reducing: The execution reduced the run's position (an exit
            leg, a close, a fired own trading stop); else it opened.
        :param share: The change the booking made to the journal share, per
            leg.
        :param credit: The part of the reduce the booking did not net from
            the journal, because a give-up had netted it ahead
            (:meth:`_ledger_reduce_credit`, :meth:`_ledger_entry_credit`).
        """
        if not self._ledger_ready(market):
            return
        exec_id = str(entry.get('execId') or '')
        if not exec_id or not self._take_ledger_exec_id(exec_id):
            return
        side = str(entry.get('side') or '').lower()
        try:
            exec_qty = float(entry.get('execQty') or 0.0)
        except (TypeError, ValueError):
            return
        if side not in ('buy', 'sell') or exec_qty <= 0.0:
            return
        if self._hedge():
            idx = self._closed_leg(side) if reducing else self._opened_leg(side)
            deltas = {idx: -exec_qty if reducing else exec_qty}
        else:
            deltas = {_ONE_WAY_IDX: exec_qty if side == 'buy' else -exec_qty}
        seq = _parse_seq(entry.get('seq'))
        self._note_ledger_seq(entry, market)
        tolerance = self._qty_tolerance(market)
        for idx in set(deltas) | {key for key, value in share.items() if value}:
            ledger = self._leg_ledger(idx)
            if seq is None:
                if ledger.started:
                    logger.warning(
                        "Bybit own execution %s carries no seq; the position "
                        "ledger cannot place it", exec_id,
                    )
                ledger.share += share.get(idx, 0.0)
                ledger.journal += share.get(idx, 0.0)
                continue
            event = LegEvent(exec_id, seq, deltas.get(idx, 0.0), own=True,
                             share=share.get(idx, 0.0),
                             credit=credit if idx in deltas else 0.0, entry=entry)
            if not ledger.add_event(event):
                gone = ledger.late_own(event, tolerance)
                if gone is not None:
                    # The journal books the venue truth before the next
                    # judgement re-reads it.
                    self._persist_ledger_credits(idx, ledger.credits)
                    self._sync_leg_journal(idx, ledger, tolerance)
                    self._ledger_apply(idx, [gone], market)
        self._ledger_advance(market)

    def _external_origin(self, entry: dict, *, ambiguous: bool) -> tuple[str, str]:
        """Classify an execution no own order explains: ``(origin, reason)``.

        A fired ``Full``-mode position TP/SL or trailing stop that no live
        ownership row explains (:meth:`_attribute_venue_bracket_execution`)
        is the bot's own only if the run ever owned a trading stop on that
        position (:meth:`_run_ever_owned_trading_stop`): its row may have been
        released while the stop still stood. A stop the position already
        carried when the run's share was first seen on it may have been
        there before the run's fill merged into the position
        (:meth:`_trading_stop_origin_unproven`): the venue executing it
        changed nothing of the bot's. Otherwise the bot never set the
        protection that fired: someone else did, and a close of the run's
        share by it is reported like a manual close.
        """
        if ambiguous:
            return ORIGIN_AMBIGUOUS, "the bracket-ownership attribution was ambiguous"
        exec_type = str(entry.get('execType') or 'Trade')
        if exec_type in VENUE_EXEC_TYPES:
            return ORIGIN_VENUE, f"execType {exec_type!r} is a venue mechanism"
        if exec_type in USER_EXEC_TYPES:
            return ORIGIN_USER, f"execType {exec_type!r} is a user transaction"
        link_id = str(entry.get('orderLinkId') or '')
        if link_id and _looks_like_pyne_link_id(link_id):
            return ORIGIN_UNPROVEN, (f"orderLinkId {link_id!r} has the shape of a "
                                     f"bot client id this run does not know")
        stop_type = str(entry.get('stopOrderType') or '')
        if stop_type in _PARTIAL_TPSL_STOP_TYPES:
            return ORIGIN_USER, (f"a position {stop_type} fired; the bot never "
                                 f"places a Partial stop")
        if stop_type in _POSITION_TPSL_STOP_TYPES:
            side = str(entry.get('side') or '').lower()
            leg_id = str(self._closed_leg(side))
            if self._run_ever_owned_trading_stop(leg_id):
                return ORIGIN_UNPROVEN, (
                    f"a position {stop_type} no live bracket of this run explains "
                    f"fired; the run set a trading stop on position {leg_id} before")
            if self._trading_stop_origin_unproven(self._closed_leg(side)):
                return ORIGIN_UNPROVEN, (
                    f"a position {stop_type} fired on position {leg_id}, which carried "
                    f"a trading stop of unproven origin when the run's share joined it")
            return ORIGIN_USER, (f"a position {stop_type} fired on position {leg_id}, "
                                 f"where this run never set a trading stop")
        create_type = str(entry.get('createType') or '')
        if not create_type:
            return ORIGIN_UNPROVEN, "no createType"
        if create_type in MANUAL_CLOSE_CREATE_TYPES:
            return ORIGIN_USER, f"createType {create_type!r} is a user order"
        if create_type in _VENUE_CREATE_TYPES:
            return ORIGIN_VENUE, f"createType {create_type!r} is a venue mechanism"
        return ORIGIN_UNPROVEN, f"unknown createType {create_type!r}"

    def _run_ever_owned_trading_stop(self, leg_id: str) -> bool:
        """Whether any process instance of this run ever owned a trading stop on ``leg_id``.

        Every own trading stop (the one-way trailing attach, the hedge
        bracket replication) is journalled as a bracket-ownership row before
        its write leaves, and a released row stays in the journal, closed,
        until the retention purge. Without a journal nothing proves the run
        never set one.
        """
        if self.store_ctx is None:
            return True
        return any(
            str((row.extras or {}).get(EXTRAS_KEY_BRACKET_OWN_LEG_ID)) == leg_id
            for row in self.store_ctx.iter_orders_in_state_for_run_id(STATE_BRACKET_OWN)
        )

    def _trading_stop_origin_unproven(self, idx: int) -> bool:
        """Whether leg ``idx`` carried a stop of unproven origin under the run's share.

        :meth:`_baseline_bare_trading_stop` journals a
        :data:`TRADING_STOP_ORIGIN_UNPROVEN_EVENT` when the first view of the
        run's share on the leg already showed a stop the bot never set. Only
        the current share counts: an event from a view older than the own
        execution that started it (:attr:`_share_start`) belongs to an
        earlier share. When that start is not known (a restart), every
        event of the leg counts.
        """
        if self.store_ctx is None:
            return True
        start = self._share_start.get(idx)
        for _ik, _coid, _eoid, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(
                    TRADING_STOP_ORIGIN_UNPROVEN_EVENT):
            if payload.get('position_idx') != idx:
                continue
            seq = _parse_seq(payload.get('seq'))
            if start is None or seq is None or seq >= start[0]:
                return True
        return False

    def _note_external_execution(
            self, entry: dict, market: 'InstrumentInfo', *, ambiguous: bool,
    ) -> None:
        """Enter one execution no own order or own trading stop explains.

        Entered once per ``execId``: the live push and the backfill overlap
        re-read both deliver every execution. Judged once the ledger can
        prove the interval it falls into (:meth:`_ledger_advance`).

        :param ambiguous: The bracket-ownership attribution refused to pick
            between several own rows — the execution may be the bot's own.
        """
        if not self._ledger_ready(market):
            return
        exec_id = str(entry.get('execId') or '')
        if not exec_id or exec_id in self._ledger_exec_ids:
            return
        side = str(entry.get('side') or '').lower()
        try:
            exec_qty = float(entry.get('execQty') or 0.0)
            closed = float(entry.get('closedSize') or 0.0)
        except (TypeError, ValueError):
            logger.warning("Bybit external execution %s is unreadable: %r", exec_id, entry)
            return
        if side not in ('buy', 'sell') or exec_qty <= 0.0:
            return
        seq = _parse_seq(entry.get('seq'))
        if seq is None:
            # Not claimed: a later copy of the same execution that carries
            # its seq (the REST backfill) can still prove the interval.
            logger.warning(
                "Bybit external execution %s carries no seq; it cannot be "
                "placed against the position views", exec_id,
            )
            return
        self._take_ledger_exec_id(exec_id)
        origin, reason = self._external_origin(entry, ambiguous=ambiguous)
        if self._hedge():
            deltas: dict[int, float] = {}
            if closed > _FILL_EPS:
                deltas[self._closed_leg(side)] = -min(closed, exec_qty)
            if exec_qty - closed > _FILL_EPS:
                deltas[self._opened_leg(side)] = exec_qty - max(closed, 0.0)
        else:
            deltas = {_ONE_WAY_IDX: exec_qty if side == 'buy' else -exec_qty}
        self._note_ledger_seq(entry, market)
        payload_entry = {**entry, '_origin_reason': reason}
        for idx, delta in deltas.items():
            event = LegEvent(exec_id, seq, delta, own=False, origin=origin,
                             entry=payload_entry)
            ledger = self._leg_ledger(idx)
            if not ledger.add_event(event):
                if ledger.anchor_seq is not None and seq <= ledger.anchor_seq:
                    # Older than the view the ledger started from: the
                    # journal it started with already accounts for it.
                    continue
                if ledger.late_foreign(event):
                    # Its interval was given up: it is judged when the late
                    # executions complete that interval.
                    continue
                # Its interval was judged without it: whatever it did is
                # already in the views, but it cannot be judged any more.
                self._ledger_apply(idx, [LegOutcome(
                    event=event, consumed=0.0, proven=False,
                    share_before=abs(ledger.share), leg_after=ledger.cursor_leg,
                    seq=seq,
                )], market)
        self._ledger_advance(market)

    def _note_non_trade_execution(self, entry: dict, market: 'InstrumentInfo') -> None:
        """Enter one execution whose ``execType`` is not ``Trade``.

        Shared by the live ``execution`` push and the REST backfill. Such an
        execution carries no order fill of the run: nothing is booked. A
        size-changing one (:data:`VENUE_EXEC_TYPES`, :data:`USER_EXEC_TYPES`)
        enters the position ledger like any foreign execution — it takes
        the run's share like a foreign close, and the ledger must see it to
        prove the intervals around it. The others (funding, dividends) are
        transactions of the symbol too: a position view may carry their
        ``seq``.
        """
        exec_type = str(entry.get('execType') or 'Trade')
        if exec_type in VENUE_EXEC_TYPES or exec_type in USER_EXEC_TYPES:
            self._note_external_execution(entry, market, ambiguous=False)
        else:
            self._note_ledger_seq(entry, market)

    def _ledger_advance(self, market: 'InstrumentInfo') -> None:
        """Judge whatever the ledgers can now prove.

        Waits for the startup adoption baseline: before it commits, the
        journal does not yet hold the run's share. Inside an execution batch
        (:meth:`_ledger_batch`) it waits for the batch to end. While the
        private stream's last outage is not re-read from the execution
        history yet (:attr:`_ledger_stream_opened_ms`), nothing is proven.
        """
        if (not self._ledger_ready(market) or not self._adoption_baselined
                or self._ledger_hold):
            return
        tolerance = self._qty_tolerance(market)
        now = epoch_time()
        # One read serves every leg: netting one hedge side leaves the other
        # side's share untouched.
        journal = self._journal_leg_shares()
        for idx in sorted(self._leg_ledgers):
            ledger = self._leg_ledgers[idx]
            credits_before = [list(credit) for credit in ledger.credits]
            outcomes: list[LegOutcome] = []
            if not ledger.started:
                clamp = ledger.start(journal.get(idx, 0.0), tolerance)
                if not ledger.started:
                    continue
                if clamp is not None:
                    # The inverse mirror was seeded from the venue: it holds
                    # the view the share is clamped to already.
                    self._sync_leg_journal(idx, ledger, tolerance, mirror=False)
                    outcomes.append(clamp)
            else:
                ledger.sync_journal(journal.get(idx, 0.0), tolerance)
            outcomes.extend(ledger.advance(
                now, self.fill_report_grace_s, tolerance, self._ledger_exec_seqs,
                provable=self._ledger_stream_opened_ms is None,
            ))
            if ledger.credits != credits_before:
                self._persist_ledger_credits(idx, ledger.credits)
            if outcomes:
                # The journal books the venue truth before anything is
                # reported: a halting policy raises.
                self._sync_leg_journal(idx, ledger, tolerance)
                self._ledger_apply(idx, outcomes, market)
        self._judge_pending_tpsl_orders(market)

    def _sync_leg_journal(
            self, idx: int, ledger: LegLedger, tolerance: float, *, mirror: bool = True,
    ) -> None:
        """Net the run's journal share on leg ``idx`` down to the ledger's.

        The ledger's journal share at its cursor (net of what its verdicts
        took: a foreign or venue close of the run's share, a clamp to the
        venue view), plus the share changes of the own executions booked
        beyond it, is what the journal must hold. More is exposure the leg
        no longer holds for the run: it is netted off, FIFO. Less is never
        raised — the flat sweep, or an own fill booked meanwhile, already
        moved the journal. Foreign exposure an own fill consumed is not
        netted (:class:`~.ledger.LegLedger`): the run's share there is
        clipped to the leg, its journal is not.

        A one-way journal holds entry rows of both sides once a folded
        reversal is booked (the reversal's closing part is never netted
        off the old side), so its signed total cannot tell which side a
        verdict took: a verdict on the old side booked after the reversal
        flipped the total's sign would never be netted. Each verdict is
        netted off the side it took (:attr:`~.ledger.LegLedger.to_net`),
        never past the ledger's journal share.

        :param mirror: Whether the inverse net-position mirror follows the
            netting (:meth:`_net_side_share`); ``False`` when the mirror
            already holds the venue view the verdicts book.
        """
        taken = ledger.take_to_net()
        if self.store_ctx is None:
            return
        target = ledger.journal + sum(
            event.share for event in ledger.events.values() if event.own)
        journal = self._journal_leg_shares().get(idx, 0.0)
        if idx == HEDGE_IDX_BUY or idx == HEDGE_IDX_SELL:
            side = 'buy' if idx == HEDGE_IDX_BUY else 'sell'
            if journal > target + tolerance:
                self._net_side_share(side, journal - max(target, 0.0), mirror=mirror)
            return
        long_cut = min(taken.get(1.0, 0.0),
                       max(0.0, journal - target + taken.get(-1.0, 0.0)))
        if long_cut > tolerance:
            self._net_side_share('buy', long_cut, mirror=mirror)
            journal = self._journal_leg_shares().get(idx, 0.0)
        short_cut = min(taken.get(-1.0, 0.0), max(0.0, target - journal))
        if short_cut > tolerance:
            self._net_side_share('sell', short_cut, mirror=mirror)
            journal = self._journal_leg_shares().get(idx, 0.0)
        if journal > max(target, 0.0) + tolerance:
            self._net_side_share('buy', journal - max(target, 0.0), mirror=mirror)
        elif journal < min(target, 0.0) - tolerance:
            self._net_side_share('sell', min(target, 0.0) - journal, mirror=mirror)

    def _ledger_reduce_credit(
            self, entry: dict, side: str, qty: float, market: 'InstrumentInfo',
    ) -> float:
        """The part of a late own reduce a ledger clamp already netted.

        An own reducing fill older than an interval the ledger gave up, or
        than the view it started from, was already booked there as venue
        truth: the journal must not net it a second time.
        """
        if not self._ledger_ready(market):
            return 0.0
        seq = _parse_seq(entry.get('seq'))
        ledger = self._leg_ledgers.get(self._closed_leg(side))
        if seq is None or ledger is None or not ledger.started:
            return 0.0
        assert ledger.cursor_seq is not None
        if seq > ledger.cursor_seq:
            return 0.0
        taken = ledger.take_credit(seq, qty, self._reduced_direction(side),
                                   self._qty_tolerance(market))
        if taken > 0.0:
            self._persist_ledger_credits(self._closed_leg(side), ledger.credits)
        return taken

    def _ledger_reduce_credit_open(
            self, entry: dict, side: str, market: 'InstrumentInfo',
    ) -> float:
        """The run's share a give-up clamp netted ahead of a late own reduce.

        What :meth:`_ledger_reduce_credit` could take for the execution,
        read without taking it: the part of the run's share the execution
        may have closed although the journal no longer holds it.
        """
        if not self._ledger_ready(market):
            return 0.0
        seq = _parse_seq(entry.get('seq'))
        ledger = self._leg_ledgers.get(self._closed_leg(side))
        if seq is None or ledger is None or ledger.cursor_seq is None \
                or seq > ledger.cursor_seq:
            return 0.0
        return ledger.open_credit(seq, self._reduced_direction(side),
                                  self._qty_tolerance(market))

    def _ledger_entry_credit_due(self, entry: dict, market: 'InstrumentInfo') -> bool:
        """Whether a one-way entry execution may meet a give-up credit.

        Only an execution older than the ledger's cursor, on a leg holding
        credits, can: the cheap test before the journal reads of
        :meth:`_ledger_entry_credit`.
        """
        if not self._ledger_ready(market) or self._hedge():
            return False
        seq = _parse_seq(entry.get('seq'))
        ledger = self._leg_ledgers.get(_ONE_WAY_IDX)
        return (seq is not None and ledger is not None and ledger.cursor_seq is not None
                and seq <= ledger.cursor_seq and bool(ledger.credits))

    def _ledger_entry_credit(
            self, entry: dict, side: str, qty: float, journal_before: float,
            market: 'InstrumentInfo',
    ) -> float:
        """Undo the part of a late one-way entry a give-up clamp already netted.

        A one-way entry against the run's position reduces it first (a
        folded reversal), and a give-up that could not see the execution may
        have netted that reduce from the journal already. Its booking added
        the whole quantity to its own side; the part that reduced is netted
        back off that side for each credit it takes:

        * the part within the journal's opposite side (booked as before);
        * beyond it, only what leaves the journal holding more of the entry's
          side than the leg does: the journal side the give-up netted may
          also have been taken by someone else before the entry, and then
          the whole entry opened exposure the leg does hold.

        Called right after the entry's booking (:meth:`_fill_event`).

        :param journal_before: The run's signed journal before the booking.
        :return: The quantity netted back off (the credit taken).
        """
        ledger = self._leg_ledgers.get(_ONE_WAY_IDX)
        seq = _parse_seq(entry.get('seq'))
        if ledger is None or seq is None or side not in ('buy', 'sell'):
            return 0.0
        tolerance = self._qty_tolerance(market)
        reduced = self._reduced_direction(side)
        journal = self._run_owned_signed_size()
        # What the booking added to the entry's side (nothing without a row).
        booked = min(qty, max(0.0, (journal_before - journal) * reduced))
        within = min(booked, max(0.0, journal_before * reduced))
        taken = ledger.take_credit(seq, within, reduced, tolerance) if within > 0.0 else 0.0
        journal += taken * reduced
        beyond = min(booked - within,
                     max(0.0, -journal * reduced - max(-ledger.cursor_leg * reduced, 0.0)))
        if beyond > tolerance:
            taken += ledger.take_credit(seq, beyond, reduced, tolerance)
        if taken <= 0.0:
            return 0.0
        self._persist_ledger_credits(_ONE_WAY_IDX, ledger.credits)
        self._net_side_share(side, taken)
        return taken

    def _reduced_direction(self, side: str) -> float:
        """The side of the share an execution on ``side`` reduces (leg frame sign).

        A hedge leg's share is its unsigned size; the one-way share is the
        signed net, which a sell reduces when long and a buy when short.
        """
        if self._hedge() or side == 'sell':
            return 1.0
        return -1.0

    def _persist_ledger_credits(self, idx: int, leg_credits: list[list[float]]) -> None:
        """Journal the credits of leg ``idx`` (the newest record wins).

        A credit is journal netting a give-up did ahead of an own reduce that
        may still be booked; it must survive a restart, whose adoption
        baseline books such a reduce (:meth:`_take_persisted_ledger_credits`).
        """
        if self.store_ctx is None:
            return
        self.store_ctx.log_event(LEDGER_CREDITS_EVENT, payload={
            'position_idx': idx,
            'credits': [[int(seq), quantity, direction]
                        for seq, quantity, direction in leg_credits],
        })

    def _load_persisted_ledger_credits(self) -> dict[int, list[list[float]]]:
        """The credits each leg's ledger left behind, across process instances."""
        by_leg: dict[int, list[list[float]]] = {}
        if self.store_ctx is None:
            return by_leg
        for _ik, _coid, _eoid, payload in \
                self.store_ctx.iter_events_by_kind_for_run_id(LEDGER_CREDITS_EVENT):
            idx = payload.get('position_idx')
            saved = payload.get('credits')
            if not isinstance(idx, int) or not isinstance(saved, list):
                continue
            loaded: list[list[float]] = []
            for item in saved:
                if (isinstance(item, list) and len(item) == 3
                        and all(isinstance(value, (int, float)) for value in item)):
                    loaded.append([float(item[0]), float(item[1]), float(item[2])])
            by_leg[idx] = loaded
        return by_leg

    def _take_persisted_ledger_credits(
            self, reduces: list[tuple[str, float, list[dict]]], tolerance: float,
    ) -> list[tuple[str, float]]:
        """Take the credits an earlier process left from reduces it never booked.

        A give-up of an earlier process netted the journal ahead of an own
        reduce it could not see; the adoption baseline now books that very
        reduce from the order's executions. Each unbooked execution at or
        before a credit's ``seq`` takes the credit instead of netting the
        journal a second time. The credits left are void afterwards: every
        own execution up to the baseline is booked now.

        :param reduces: Per exit / close row: its side, the quantity it
            executed beyond its booked cursor, and its execution rows.
        :param tolerance: Quantity tolerance.
        :return: Per row: its side and the quantity still to net.
        """
        by_leg = self._load_persisted_ledger_credits()
        left_behind = [idx for idx, saved in by_leg.items() if saved]
        result: list[tuple[str, float]] = []
        for side, quantity, executions in reduces:
            idx = self._closed_leg(side)
            ledger = LegLedger(credits=by_leg.get(idx, []))
            remaining = quantity
            taken = 0.0
            # The unbooked part of an order is its newest executions.
            for entry in sorted(executions, key=execution_sort_key, reverse=True):
                if remaining <= tolerance:
                    break
                exec_qty = _venue_qty(entry.get('execQty')) or 0.0
                part = min(exec_qty, remaining)
                seq = _parse_seq(entry.get('seq'))
                if seq is not None:
                    taken += ledger.take_credit(seq, part, self._reduced_direction(side),
                                                tolerance)
                remaining -= part
            by_leg[idx] = ledger.credits
            result.append((side, quantity - taken))
        for idx in left_behind:
            self._persist_ledger_credits(idx, [])
        return result

    def _sweep_waits_for_ledger(self) -> bool:
        """Whether the one-way flat sweep must wait for the position ledger.

        The one-way sweep judges flatness from the venue view alone. A flat
        view whose executions the ledger does not know yet may be the run's
        own close: retiring the run's entry rows before that execution is
        booked would make its booking move no share, and the ledger would
        still charge the run's share to a later foreign close of foreign
        exposure. Hedge mode judges flatness from the journal itself.
        """
        if self._hedge():
            return False
        ledger = self._leg_ledgers.get(_ONE_WAY_IDX)
        return ledger is not None and ledger.open

    # --- share ledger: verdicts ---------------------------------------------------

    def _ledger_apply(
            self, idx: int, outcomes: list[LegOutcome], market: 'InstrumentInfo',
    ) -> None:
        """Log and report what the ledger decided for leg ``idx``.

        The reports come last, since a halting policy raises.
        """
        reports: list[tuple[LegEvent, LegOutcome]] = []
        for outcome in outcomes:
            event = outcome.event
            if event is None:
                self._log_ledger_clamp(idx, outcome, market)
                continue
            if event.own:
                if outcome.consumed > 0.0:
                    self._log_late_own_fill_gone(idx, event, outcome, market)
                else:
                    self._log_own_fill_clip(idx, event, outcome, market)
                continue
            entry = event.entry or {}
            try:
                closed = float(entry.get('closedSize') or 0.0)
            except (TypeError, ValueError):
                closed = 0.0
            reason = str(entry.get('_origin_reason') or '')
            if outcome.settled:
                self._log_settled_execution(idx, event, outcome, closed, reason)
                continue
            if not outcome.proven:
                if outcome.consumed > 0.0 or (
                        event.origin == ORIGIN_USER and closed > 0.0
                        and outcome.share_before > 0.0):
                    self._log_external_close(
                        logging.ERROR, 'external_close_unproven', idx, event, outcome,
                        "the position views around it could not be reconciled "
                        f"with the executions ({reason})",
                    )
                continue
            if outcome.consumed > 0.0:
                if event.origin == ORIGIN_USER:
                    if outcome.doubtful >= outcome.consumed:
                        self._log_external_close(
                            logging.ERROR, 'external_close_unproven', idx, event,
                            outcome, "the share it took may have been closed "
                            f"already by an own execution the stream lost ({reason})",
                        )
                    else:
                        reports.append((event, outcome))
                    continue
                if event.origin == ORIGIN_UNPROVEN:
                    self._log_external_close(
                        logging.ERROR, 'external_close_unproven', idx, event, outcome,
                        reason,
                    )
                else:
                    self._log_external_close(
                        logging.WARNING, 'own_share_clamped', idx, event, outcome,
                        reason,
                    )
            elif event.origin == ORIGIN_USER and closed > 0.0:
                self._log_external_close(
                    logging.INFO, 'external_close_foreign_only', idx, event, outcome,
                    "it closed only foreign exposure",
                )
        for event, outcome in reports:
            self._report_external_close(idx, event, outcome, market)

    def _net_side_share(self, side: str, quantity: float, *, mirror: bool = True) -> None:
        """Net ``quantity`` of the run's journal share off its ``side`` entry rows.

        Consumes the run's own entry rows of that side FIFO, exactly like an
        own reducing fill (:meth:`_net_entry_row`); startup-adopted rows are
        not the run's and stay untouched.

        :param mirror: Whether the inverse net-position mirror follows the
            journal (:meth:`_net_inverse_mirror`).
        """
        if self.store_ctx is None or quantity <= 0.0:
            return
        remaining = quantity
        for row in sorted((row for row in self._owned_entry_rows() if row.side == side),
                          key=lambda item: item.created_ts_ms):
            if remaining <= _FILL_EPS:
                break
            consumed = min(remaining, float(row.filled_qty))
            self._net_entry_row(row, consumed)
            remaining -= consumed
        if mirror:
            self._net_inverse_mirror(side, quantity - max(remaining, 0.0))
        # The run may hold nothing of the position any more: no protection
        # of the bot's is left on it, whatever stays on the leg.
        self._forget_trading_stop_if_flat('sell' if side == 'buy' else 'buy')

    def _log_settled_execution(
            self, idx: int, event: LegEvent, outcome: LegOutcome, closed: float,
            reason: str,
    ) -> None:
        """Log the late verdict on one foreign execution of a given-up interval.

        The interval's executions turned out complete after all, and it was
        walked again (:class:`~.ledger.LegGap`). The give-up booked the
        share already, so nothing is booked or reported here: a foreign
        execution that took part of the run's share keeps the share in
        doubt, and is logged like an unproven close (a venue mechanism as a
        warning).
        """
        if outcome.consumed > 0.0:
            venue = event.origin in (ORIGIN_VENUE, ORIGIN_AMBIGUOUS)
            self._log_external_close(
                logging.WARNING if venue else logging.ERROR,
                'own_share_clamped' if venue else 'external_close_unproven',
                idx, event, outcome,
                "its interval was given up before its executions were complete; "
                f"judged afterwards ({reason})",
            )
        elif event.origin == ORIGIN_USER and closed > 0.0:
            self._log_external_close(
                logging.INFO, 'external_close_foreign_only', idx, event, outcome,
                "it closed only foreign exposure (judged after its interval was "
                "given up)",
            )

    def _log_ledger_clamp(
            self, idx: int, outcome: LegOutcome, market: 'InstrumentInfo',
    ) -> None:
        """Log a share clamp or an interval the ledger had to give up."""
        if self.store_ctx is None:
            return
        tolerance = self._qty_tolerance(market)
        if (outcome.consumed <= 0.0 and abs(outcome.unexplained) <= tolerance
                and outcome.doubtful <= 0.0):
            return
        payload = {
            'position_idx': idx, 'seq': outcome.seq,
            'share_before': outcome.share_before, 'clamped': outcome.consumed,
            'leg': outcome.leg_after, 'unexplained': outcome.unexplained,
            'doubt': outcome.doubtful,
        }
        if outcome.consumed > 0.0:
            logger.error(
                "Bybit position %s on %s holds %s for this run, below its "
                "journal share %s, and no execution explains it: booking the "
                "venue truth, %s of what is left in doubt", idx, market.symbol,
                outcome.leg_after, outcome.share_before, outcome.doubtful,
            )
            self.store_ctx.log_event('own_share_clamped', payload=payload)
            return
        logger.warning(
            "Bybit position %s on %s changed by %s that no known execution "
            "explains, or the executions of the change are incomplete; "
            "continuing from the venue view with %s of this run's share in "
            "doubt (a close of it is not reported)", idx, market.symbol,
            outcome.unexplained, outcome.doubtful,
        )
        self.store_ctx.log_event('position_ledger_gap', payload=payload)

    def _log_own_fill_clip(
            self, idx: int, event: LegEvent, outcome: LegOutcome,
            market: 'InstrumentInfo',
    ) -> None:
        """Log an own fill that consumed foreign exposure merged into the leg.

        The run's journal holds what its own fills built; the leg holds less
        of the run's side, because the fill netted out someone else's
        exposure (a one-way reversal over a foreign same-direction add). No
        one changed the run's things: never reported, and the journal is not
        netted for it.
        """
        entry = event.entry or {}
        order_id = str(entry.get('orderId') or '') or None
        logger.warning(
            "Bybit own execution %s (order %s) on position %s of %s consumed %s "
            "of foreign exposure merged into the leg: the leg holds %s of this "
            "run's side, its journal holds more", event.exec_id, order_id, idx,
            market.symbol, outcome.clipped, outcome.leg_after,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'own_fill_consumed_foreign_exposure', exchange_order_id=order_id,
                payload={'exec_id': event.exec_id, 'seq': event.seq, 'position_idx': idx,
                         'clipped': outcome.clipped, 'owned': outcome.share_before,
                         'physical_after': outcome.leg_after,
                         'unproven': not outcome.proven},
            )

    def _log_late_own_fill_gone(
            self, idx: int, event: LegEvent, outcome: LegOutcome,
            market: 'InstrumentInfo',
    ) -> None:
        """Log the part of a late own fill's share the leg no longer holds.

        The fill arrived after the steps it preceded were judged without it
        (:meth:`~.ledger.LegLedger.late_own`): a reduction among them took
        what it built, or an own reduce not booked yet did. Which one the
        evidence cannot tell, so nothing is reported; the journal booked the
        venue truth.
        """
        entry = event.entry or {}
        order_id = str(entry.get('orderId') or '') or None
        logger.error(
            "Bybit own execution %s (order %s) on position %s of %s arrived after "
            "the position steps it preceded were judged: %s of the %s it built "
            "for this run is no longer on the leg (taken by a reduction since, or "
            "by an own reduce not booked yet); booking the venue truth",
            event.exec_id, order_id, idx, market.symbol, outcome.consumed,
            outcome.share_before,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'own_share_clamped', exchange_order_id=order_id,
                payload={'exec_id': event.exec_id, 'seq': event.seq, 'position_idx': idx,
                         'share_before': outcome.share_before, 'clamped': outcome.consumed,
                         'leg': outcome.leg_after, 'cause': 'late_own_fill'},
            )

    def _log_external_close(
            self, level: int, kind: str, idx: int, event: LegEvent,
            outcome: LegOutcome, reason: str,
    ) -> None:
        """Log one judged foreign execution with a durable audit event."""
        entry = event.entry or {}
        order_id = str(entry.get('orderId') or '') or None
        logger.log(
            level,
            "Bybit execution %s (order %s, %s) on position %s took %s of this "
            "run's %s: %s", event.exec_id, order_id,
            entry.get('createType') or entry.get('execType'), idx,
            outcome.consumed, outcome.share_before, reason,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(kind, exchange_order_id=order_id, payload={
                'exec_id': event.exec_id, 'seq': event.seq, 'position_idx': idx,
                'origin': event.origin, 'reason': reason,
                'create_type': str(entry.get('createType') or ''),
                'consumed': outcome.consumed, 'unproven': outcome.doubtful,
                'owned': outcome.share_before, 'physical_after': outcome.leg_after,
            })

    def _report_external_close(
            self, idx: int, event: LegEvent, outcome: LegOutcome,
            market: 'InstrumentInfo',
    ) -> None:
        """Report a user-side execution that took the leg below the run's share."""
        entry = event.entry or {}
        order_id = str(entry.get('orderId') or '') or None
        key = order_id or event.exec_id
        if self.store_ctx is not None:
            self.store_ctx.log_event('own_share_clamped', exchange_order_id=order_id,
                                     payload={'exec_id': event.exec_id,
                                              'position_idx': idx,
                                              'consumed': outcome.consumed,
                                              'cause': 'external_intervention'})
        if key in self._reported_close_orders:
            return
        self._reported_close_orders.add(key)
        side = str(entry.get('side') or '').lower()
        create_type = str(entry.get('createType') or '')
        self.report_external_intervention(
            f"bot position on {market.symbol} reduced on the venue by someone "
            f"else: {side} {entry.get('execQty')} ({create_type}, order "
            f"{order_id}) left {outcome.leg_after} of the run's "
            f"{outcome.share_before}",
            {
                'exec_id': event.exec_id,
                'order_id': order_id,
                'create_type': create_type,
                'stop_order_type': str(entry.get('stopOrderType') or ''),
                'closed_size': entry.get('closedSize'),
                'side': side,
                'position_idx': idx,
                'physical_after': outcome.leg_after,
                'owned': outcome.share_before,
                'consumed': outcome.consumed - outcome.doubtful,
                'unproven': outcome.doubtful,
            },
            exchange_order_id=order_id,
        )

    def _owned_entry_rows(self) -> list:
        """The run's own live entry rows carrying exposure (not adopted)."""
        if self.store_ctx is None:
            return []
        return [
            row for row in self.store_ctx.iter_live_orders()
            if (row.extras or {}).get('kind') in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)
            and not (row.extras or {}).get(ADOPTED_STARTUP_EXTRA_KEY)
            and float(row.filled_qty) > _FILL_EPS
        ]
