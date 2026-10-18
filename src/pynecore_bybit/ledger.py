"""Per-leg position ledger behind the external-close verdict (derivatives).

A Bybit derivative leg (the one-way ``positionIdx=0`` net, or one aggregate
hedge leg) is shared: other runs, other bots and the operator trade the same
account. The run's own exposure on it — its *share* — is known from its own
fills; the rest is foreign. An execution no own order explains is an
intervention only when it took the leg BELOW the run's share; a close of
foreign exposure, a same-direction add, a trade on the other hedge leg never
is.

The ledger judges that from evidence, never from timing:

- **Every execution and every position view carries ``seq``.** Bybit stamps
  each fill of a symbol with one increasing cross sequence, and the position
  update a fill causes carries the fill's ``seq`` (executions concluded at
  the same time share one); an update without a fill (a setting, an order
  event) carries the ``seq`` of the last transaction on linear, but the
  symbol's current cross sequence on inverse — a ``seq`` no execution of
  the account carries. So a view at ``seq`` V shows the leg after every
  execution up to V, and the execution stamped V is the last one before it.
- **A view repeating the newest one is its echo.** Every fill stamps the
  position's ``updatedTime``, so a later view with the same size and
  ``updatedTime`` as the newest view shows the same state: no execution of
  the leg lies between them, whatever its ``seq``. It is not recorded, so
  the fill-less updates inverse stamps with a foreign ``seq`` (and a REST
  read, whose ``seq`` never names an execution) leave the interval to the
  view of the fill itself.
- **Complete intervals only.** From the cursor view C, the ledger advances to
  a later view V only when the evidence closes the interval: every view in
  ``(C, V]`` carries the ``seq`` of a known execution (the last transaction
  before it is known) and equals the leg the known executions predict at
  that ``seq``. A missing execution — an own fill the stream has not
  delivered yet, a foreign fill a WS outage dropped — leaves the interval
  open, and nothing in it is judged. A matching sum alone is not enough:
  missing executions that cancel each other out leave it intact, and a
  view stamped by one of them shows only that something is missing. What
  the two tests cannot see is such a pair lost between two known
  executions of the interval with no view between them. The plugin
  enters a whole batch of executions (one push, one backfill window) before
  the ledger judges any of it. That is the gate that keeps a late own fill
  from making a foreign close of foreign exposure look like a close of the
  run's share, and a late own add from hiding a foreign close of it.
  A stream outage loses views and executions alike, so the pair test
  cannot see what went missing in it: while the plugin has not re-read the
  outage window from the execution history, no interval is proven
  (``provable``).
- **The share follows the evidence.** Walking a complete interval in ``seq``
  order, own fills move the share by what their booking moved the journal;
  a foreign or venue execution that leaves the leg below the share consumes
  the difference. The consumed part is the verdict.
- **The share never exceeds the journal.** The run owns at most what its
  own journaled fills built (:attr:`LegLedger.journal`), on the same side.
  An own fill can consume foreign exposure merged into the leg: a one-way
  reversal over someone else's same-direction exposure nets it out, and
  the leg then holds less of the run's side than the journal says. The
  share keeps only what the leg holds (reported as a ``clipped`` outcome),
  and the journal is not netted for it: the foreign party's next trade may
  take its exposure back out, and the run's next own fill moves the share
  back towards the journal. Without the bound, a later reversal would carry
  the clipped offset across zero and leave the share above the journal,
  and the foreign party's cleanup of its own exposure would be charged to
  the run.
- **Give up loudly, never guess.** An interval that stays open past the
  grace is re-anchored on its first view: the executions in it are judged
  unprovable — a known foreign one takes nothing of the share, since the
  missing ones leave the leg around it unknown — and a share above that
  view is clamped to it (the venue truth). The clamp leaves a credit, so an
  own reducing fill that arrives even later does not net the journal a
  second time.
- **What a give-up cannot prove stays in doubt.** The executions missing
  from a given-up interval may include an own reduce that a missing foreign
  add offsets, so the leg's net change does not bound it, and the known
  foreign reductions in it took an unknown part of the share: the whole
  share left after the clamp is held as :attr:`LegLedger.doubt`. A later
  foreign reduction is charged to the run provably only below the rest of
  the share; what it takes of the doubt is booked like any other share it
  took, but never reported. A late own reduce resolves the part of the
  doubt it took; the share running out resolves all of it.
- **A given-up interval is judged again once complete.** It is kept
  (:class:`LegGap`) with the executions known in it; the late ones join
  it. When they close it by the same test as any other interval, it is
  walked again from its start: if no foreign execution in it took any of
  the run's share and no own fill was clipped, the give-up guessed nothing
  and its doubt is released. Otherwise the doubt stays (logged, never
  reported).
- **A late own fill keeps only what the steps after it left.** An own
  execution older than the cursor (its interval was given up, or the
  ledger started past it) arrives after the steps it preceded were walked
  without it, so a foreign or venue reduction among them may have taken
  what it built. The ledger keeps, per side, the least foreign exposure
  the leg held at any step since the view it last re-anchored on
  (:attr:`LegLedger.room`); the share a late fill builds is bounded by it,
  and the rest is booked gone (the venue truth) with a credit for an own
  reduce that may still arrive. Only a fill stamped with that view's
  ``seq`` is known to have reached it: any other late fill's kept part is
  held in doubt.

Leg frame: a hedge leg's size is its unsigned size; the one-way leg is the
signed net (buy positive). The share uses the same frame, so a one-way share
can be short. All quantities are wire-domain (base on linear, contracts on
inverse).
"""
from collections.abc import Container
from dataclasses import dataclass, field, replace

__all__ = [
    'ORIGIN_AMBIGUOUS',
    'ORIGIN_UNPROVEN',
    'ORIGIN_USER',
    'ORIGIN_VENUE',
    'LegEvent',
    'LegGap',
    'LegLedger',
    'LegOutcome',
]

#: A foreign execution placed by a user-side order type: reportable.
ORIGIN_USER = 'user'
#: Venue mechanics (liquidation, ADL, takeover, settlement, admin close):
#: never reported.
ORIGIN_VENUE = 'venue'
#: A position-stop fire the bracket attribution refused to split between
#: several own rows: possibly the bot's own, never reported.
ORIGIN_AMBIGUOUS = 'ambiguous'
#: An execution whose origin cannot be proven (no or unknown ``createType``,
#: a bot-shaped link id this run does not know, a position stop no live
#: bracket explains on a position the run once set a trading stop on):
#: logged as an error, never reported.
ORIGIN_UNPROVEN = 'unproven'

#: Views kept beyond the cursor; an interval that needs more stays open.
_MAX_VIEWS = 64

#: Events kept beyond the cursor.
_MAX_EVENTS = 512

#: Clamp credits kept per leg.
_MAX_CREDITS = 8

#: Given-up intervals kept per leg; the oldest ones beyond are merged into
#: one that never completes.
_MAX_GAPS = 16


@dataclass(slots=True)
class LegEvent:
    """One execution's effect on one leg.

    :ivar exec_id: The venue ``execId``.
    :ivar seq: The execution's cross sequence.
    :ivar delta: The leg change, in the leg frame.
    :ivar own: Whether this run booked the execution.
    :ivar share: For an own execution: the change its booking made to the
        run's journal share on the leg, in the leg frame.
    :ivar credit: For an own reducing execution: the part of it its booking
        did not net from the journal, because a give-up had netted it ahead
        (a credit it took; magnitude).
    :ivar origin: For a foreign execution: one of the ``ORIGIN_*`` values.
    :ivar entry: The raw execution row, for the report context.
    """
    exec_id: str
    seq: int
    delta: float
    own: bool
    share: float = 0.0
    credit: float = 0.0
    origin: str = ''
    entry: dict | None = None

    @property
    def own_share(self) -> float:
        """The journal change the own execution stands for, its credit included.

        A reduce booked against a credit moved the journal by less than it
        took from the run's side: the credited part was netted ahead.
        """
        if not self.credit:
            return self.share
        return self.share + (self.credit if self.delta > 0.0 else -self.credit)


@dataclass(slots=True)
class LegOutcome:
    """What one walk step decided.

    :ivar event: The foreign execution judged, the own execution whose share
        change the leg could not hold (``clipped``, or ``consumed`` for a
        late one), or ``None`` for a clamp of the share to a view at the end
        of an interval given up.
    :ivar consumed: The run's share this step took (leg frame magnitude).
        For a late own execution (:meth:`LegLedger.late_own`): the part of
        the share it built that the leg no longer holds, booked gone.
    :ivar proven: ``False`` inside an interval given up: the verdict rests
        on incomplete evidence.
    :ivar share_before: The run's share before the step (magnitude).
    :ivar leg_after: The leg after the step, in the bot's direction.
    :ivar seq: The step's ``seq`` (the execution's, or the view's).
    :ivar unexplained: For a give-up step: the leg change no known execution
        explains (leg frame).
    :ivar doubtful: The part of ``consumed`` the run may not have held any
        more: it was in :attr:`LegLedger.doubt` (a missing own reduce may
        already have taken it). Never reported. For a give-up step: the
        share it put in doubt.
    :ivar clipped: For an own execution: the part of the journal share it
        built that the leg does not hold (magnitude) — foreign exposure the
        run's own fill consumed. Never reported, never netted.
    :ivar settled: A verdict of a given-up interval judged again once its
        executions turned out complete (:class:`LegGap`): the share and the
        journal were booked by the give-up already. Never reported.
    """
    event: LegEvent | None
    consumed: float
    proven: bool
    share_before: float
    leg_after: float
    seq: int
    unexplained: float = 0.0
    doubtful: float = 0.0
    clipped: float = 0.0
    settled: bool = False


@dataclass(slots=True, eq=False)
class LegGap:
    """An interval the ledger gave up, kept until its executions are complete.

    :ivar start_seq: ``seq`` of the cursor view the interval started from;
        ``None`` for the view the ledger started on, whose past is not known
        (it never completes).
    :ivar end_seq: ``seq`` of the view the interval was given up on.
    :ivar start_leg: The leg at ``start_seq``.
    :ivar end_leg: The leg at ``end_seq``.
    :ivar start_share: The run's share at ``start_seq``.
    :ivar start_journal: The run's journal share at ``start_seq``.
    :ivar start_doubt: The doubt at ``start_seq``.
    :ivar events: Every execution of the interval known, by ``execId`` (the
        own ones by the journal change they stand for, credits included).
    :ivar late: The ``execId`` of the executions that arrived after the
        give-up.
    :ivar doubt: The most of :attr:`LegLedger.doubt` this give-up accounts
        for.
    :ivar settled: The interval turned out complete, and a foreign execution
        in it took part of the run's share (or an own fill was clipped): the
        give-up's doubt stays.
    """
    start_seq: int | None
    end_seq: int
    start_leg: float
    end_leg: float
    start_share: float
    start_journal: float
    start_doubt: float
    events: dict[str, LegEvent] = field(default_factory=dict)
    late: set[str] = field(default_factory=set)
    doubt: float = 0.0
    settled: bool = False

    def holds(self, seq: int) -> bool:
        """Whether an execution at ``seq`` falls into the interval."""
        if self.start_seq is None:
            return seq <= self.end_seq
        return self.start_seq < seq <= self.end_seq

    def complete(self, known_seqs: Container[int], tolerance: float) -> bool:
        """Whether the known executions now account for the interval.

        The same test an open interval is proven by: the execution the end
        view is stamped with is known, and the executions sum up to that
        view (no view lies inside a given-up interval).
        """
        if self.start_seq is None or self.end_seq not in known_seqs:
            return False
        total = self.start_leg + sum(event.delta for event in self.events.values())
        return abs(total - self.end_leg) <= tolerance


def _direction(share: float) -> float:
    """The sign of the run's side on the leg; ``0.0`` without a share."""
    if share > 0.0:
        return 1.0
    if share < 0.0:
        return -1.0
    return 0.0


@dataclass
class LegLedger:
    """The views, executions and run share of one leg."""
    views: list[tuple[int, float]] = field(default_factory=list)
    """Position views ``(seq, size)`` at or beyond the cursor, in ``seq`` order."""
    events: dict[str, LegEvent] = field(default_factory=dict)
    """Executions beyond the cursor, by ``execId``."""
    cursor_seq: int | None = None
    """``seq`` of the view everything up to which was judged."""
    anchor_seq: int | None = None
    """``seq`` of the view the ledger started from."""
    cursor_leg: float = 0.0
    """The leg at the cursor."""
    share: float = 0.0
    """The run's share at the cursor."""
    journal: float = 0.0
    """The run's journal share at the cursor: what its own booked fills built,
    net of what the ledger's verdicts take off. The share is bounded by it."""
    gap_since: float | None = None
    """Wall-clock time the interval beyond the cursor was first seen open."""
    credits: list[list[float]] = field(default_factory=list)
    """``[seq, quantity, direction]`` of each share reduction booked ahead of
    an own reduce at or before ``seq`` that may still arrive (a give-up
    clamp, a close of the doubt); ``direction`` is the side of the share it
    reduced (leg frame sign)."""
    latest_view_seq: int = 0
    """The newest view ``seq`` ever seen (a delayed view is older)."""
    newest_view: tuple[int, float, int] | None = None
    """``(seq, size, updatedTime)`` of the newest view recorded; ``None``
    before one, or when it carried no ``updatedTime``."""
    doubt: float = 0.0
    """The part of the share (magnitude) no evidence proves the run still holds."""
    doubt_seq: int = 0
    """``seq`` of the newest view the missing executions behind :attr:`doubt` precede."""
    blind_seq: int | None = None
    """``seq`` of the newest view the cursor moved onto without proving the
    interval before it: the view the ledger started on, or an interval given up."""
    room: dict[float, float] = field(default_factory=dict)
    """Per side of the leg (leg frame sign): the least foreign exposure the leg
    held on that side at any step walked since :attr:`blind_seq`, that view
    included — the most of what a late own execution built that can still be
    the run's (:meth:`late_own`)."""
    gaps: list[LegGap] = field(default_factory=list)
    """The given-up intervals whose doubt is still held, in ``seq`` order."""
    to_net: dict[float, float] = field(default_factory=dict)
    """Per side of the leg (leg frame sign): the journal share the verdicts took
    off that side that the run's entry rows still hold (:meth:`take_to_net`). A
    one-way journal holds rows of both sides after a folded reversal, so its
    signed total alone cannot tell which side a verdict took."""

    @property
    def started(self) -> bool:
        """Whether the ledger has a cursor to judge from."""
        return self.cursor_seq is not None

    @property
    def latest_size(self) -> float | None:
        """The leg in the newest view recorded, ``None`` before any view."""
        return self.views[-1][1] if self.views else None

    @property
    def open(self) -> bool:
        """Whether a view beyond the cursor waits for executions not known yet."""
        return self.cursor_seq is not None and any(
            view[0] > self.cursor_seq for view in self.views)

    def add_view(self, seq: int, size: float, updated_ms: int | None = None) -> None:
        """Record one position view of the leg.

        :param updated_ms: The view's ``updatedTime``; ``None`` when not
            known. A later view repeating the size and ``updatedTime`` of
            the newest view is its echo, and is not recorded.
        """
        self.latest_view_seq = max(self.latest_view_seq, seq)
        newest = self.newest_view
        if (newest is not None and updated_ms is not None and seq > newest[0]
                and size == newest[1] and updated_ms == newest[2]):
            return
        if newest is None or seq >= newest[0]:
            self.newest_view = None if updated_ms is None else (seq, size, updated_ms)
        if self.cursor_seq is not None and seq <= self.cursor_seq:
            return
        self.views = [view for view in self.views if view[0] != seq]
        self.views.append((seq, size))
        self.views.sort(key=lambda view: view[0])
        del self.views[:-_MAX_VIEWS]

    def add_event(self, event: LegEvent) -> bool:
        """Record one execution beyond the cursor.

        :return: ``False`` when the execution is not beyond the cursor (a
            late one): the caller handles it.
        """
        if self.cursor_seq is not None and event.seq <= self.cursor_seq:
            return False
        self.events[event.exec_id] = event
        if len(self.events) > _MAX_EVENTS:
            oldest = min(self.events.values(), key=lambda item: item.seq)
            del self.events[oldest.exec_id]
        return True

    def start(self, journal_share: float, tolerance: float) -> LegOutcome | None:
        """Anchor the cursor on the newest view.

        A journal share above what that view holds is clamped to it, like the
        end of an interval given up: the run cannot own more than the leg.
        The journal is behind the venue then, so an own reduce it has not
        booked yet may have taken any part of what is left: all of it is
        held in doubt.

        :param journal_share: The run's journal share on the leg now; the
            share changes of own executions recorded beyond the view are
            taken back out of it.
        :param tolerance: Quantity tolerance.
        :return: The clamp, when the journal share exceeded the view;
            ``None`` otherwise, and while no view is known (check
            :attr:`started`).
        """
        if not self.views:
            return None
        seq, size = self.views[-1]
        share = journal_share - self._own_beyond(seq)
        self.cursor_seq = seq
        self.anchor_seq = seq
        self.cursor_leg = size
        self.views = [self.views[-1]]
        self.events = {key: event for key, event in self.events.items() if event.seq > seq}
        self.share = self._capped(share, size, tolerance)
        self.journal = share
        self.doubt = 0.0
        self.gaps = []
        self.blind_seq = seq
        self.room = {}
        self._note_room(size, self.share)
        clamped = abs(share) - abs(self.share)
        if not _direction(share) or clamped <= tolerance:
            return None
        # The venue truth is booked: the journal is netted down to the view.
        self._take_journal(clamped, _direction(share))
        self._add_doubt(seq, abs(self.share), tolerance)
        self._add_credit(seq, clamped, _direction(share))
        if self.doubt:
            self._add_gap(LegGap(
                start_seq=None, end_seq=seq, start_leg=size, end_leg=size,
                start_share=self.share, start_journal=self.journal, start_doubt=0.0,
                doubt=self.doubt,
            ))
        return LegOutcome(
            event=None, consumed=clamped, proven=False, share_before=abs(share),
            leg_after=max(size * _direction(share), 0.0), seq=seq, doubtful=self.doubt,
        )

    def sync_journal(self, journal_share: float, tolerance: float) -> None:
        """Take the run's journal share as it stands now.

        The journal also moves outside the executions the ledger walks: the
        flat sweep retires the entry rows of a closed position, a netting
        lands after a verdict. The share never exceeds what the journal
        holds.

        :param journal_share: The run's journal share on the leg now, the
            share changes of the own executions recorded beyond the cursor
            included.
        :param tolerance: Quantity tolerance.
        """
        if self.cursor_seq is None:
            return
        self.journal = journal_share - self._own_beyond(self.cursor_seq)
        self.share = self._bounded(self.share, self.journal, tolerance)
        self._cap_doubt(tolerance)

    def late_own(self, event: LegEvent, tolerance: float) -> LegOutcome | None:
        """Apply the journal change of an own execution older than the cursor.

        The cursor's view already shows its leg change; only the share moves.
        What it takes off the run's side moves the share as booked; at or
        before :attr:`doubt_seq` it is the kind of execution the doubt waits
        for, and resolves as much of the doubt. What it builds was missing
        from the steps walked since, and a reduction among them may have
        taken it: the share keeps at most the :attr:`room` of that side, and
        the rest is booked gone — the journal is netted (the venue truth),
        with a credit for an own reduce older than the cursor that may be
        what took it. The kept part is proven only for an execution stamped
        with :attr:`blind_seq`: the view the ledger re-anchored on then shows
        it, and every step since is walked. Any other kept part is held in
        doubt, released only with the given-up interval it falls into when
        that interval ends on :attr:`blind_seq`. An execution of a given-up
        interval joins it (:class:`LegGap`).

        :return: The booking of the gone part, or ``None`` when nothing of
            what it built is gone.
        """
        assert self.cursor_seq is not None and self.blind_seq is not None
        before = self.share
        old_side = _direction(before)
        self.journal += event.share
        wanted = self._bounded(before + event.share, self.journal, tolerance)
        new_side = _direction(wanted)
        if new_side == old_side:
            shrunk = max(0.0, abs(before) - abs(wanted))
            grown = max(0.0, abs(wanted) - abs(before))
        else:
            shrunk = abs(before)
            grown = abs(wanted)
        share = before - shrunk * old_side
        if shrunk > 0.0 and event.seq <= self.blind_seq:
            # Every step since the view held that much less of the run's side.
            self.room[old_side] = self.room.get(old_side, 0.0) + shrunk
        gap = self._gap_of(event.seq)
        if gap is not None:
            gap.events[event.exec_id] = event
            gap.late.add(event.exec_id)
        if old_side and event.seq <= self.doubt_seq:
            resolved = max(0.0, -event.share * old_side)
            self.doubt = max(0.0, self.doubt - resolved)
            if gap is not None:
                gap.doubt = max(0.0, gap.doubt - resolved)
        kept = gone = 0.0
        if grown > 0.0:
            kept = min(grown, max(self.room.get(new_side, 0.0), 0.0))
            if grown - kept <= tolerance:
                kept = grown
            elif kept <= tolerance:
                kept = 0.0
            gone = grown - kept
            self.room[new_side] = self.room.get(new_side, 0.0) - kept
            share += kept * new_side
            if gone > 0.0:
                self._take_journal(gone, new_side)
                self._add_credit(self.cursor_seq, gone, new_side)
        self.share = share
        if kept > tolerance and event.seq != self.blind_seq:
            if (gap is not None and gap.start_seq is not None
                    and gap.end_seq == self.blind_seq):
                gap.doubt += kept
            else:
                # Steps nothing can prove again: the doubt is never released.
                self._add_gap(LegGap(
                    start_seq=None, end_seq=self.cursor_seq, start_leg=self.cursor_leg,
                    end_leg=self.cursor_leg, start_share=self.share,
                    start_journal=self.journal, start_doubt=self.doubt, doubt=kept,
                ))
            self._add_doubt(self.blind_seq, kept, tolerance)
        self._cap_doubt(tolerance)
        if gone <= 0.0:
            return None
        return LegOutcome(
            event=event, consumed=gone, proven=False, share_before=abs(wanted),
            leg_after=max(self.cursor_leg * new_side, 0.0), seq=event.seq,
        )

    def late_foreign(self, event: LegEvent) -> bool:
        """Enter a foreign execution older than the cursor into its given-up interval.

        :return: ``True`` when it fell into a given-up interval still kept:
            it is judged once that interval turns out complete. ``False``
            otherwise (its interval was proven without it, or its doubt is
            gone): the caller handles it.
        """
        gap = self._gap_of(event.seq)
        if gap is None or gap.start_seq is None:
            return False
        gap.events[event.exec_id] = event
        gap.late.add(event.exec_id)
        return True

    def take_to_net(self) -> dict[float, float]:
        """Hand over :attr:`to_net`: what the run's entry rows must still lose, per side.

        :return: Per side of the leg (leg frame sign), the journal share the
            verdicts took since the last call.
        """
        taken = self.to_net
        self.to_net = {}
        return taken

    def open_credit(self, seq: int, direction: float, tolerance: float) -> float:
        """The credit a late own reduce at ``seq`` could still take (nothing is taken).

        :param seq: The reducing execution's ``seq``.
        :param direction: The side of the share it reduces (leg frame sign).
        :param tolerance: Quantity tolerance.
        :return: The summed quantity of the credits :meth:`take_credit` would draw on.
        """
        return sum(credit[1] for credit in self.credits
                   if seq <= credit[0] and credit[1] > tolerance
                   and credit[2] == direction)

    def take_credit(
            self, seq: int, quantity: float, direction: float, tolerance: float,
    ) -> float:
        """Take the part of a late own reduce a give-up clamp already netted.

        :param seq: The reducing execution's ``seq``.
        :param quantity: Its quantity.
        :param direction: The side of the share it reduced (leg frame sign).
        :param tolerance: Quantity tolerance.
        :return: The quantity the journal must not net again.
        """
        taken = 0.0
        for credit in self.credits:
            if quantity - taken <= tolerance:
                break
            if seq > credit[0] or credit[1] <= tolerance or credit[2] != direction:
                continue
            part = min(credit[1], quantity - taken)
            credit[1] -= part
            taken += part
        self.credits = [credit for credit in self.credits if credit[1] > tolerance]
        return taken

    def advance(
            self, now: float, grace: float, tolerance: float,
            known_seqs: Container[int], *, provable: bool = True,
    ) -> list[LegOutcome]:
        """Judge every complete interval beyond the cursor.

        An interval closes on the furthest view that, like every view before
        it, carries the ``seq`` of a known execution and matches the known
        executions. A given-up interval its late executions completed is
        judged again first (:meth:`_settle_gaps`).

        :param now: Wall clock, seconds.
        :param grace: How long an open interval may wait for its missing
            executions before it is given up.
        :param tolerance: Quantity tolerance.
        :param known_seqs: The ``seq`` of every execution of the symbol
            known, on any leg (a view may carry the ``seq`` of an execution
            on the other hedge leg).
        :param provable: ``False`` while executions may be missing that the
            views cannot reveal (a stream outage not yet re-read): nothing
            is proven, every interval waits, and gives up past the grace.
        :return: The decisions, in walk order.
        """
        outcomes: list[LegOutcome] = []
        if self.cursor_seq is None:
            return outcomes
        if provable:
            outcomes.extend(self._settle_gaps(known_seqs, tolerance))
        while True:
            later = [view for view in self.views if view[0] > self.cursor_seq]
            if not later:
                self.gap_since = None
                return outcomes
            target: tuple[int, float] | None = None
            for view in (later if provable else ()):
                if (abs(self._expected_leg(view[0]) - view[1]) > tolerance
                        or view[0] not in known_seqs):
                    break
                target = view
            if target is not None:
                self.gap_since = None
                outcomes.extend(self._walk(target, proven=True, tolerance=tolerance))
                continue
            if self.gap_since is None:
                self.gap_since = now
            if now - self.gap_since < grace:
                return outcomes
            self.gap_since = None
            outcomes.extend(self._give_up(later[0], tolerance))

    def _give_up(self, first: tuple[int, float], tolerance: float) -> list[LegOutcome]:
        """Re-anchor on the first view beyond the cursor without proving the interval.

        The known executions of the interval are applied unproven: the own
        ones move the share, a foreign one takes nothing of it (the missing
        executions leave the leg it acted on unknown). The share is then
        clamped to the view, and what is left of it is held in doubt.
        """
        assert self.cursor_seq is not None
        cursor = self.cursor_seq
        gap = LegGap(
            start_seq=cursor, end_seq=first[0], start_leg=self.cursor_leg,
            end_leg=first[1], start_share=self.share, start_journal=self.journal,
            start_doubt=self.doubt,
            events={key: event for key, event in self.events.items()
                    if cursor < event.seq <= first[0]},
        )
        unexplained = first[1] - self._expected_leg(first[0])
        outcomes = self._walk(first, proven=False, tolerance=tolerance)
        direction = _direction(self.share)
        share_before = abs(self.share)
        leg_dir = first[1] * direction
        clamped = 0.0
        if direction and share_before > max(leg_dir, 0.0) + tolerance:
            clamped = share_before - max(leg_dir, 0.0)
            self.share = max(leg_dir, 0.0) * direction
            self._take_journal(clamped, direction)
        # The clamp may be the missing own reduce's doing: an own reduce that
        # arrives later finds the journal already netted for it.
        self._add_credit(first[0], clamped, direction)
        # What is left may be gone too: a missing own reduce offset by a
        # missing foreign add moves the leg by nothing, and the known foreign
        # reductions took an unknown part of it.
        self._add_doubt(first[0], abs(self.share), tolerance)
        if abs(self.share) > tolerance:
            gap.doubt = abs(self.share)
            self._add_gap(gap)
        self.blind_seq = first[0]
        self.room = {}
        self._note_room(first[1], self.share)
        outcomes.append(LegOutcome(
            event=None, consumed=clamped, proven=False,
            share_before=share_before, leg_after=leg_dir, seq=first[0],
            unexplained=unexplained, doubtful=gap.doubt,
        ))
        return outcomes

    def _settle_gaps(self, known_seqs: Container[int], tolerance: float) -> list[LegOutcome]:
        """Judge again every given-up interval its late executions completed.

        Walked from its start like a proven interval, on the journal changes
        its own executions stand for: when no foreign execution in it took
        any of the run's share and no own fill was clipped, the give-up
        guessed nothing, and its doubt is released. Otherwise it is kept
        (``settled``): the doubt it holds stays.

        :return: The verdicts on the interval's foreign executions
            (``settled``, never reported).
        """
        outcomes: list[LegOutcome] = []
        for gap in list(self.gaps):
            if (gap not in self.gaps or gap.settled
                    or not gap.complete(known_seqs, tolerance)):
                # A release before may have taken the whole doubt.
                continue
            assert gap.start_seq is not None
            scratch = LegLedger(
                cursor_seq=gap.start_seq, cursor_leg=gap.start_leg,
                share=gap.start_share, journal=gap.start_journal, doubt=gap.start_doubt,
                events={key: replace(event, share=event.own_share, credit=0.0)
                        if event.own else event
                        for key, event in gap.events.items()},
            )
            walked = scratch._walk((gap.end_seq, gap.end_leg), proven=True,
                                   tolerance=tolerance)
            guessed = False
            for item in walked:
                if item.event is not None and item.event.own:
                    guessed = guessed or item.clipped > 0.0
                    continue
                guessed = guessed or item.consumed > 0.0
                outcomes.append(replace(item, proven=False, settled=True))
            if guessed:
                gap.settled = True
                continue
            self.gaps.remove(gap)
            self.doubt = min(self.doubt, sum(item.doubt for item in self.gaps))
            self._cap_doubt(tolerance)
        return outcomes

    def _gap_of(self, seq: int) -> LegGap | None:
        """The given-up interval an execution at ``seq`` falls into."""
        for gap in self.gaps:
            if gap.holds(seq):
                return gap
        return None

    def _add_gap(self, gap: LegGap) -> None:
        """Keep one more given-up interval; the oldest merge beyond the limit.

        Merged ones never complete: their doubt is never released, only
        resolved by own reduces or the share running out.
        """
        self.gaps.append(gap)
        while len(self.gaps) > _MAX_GAPS:
            oldest = self.gaps.pop(0)
            nxt = self.gaps[0]
            self.gaps[0] = LegGap(
                start_seq=None, end_seq=nxt.end_seq, start_leg=nxt.end_leg,
                end_leg=nxt.end_leg, start_share=nxt.start_share,
                start_journal=nxt.start_journal, start_doubt=nxt.start_doubt,
                doubt=oldest.doubt + nxt.doubt,
            )

    def _take_journal(self, quantity: float, direction: float) -> None:
        """Take ``quantity`` off the journal share's ``direction`` side (a verdict).

        The run's entry rows of that side are netted for it
        (:attr:`to_net`).
        """
        if quantity <= 0.0 or not direction:
            return
        self.journal -= quantity * direction
        self.to_net[direction] = self.to_net.get(direction, 0.0) + quantity

    def _add_credit(self, seq: int, quantity: float, direction: float) -> None:
        """Remember a share reduction a late own reduce may already be part of."""
        if quantity <= 0.0 or not direction:
            return
        self.credits.append([float(seq), quantity, direction])
        del self.credits[:-_MAX_CREDITS]

    def _add_doubt(self, seq: int, quantity: float, tolerance: float) -> None:
        """Hold ``quantity`` more of the share in doubt, behind the view at ``seq``."""
        if quantity <= tolerance:
            return
        self.doubt += quantity
        self.doubt_seq = max(self.doubt_seq, seq)
        self._cap_doubt(tolerance)

    def _cap_doubt(self, tolerance: float) -> None:
        """Keep the doubt within the share: what the run no longer holds is no doubt.

        Without any doubt left, no given-up interval has anything to release.
        """
        self.doubt = min(self.doubt, abs(self.share))
        if self.doubt <= tolerance:
            self.doubt = 0.0
            self.gaps.clear()
            return
        for gap in self.gaps:
            gap.doubt = min(gap.doubt, abs(self.share))

    def _note_room(self, leg: float, share: float) -> None:
        """Lower :attr:`room` to the foreign exposure of one step, on each side."""
        for side in (1.0, -1.0):
            foreign = max(leg * side, 0.0) - max(share * side, 0.0)
            self.room[side] = min(self.room.get(side, foreign), foreign)

    def _own_beyond(self, seq: int) -> float:
        """The journal change of the own executions recorded beyond ``seq``."""
        return sum(event.share for event in self.events.values()
                   if event.own and event.seq > seq)

    def _expected_leg(self, seq: int) -> float:
        """The leg the known executions predict at ``seq``."""
        assert self.cursor_seq is not None
        return self.cursor_leg + sum(
            event.delta for event in self.events.values()
            if self.cursor_seq < event.seq <= seq
        )

    def _walk(
            self, view: tuple[int, float], *, proven: bool, tolerance: float,
    ) -> list[LegOutcome]:
        """Apply the executions up to ``view`` and move the cursor onto it.

        Executions sharing a ``seq`` were concluded together; the own ones
        and the foreign ones that grow the run's side go first, so a foreign
        reduction is never charged to the share when the same instant gave
        the leg room for it. A foreign reduction is charged to the run
        provably only for what it took below the share's proven part; the
        rest of what it took was in doubt (``doubtful``), and an own reduce
        arriving late finds the journal already netted for it.

        An own execution moves the share and the journal alike, but the
        share only as far as the leg holds and never past the journal: the
        part of the journal share the leg does not hold is foreign exposure
        the run's own fill consumed (``clipped``).

        Unproven (an interval given up), the leg the known executions
        rebuild misses what the missing ones did: a foreign execution takes
        nothing of the share, and an own one is bounded by the journal
        alone. The clamp to the view the interval ends on books the venue
        truth instead (:meth:`_give_up`).
        """
        assert self.cursor_seq is not None
        cursor = self.cursor_seq
        direction = _direction(self.share)

        def rank(event: LegEvent) -> tuple[int, int]:
            if event.own:
                return event.seq, 0
            return event.seq, 1 if event.delta * direction >= 0.0 else 2

        due = sorted(
            (event for event in self.events.values() if cursor < event.seq <= view[0]),
            key=rank,
        )
        outcomes: list[LegOutcome] = []
        leg = self.cursor_leg
        for event in due:
            if event.own:
                leg += event.delta
                share_before = abs(self.share)
                self.journal += event.share
                wanted = self._bounded(self.share + event.share, self.journal, tolerance)
                self.share = self._bounded(
                    self._capped(self.share + event.share, leg, tolerance),
                    self.journal, tolerance) if proven else wanted
                clipped = abs(wanted) - abs(self.share)
                if clipped > tolerance:
                    outcomes.append(LegOutcome(
                        event=event, consumed=0.0, proven=proven,
                        share_before=share_before,
                        leg_after=leg * _direction(wanted), seq=event.seq,
                        clipped=clipped,
                    ))
                self._cap_doubt(tolerance)
                if proven:
                    self._note_room(leg, self.share)
                continue
            direction = _direction(self.share)
            share = abs(self.share)
            leg_after = (leg + event.delta) * direction if direction else leg + event.delta
            consumed = doubtful = 0.0
            if proven and direction and event.delta * direction < 0.0:
                consumed = min(share, max(0.0, share - leg_after))
                if consumed <= tolerance:
                    consumed = 0.0
                else:
                    taken = min(consumed, max(0.0, share - self.doubt - leg_after))
                    doubtful = consumed - taken
                    if doubtful <= tolerance:
                        doubtful = 0.0
                    elif taken <= tolerance:
                        doubtful = consumed
            outcomes.append(LegOutcome(
                event=event, consumed=consumed, proven=proven, share_before=share,
                leg_after=leg_after, seq=event.seq, doubtful=doubtful,
            ))
            if consumed:
                self.share = (share - consumed) * direction
                self._take_journal(consumed, direction)
            if doubtful:
                self.doubt = max(0.0, self.doubt - doubtful)
                self._add_credit(self.doubt_seq, doubtful, direction)
            self._cap_doubt(tolerance)
            leg += event.delta
            if proven:
                self._note_room(leg, self.share)
        for event in due:
            del self.events[event.exec_id]
        self.cursor_seq = view[0]
        self.cursor_leg = view[1]
        self.share = self._capped(self.share, view[1], tolerance) if proven else self.share
        self._cap_doubt(tolerance)
        if proven:
            self._note_room(view[1], self.share)
        self.views = [item for item in self.views if item[0] >= view[0]]
        return outcomes

    @staticmethod
    def _bounded(share: float, journal: float, tolerance: float) -> float:
        """``share`` limited to ``journal``: never larger, never on the other side."""
        direction = _direction(share)
        if not direction or abs(share) <= tolerance or direction != _direction(journal):
            return 0.0
        if abs(share) <= abs(journal) + tolerance:
            return share
        return journal

    @staticmethod
    def _capped(share: float, leg: float, tolerance: float) -> float:
        """``share`` limited to what ``leg`` holds on the share's side."""
        direction = _direction(share)
        if not direction:
            return 0.0
        room = max(leg * direction, 0.0)
        if abs(share) <= room + tolerance:
            return share if abs(share) > tolerance else 0.0
        return room * direction
