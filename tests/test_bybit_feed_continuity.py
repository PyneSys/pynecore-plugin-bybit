"""
@pyne
"""
import asyncio

import pytest

from pynecore.types.ohlcv import OHLCV

import pynecore_bybit.live_provider as live_provider_module
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.models import InstrumentInfo


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


# === Fixed grid ============================================================

#: One minute in milliseconds — every scenario runs on the ``1`` timeframe.
MIN = 60_000
#: Minute-aligned epoch anchor; the clock is faked, so no wall-clock reads.
BASE = 1_700_000_000_000 // MIN * MIN


def _bar(k: int) -> int:
    """Open timestamp of the k-th minute bar of the scenario grid."""
    return BASE + k * MIN


def _instrument() -> InstrumentInfo:
    """Minimal tradable spot instrument — only symbol/category are used here."""
    return InstrumentInfo(
        category='spot', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='', status='Trading', tick_size_str='0.10', tick_size=0.1,
        qty_step_str='0.001', qty_step=0.001, min_order_qty=0.0,
        min_order_amt=1.0, min_notional=0.0, max_limit_order_qty=100.0,
        max_market_order_qty=50.0, contract_type='', delivery_time=None,
    )


# === Fake venue ============================================================

class _FakeWS:
    """Stand-in for :class:`BybitWebSocket` with an injectable death switch.

    Holds the plugin's dispatch callbacks so a test can push kline frames
    (:meth:`push`) and kill the transport the way the real receive loop does
    (:meth:`die` -> ``on_closed``).
    """

    def __init__(self, url, on_message, on_closed=None):
        self.url = url
        self.on_message = on_message
        self.on_closed = on_closed
        self.open_error: Exception | None = None
        self.topics: list[str] = []
        self.opened = False
        self.closed = False
        self.last_message_ts = float('inf')  # never stale for the watchdog

    @property
    def is_open(self) -> bool:
        return self.opened and not self.closed

    async def open(self) -> None:
        if self.open_error is not None:
            raise self.open_error
        self.opened = True

    async def subscribe(self, topics) -> None:
        self.topics = list(topics)

    async def close(self) -> None:
        self.closed = True

    async def die(self) -> None:
        """Transport death: receive loop exits and notifies the plugin."""
        self.closed = True
        if self.on_closed is not None:
            await self.on_closed()

    def push(self, ts: int, *, confirm: bool, close: float = 100.5) -> None:
        """Deliver one kline frame for the bar opening at ``ts``."""
        self.on_message({
            'topic': 'kline.1.BTCUSDT',
            'data': [{'start': str(ts), 'open': '100', 'high': '102',
                      'low': '99', 'close': str(close), 'volume': '3',
                      'confirm': confirm}],
        })


class _Venue(Bybit):
    """Bybit wired to a faked kline endpoint, fake clock and fake sockets.

    ``rest_failures`` makes the next N kline requests raise a transport
    error; ``connect_failures`` makes the next N WS handshakes fail;
    ``overlap`` makes every kline response also carry that many bars from
    BEFORE the requested window (a real reconnect re-serve), which the
    plugin must not emit again.
    """

    def __init__(self, **kwargs):
        kwargs.setdefault('config', BybitConfig())
        kwargs.setdefault('symbol', 'BTCUSDT')
        kwargs.setdefault('timeframe', '1')
        super().__init__(**kwargs)
        self._market = _instrument()
        #: Faked wall clock in seconds, advanced explicitly by the tests.
        self.now_s = _bar(0) / 1000.0
        self.rest_failures = 0
        #: Kline request ordinals (0-based) that must fail regardless of
        #: ``rest_failures`` — used to break a specific backfill page.
        self.rest_fail_at: set[int] = set()
        self.connect_failures = 0
        self.overlap = 0
        self.kline_calls: list[tuple[int, int]] = []
        self.sockets: list[_FakeWS] = []

    # --- fake transports ---------------------------------------------------

    @property
    def ws(self) -> _FakeWS:
        """The socket the plugin is currently talking to."""
        return self.sockets[-1]

    def make_ws(self, url, on_message, on_closed=None) -> _FakeWS:
        ws = _FakeWS(url, on_message, on_closed)
        if self.connect_failures > 0:
            self.connect_failures -= 1
            ws.open_error = BybitConnectionError("WS handshake refused")
        self.sockets.append(ws)
        return ws

    def advance_to(self, k: int, *, offset_s: float = 30.0) -> None:
        """Move the fake clock into the k-th bar, ``offset_s`` past its open."""
        self.now_s = _bar(k) / 1000.0 + offset_s

    # --- fake REST ---------------------------------------------------------

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        assert endpoint == '/v5/market/kline', endpoint
        params = dict(params or {})
        start, end = int(params['start']), int(params['end'])
        ordinal = len(self.kline_calls)
        self.kline_calls.append((start, end))
        if ordinal in self.rest_fail_at:
            raise BybitConnectionError("kline endpoint down")
        if self.rest_failures > 0:
            self.rest_failures -= 1
            raise BybitConnectionError("kline endpoint down")
        now_ms = int(self.now_s * 1000)
        stamps = []
        # The venue serves the still-forming bar too; the plugin filters it.
        for i in range(1, self.overlap + 1):
            stamps.append(start - i * MIN)
        ts = start
        while ts <= min(end, now_ms):
            stamps.append(ts)
            ts += MIN
        stamps = [t for t in sorted(stamps) if t >= BASE][-int(params['limit']):]
        return {'list': [[str(t), '100', '102', '99', '100.5', '3', '300']
                         for t in reversed(stamps)]}


# === Runner-side simulation =================================================

class _Runner:
    """Mimics the core live loop's consumption of the provider stream.

    Records the closed bars the plugin actually delivered and, separately,
    the ones the core monotonicity guard had to drop — a non-empty
    ``dropped`` means the PLUGIN re-served settled history.
    """

    def __init__(self):
        self.closed: list[int] = []
        self.dropped: list[int] = []
        self.last_confirmed: int | None = None

    def feed(self, bar: OHLCV) -> None:
        if not bar.is_closed:
            return
        if self.last_confirmed is not None and bar.timestamp <= self.last_confirmed:
            self.dropped.append(bar.timestamp)
            return
        self.last_confirmed = bar.timestamp
        self.closed.append(bar.timestamp)


async def _drain(plugin: _Venue, runner: _Runner) -> bool:
    """Consume everything ``watch_ohlcv`` can yield without blocking.

    :return: ``True`` when the stream signalled its death (the sentinel made
        ``watch_ohlcv`` raise), i.e. the runner would enter a reconnect.
    """
    while True:
        queue = plugin._update_queue
        if queue is None:
            return True
        if queue.empty() and plugin._latest_snapshot is None:
            return False
        try:
            runner.feed(await plugin.watch_ohlcv('BTCUSDT', '1'))
        except ConnectionError:
            return True


async def _reconnect(plugin: _Venue, runner: _Runner, *, attempts: int = 8) -> int:
    """Replay the live runner's reconnect loop verbatim.

    ``on_disconnect`` -> ``disconnect`` -> ``connect`` -> ``on_reconnect``,
    retried on any exception. The runner does NOT drain the queue between a
    failed attempt and the next ``disconnect()`` — that queue is thrown
    away — so this driver must not drain there either.

    :return: The number of attempts the reconnect took.
    """
    for attempt in range(1, attempts + 1):
        await plugin.on_disconnect()
        try:
            await plugin.disconnect()
        except Exception:  # noqa: BLE001 - the runner logs and continues too
            pass
        try:
            await plugin.connect()
            await plugin.on_reconnect()
        except Exception:  # noqa: BLE001 - "Reconnect failed", next attempt
            continue
        await _drain(plugin, runner)
        return attempt
    raise AssertionError("reconnect never succeeded")


def _run(scenario, monkeypatch) -> None:
    """Run ``scenario(plugin, runner)`` with the venue fakes installed."""
    plugin = _Venue()
    runner = _Runner()
    monkeypatch.setattr(live_provider_module, 'BybitWebSocket', plugin.make_ws)
    monkeypatch.setattr(live_provider_module, 'epoch_time', lambda: plugin.now_s)

    async def _main():
        await scenario(plugin, runner)
        await plugin.disconnect()

    asyncio.run(_main())


async def _start_live(plugin: _Venue, runner: _Runner) -> None:
    """Bring the stream up and let it close one bar, as a real session does."""
    plugin.advance_to(1)
    await plugin.connect()
    plugin.ws.push(_bar(0), confirm=True)
    plugin.ws.push(_bar(1), confirm=False)
    await _drain(plugin, runner)
    assert runner.closed == [_bar(0)]


# === (a) Outage bars arrive via backfill, seam stays clean ==================

def __test_bybit_outage_bars_backfilled_in_order__(monkeypatch):
    """Bars that close during an outage are delivered once, in order."""

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)

        # The socket dies inside bar 1; bars 1..3 close while offline.
        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(4)

        assert await _reconnect(plugin, runner) == 1
        # Live streaming resumes on the fresh socket with no seam gap.
        plugin.advance_to(5)
        plugin.ws.push(_bar(4), confirm=True)
        await _drain(plugin, runner)

        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (b) Reconnect backfill re-serving pre-outage bars ======================

def __test_bybit_backfill_overlap_not_re_emitted__(monkeypatch):
    """Overlap from the backfill window is dropped by the PLUGIN, not the core.

    The plugin's contract is the strong one: bars at or before the newest
    closed bar it already emitted never reach the runner at all, so the
    core's monotonicity guard never has to fire.
    """

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)
        # Every kline response carries three bars from before the window.
        plugin.overlap = 3

        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(4)
        await _reconnect(plugin, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_bybit_seam_bar_emitted_exactly_once__(monkeypatch):
    """The bar spanning the reconnect boundary is neither lost nor doubled.

    The re-subscribed stream re-pushes the ``confirm`` snapshot of the last
    closed bar while the backfill is still in flight; that same bar also
    comes back from REST. Exactly one copy may reach the runner.
    """

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)

        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(3)

        # Hand-driven reconnect so the WS can push mid-backfill.
        await plugin.on_disconnect()
        await plugin.disconnect()
        await plugin.connect()
        # Fresh subscription replays bar 2's close before REST answers.
        plugin.ws.push(_bar(2), confirm=True)
        await plugin.on_reconnect()
        await _drain(plugin, runner)

        assert runner.closed == [_bar(0), _bar(1), _bar(2)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (c) Repeated failed reconnect attempts =================================

def __test_bybit_no_bar_lost_across_failed_reconnects__(monkeypatch):
    """Two dead handshakes and a dead REST later, the gap is still intact."""

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)

        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(5)
        plugin.connect_failures = 2  # attempts 1-2 never open a socket
        plugin.rest_failures = 1     # attempt 3 dies inside the backfill

        assert await _reconnect(plugin, runner) == 4
        assert runner.closed == [_bar(k) for k in range(5)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_bybit_partial_backfill_page_failure_keeps_gap__(monkeypatch):
    """A REST failure on the SECOND backfill page loses nothing.

    The first page's bars are already queued and the cursor has advanced
    past them, but the runner never drains that queue: it goes straight
    back into ``disconnect()``, which throws the queue away. The cursor
    must therefore rewind so the retry re-fetches the whole gap.
    """

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)

        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(6)
        # One page per bar: page 1 succeeds, page 2 dies.
        monkeypatch.setattr(live_provider_module, 'KLINE_LIMIT', 1)
        plugin.rest_fail_at = {1}

        assert await _reconnect(plugin, runner) == 2
        assert runner.closed == [_bar(k) for k in range(6)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === (d) Drop exactly on a bar boundary =====================================

def __test_bybit_drop_at_bar_boundary_delivers_once__(monkeypatch):
    """A bar closing at the very moment of the drop arrives exactly once.

    Two variants of the same instant: the ``confirm`` frame slips through
    just before the socket dies (the backfill must not repeat it), and it
    does not (the backfill must supply it).
    """

    async def confirm_arrived(plugin, runner):
        await _start_live(plugin, runner)
        plugin.advance_to(2, offset_s=0.0)
        # Bar 1 confirms, then the transport dies in the same instant.
        plugin.ws.push(_bar(1), confirm=True)
        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(3)
        await _reconnect(plugin, runner)

        assert runner.closed == [_bar(0), _bar(1), _bar(2)]
        assert runner.dropped == []

    async def confirm_lost(plugin, runner):
        await _start_live(plugin, runner)
        plugin.advance_to(2, offset_s=0.0)
        # Same instant, but the confirm frame died with the socket.
        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(3)
        await _reconnect(plugin, runner)

        assert runner.closed == [_bar(0), _bar(1), _bar(2)]
        assert runner.dropped == []

    _run(confirm_arrived, monkeypatch)
    _run(confirm_lost, monkeypatch)


# === Drop before the stream's first close ===================================

def __test_bybit_gap_after_startup_backfill_only__(monkeypatch):
    """A drop before the first live close still backfills the gap.

    The startup-gap query hands the runner every bar up to ``since_ms``;
    that is the newest closed bar the run holds, so it — not "nothing" —
    is where a reconnect backfill has to resume from.
    """

    async def scenario(plugin, runner):
        plugin.advance_to(1)
        await plugin.connect()
        # Framework startup gap: warmup history ended at bar 0.
        recovered = await plugin.backfill_closed_bars('BTCUSDT', '1', _bar(0))
        assert [b.timestamp for b in recovered] == []
        for bar in recovered:
            runner.feed(bar)
        runner.feed(OHLCV(timestamp=_bar(0), open=100.0, high=102.0, low=99.0,
                          close=100.5, volume=3.0, is_closed=True))

        # The socket dies without ever having delivered a frame.
        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(4)
        await _reconnect(plugin, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


def __test_bybit_gap_with_only_a_forming_bar_seen__(monkeypatch):
    """A drop after only forming ticks still backfills the gap.

    Nothing has closed on the stream yet, so the cursor is unset; the bar
    seen forming is the first one that can close while offline, and every
    bar before it came from the warmup history.
    """

    async def scenario(plugin, runner):
        plugin.advance_to(1)
        await plugin.connect()
        runner.feed(OHLCV(timestamp=_bar(0), open=100.0, high=102.0, low=99.0,
                          close=100.5, volume=3.0, is_closed=True))
        plugin.ws.push(_bar(1), confirm=False)
        await _drain(plugin, runner)
        assert runner.closed == [_bar(0)]

        await plugin.ws.die()
        assert await _drain(plugin, runner)
        plugin.advance_to(4)
        await _reconnect(plugin, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []

    _run(scenario, monkeypatch)


# === Sanity: no reconnect, no spurious REST =================================

def __test_bybit_clean_stream_never_backfills__(monkeypatch):
    """Without a drop the plugin serves the stream alone — no REST at all."""

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)
        for k in (1, 2, 3):
            plugin.advance_to(k + 1)
            plugin.ws.push(_bar(k), confirm=True)
            plugin.ws.push(_bar(k + 1), confirm=False)
            await _drain(plugin, runner)

        assert runner.closed == [_bar(k) for k in range(4)]
        assert runner.dropped == []
        assert plugin.kline_calls == []

    _run(scenario, monkeypatch)


def __test_bybit_watch_ohlcv_reports_dead_stream__(monkeypatch):
    """The death sentinel surfaces only AFTER the bars queued before it."""

    async def scenario(plugin, runner):
        await _start_live(plugin, runner)
        plugin.advance_to(2, offset_s=0.0)
        plugin.ws.push(_bar(1), confirm=True)
        await plugin.ws.die()

        first = await plugin.watch_ohlcv('BTCUSDT', '1')
        assert first.is_closed and first.timestamp == _bar(1)
        with pytest.raises(ConnectionError):
            await plugin.watch_ohlcv('BTCUSDT', '1')

    _run(scenario, monkeypatch)
