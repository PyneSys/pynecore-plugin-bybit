"""WebSocket market-data streaming for the Bybit plugin.

Bybit's public ``kline.{interval}.{symbol}`` topic pushes complete bar
snapshots (open/high/low/close/volume with a ``confirm`` flag), so — unlike
the Coinbase plugin, which aggregates raw trades — the mapping to
:class:`~pynecore.types.ohlcv.OHLCV` is direct: ``confirm: true`` is the
closed bar, ``confirm: false`` an intra-bar refinement of the forming bar.

Demo mode notes: the demo environment has no public stream of its own (demo
runs on real mainnet prices), so the public WS host resolves to the
region's mainnet stream while REST stays on the ``*-demo`` host — the host
table in :mod:`.hosts` encodes that.

Consumer pattern mirrors the Coinbase plugin: closed bars travel through an
ordered queue, the forming bar coalesces into a single snapshot slot so a
busy symbol cannot flood memory, and an event wakes
:meth:`_LiveProviderMixin.watch_ohlcv`. A staleness watchdog force-closes the
transport when even pongs stop arriving; the framework then drives the
``disconnect()`` -> ``connect()`` cycle.

The stream hands on only what the venue pushes. Recovering closed bars the
socket did not deliver (startup gap, reconnect gap, a close the socket
missed) is the framework's job: it asks
:meth:`_LiveProviderMixin.backfill_closed_bars`, a pure kline REST query, and
splices the answer onto its own closed-bar cursor.

Module state lives on :class:`._base._BybitBase`; see the WebSocket section
there for the attribute surface.
"""
import asyncio
import logging
from abc import ABC
from time import time as epoch_time

from pynecore.core.plugin import override
from pynecore.types.ohlcv import OHLCV

from ._base import _BybitBase
from .exceptions import BybitConnectionError, BybitError
from .helpers import KLINE_LIMIT, WS_STALE_THRESHOLD_S, add_interval, bar_close_ts
from .ws import BybitWebSocket

logger = logging.getLogger(__name__)


class _LiveProviderMixin(_BybitBase, ABC):
    """Public kline stream + queue/snapshot bridge to ``watch_ohlcv``."""

    # Measured on the public kline endpoint: 8 illiquid symbols (4 spot,
    # 4 linear), 1000 one-minute bars each, no slot skipped; a slot without
    # trades comes back as a zero-volume bar with open = high = low = close.
    history_has_idle_bars = True

    # Measured on BTCUSDT linear, BTCUSDT spot and MNTUSDT linear: the kline
    # endpoint served the closed bar 0.2-1.2 s after its close, and its values
    # did not change over the following 20 s.
    history_publish_lag = 3.0

    # --- Lifecycle -----------------------------------------------------------

    @override
    async def connect(self) -> None:
        """Resolve the market, open the public WS and subscribe the kline topic.

        Market-data channels are public — no credentials are sent. Called
        for the initial connection AND on every framework reconnect, so all
        connection-scoped state is rebuilt here from scratch.
        """
        if self.timeframe is None or self.symbol is None:
            raise BybitConnectionError(
                "Bybit live provider requires both 'symbol' and 'timeframe' "
                "to be configured before connect()"
            )
        interval = self.to_exchange_timeframe(self.timeframe)
        # Category + native name decide the public stream path; REST probes
        # run off-loop.
        market = await asyncio.to_thread(self._get_market)

        host = self._hosts.ws_public
        if not host:
            raise BybitConnectionError(
                f"Bybit region {self.config.region!r} has no public stream "
                f"host; set the ws_public_host override in the config"
            )

        # Fresh connection-scoped state on every (re)connect.
        self._update_queue = asyncio.Queue()
        self._latest_snapshot = None
        self._data_ready = asyncio.Event()

        ws = BybitWebSocket(
            f"wss://{host}/v5/public/{market.category}",
            on_message=self._on_ws_message,
            on_closed=self._on_ws_closed,
        )
        self._public_ws = ws
        try:
            await ws.open()
            await ws.subscribe([f"kline.{interval}.{market.symbol}"])
        except BybitError:
            self._public_ws = None
            await ws.close()
            raise

        self._watchdog_task = asyncio.create_task(self._feed_watchdog_loop())

    @override
    async def disconnect(self) -> None:
        """Close the WS transport and drop connection-scoped state."""
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None:
            task.cancel()
        ws = self._public_ws
        self._public_ws = None
        if ws is not None:
            await ws.close()
        self._update_queue = None
        self._latest_snapshot = None
        self._data_ready = None
        # Drop the pooled HTTP client too: on a final shutdown it must not
        # linger, and a reconnect cycle only pays one extra TLS handshake. The
        # broker side shares it, so it is closed once the requests still
        # running on it end.
        self._close_http_client()

    @property
    @override
    def is_connected(self) -> bool:
        """Whether the public stream transport is currently open."""
        ws = self._public_ws
        return ws is not None and ws.is_open

    # --- Data streaming --------------------------------------------------------

    @override
    async def watch_ohlcv(self, symbol: str, timeframe: str) -> OHLCV:
        """Block until the next OHLCV update arrives.

        Drains the authoritative queue (closed bars, the force-close
        sentinel) before the coalesced forming snapshot, so an
        intra-bar update can never precede the close of its predecessor.

        :param symbol: Ignored — the subscription is bound to the symbol
            the plugin was constructed with.
        :param timeframe: Ignored for the same reason.
        :raises ConnectionError: When the stream has been force-closed.
        """
        while True:
            queue = self._update_queue
            event = self._data_ready
            if queue is None or event is None:
                raise ConnectionError(
                    "Bybit live provider is not connected — call connect() first."
                )
            if not queue.empty():
                item = queue.get_nowait()
                if item is None:
                    raise ConnectionError("Bybit WebSocket stream closed unexpectedly")
                return item
            snapshot = self._latest_snapshot
            if snapshot is not None:
                self._latest_snapshot = None
                return snapshot
            # The producer runs on this same loop with no await between our
            # clear() and wait(), and the data lives in the queue / slot
            # rather than the event edge — a spurious set only costs one
            # re-check, never a lost update.
            event.clear()
            await event.wait()

    @override
    async def backfill_closed_bars(
            self, symbol: str, timeframe: str, since_ms: int,
    ) -> list[OHLCV]:
        """Query the kline endpoint for the bars that closed after ``since_ms``.

        A pure query: it reads nothing but the venue and writes nothing, so
        the framework may call it any number of times while the stream runs
        (startup gap, reconnect gap, a close the socket missed). The window
        is walked in requests of at most :data:`KLINE_LIMIT` bars, the
        endpoint's per-request cap: a longer window would silently return
        only the newest candles and lose the head of the gap. The bar still
        forming is left out; the stream delivers it.

        :param symbol: Provider-format symbol (unused; the plugin instance is
            already bound to its instrument).
        :param timeframe: Timeframe in TradingView format.
        :param since_ms: Opening of the last bar the framework already holds.
        :return: Closed bars strictly after ``since_ms``, ascending; empty when
            the venue has none.
        :raises BybitError: When a request was not answered or the answer
            carries no kline list; nothing is returned then, not even the
            pages already read.
        """
        interval = self.to_exchange_timeframe(timeframe)
        now_ts = int(epoch_time() * 1000)
        cursor = add_interval(since_ms, interval, 1)
        if bar_close_ts(cursor, interval) > now_ts:
            return []
        market = await asyncio.to_thread(self._get_market)
        recovered: list[OHLCV] = []
        while bar_close_ts(cursor, interval) <= now_ts:
            chunk_end = min(add_interval(cursor, interval, KLINE_LIMIT - 1), now_ts)
            result = await self._call('/v5/market/kline', {
                'category': market.category,
                'symbol': market.symbol,
                'interval': interval,
                'start': cursor,
                'end': chunk_end,
                'limit': KLINE_LIMIT,
            })
            rows = result.get('list')
            if not isinstance(rows, list):
                raise BybitConnectionError(
                    f"Bybit kline answer for {market.symbol} has no bar list: {result!r}"
                )
            newest: int | None = None
            for row in sorted(rows, key=lambda r: int(r[0])):
                bar_start = int(row[0])
                if bar_start <= since_ms:
                    continue
                if recovered and bar_start <= recovered[-1].timestamp:
                    continue
                if bar_close_ts(bar_start, interval) > now_ts:
                    continue
                recovered.append(OHLCV(
                    timestamp=bar_start,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    is_closed=True,
                ))
                newest = bar_start
            if newest is not None:
                cursor = add_interval(newest, interval, 1)
            else:
                cursor = max(add_interval(chunk_end, interval, 1),
                             add_interval(cursor, interval, 1))
        return recovered

    def _enqueue_closed(self, queue: 'asyncio.Queue[OHLCV | None]', bar: OHLCV) -> None:
        """Queue one closed bar of the socket and advance the closed cursor.

        Also drops a forming snapshot that is now stale (same bar or older
        than the close): ``watch_ohlcv`` drains the queue before the
        snapshot slot, so an undrained snapshot of an already-finalized bar
        would otherwise be emitted AFTER its own close as a bogus intra-bar
        tick. A genuinely newer forming bar is preserved.
        """
        snapshot = self._latest_snapshot
        if snapshot is not None and snapshot.timestamp <= bar.timestamp:
            self._latest_snapshot = None
        self._last_closed_bar_ts = bar.timestamp
        queue.put_nowait(bar)

    # --- Inbound dispatch --------------------------------------------------------

    def _on_ws_message(self, data: dict) -> None:
        """Map one kline push to OHLCV and hand it to the consumer side.

        Runs on the event loop inside the WS receive task — parse and
        enqueue only. Each push's ``data`` array carries one or more bar
        snapshots; ``confirm: true`` entries are authoritative closed bars
        (queued in order), the ``confirm: false`` entry refines the forming
        bar (coalesced into the single snapshot slot).
        """
        queue = self._update_queue
        if queue is None or not str(data.get('topic', '')).startswith('kline.'):
            return
        for entry in data.get('data') or ():
            try:
                bar = OHLCV(
                    timestamp=int(entry['start']),
                    open=float(entry['open']),
                    high=float(entry['high']),
                    low=float(entry['low']),
                    close=float(entry['close']),
                    volume=float(entry['volume']),
                    is_closed=bool(entry.get('confirm', False)),
                )
            except (KeyError, ValueError, TypeError):
                continue
            # Bybit occasionally re-pushes a slot it already closed: the
            # confirm snapshot of a closed bar, and a ``confirm: false``
            # snapshot (measured live 2026-10-05, linear ETHUSDT: once for
            # the bar closed a minute earlier, once for a bar nine minutes
            # old). The bar is settled, so the push refines nothing, and its
            # close is not the latest trade. Drop it here, before the price
            # cache: forwarded, a forming one would reach the core as an
            # intra-bar update under a closed bar's timestamp.
            if self._last_closed_bar_ts is not None \
                    and bar.timestamp <= self._last_closed_bar_ts:
                continue
            # Every other push carries the instrument's latest trade price —
            # feeds the spot position mark and the market-order
            # minimum-notional pre-check.
            self._last_price = bar.close
            if bar.is_closed:
                self._enqueue_closed(queue, bar)
            else:
                self._latest_snapshot = bar
        if self._data_ready is not None:
            self._data_ready.set()

    async def _on_ws_closed(self) -> None:
        """Surface an unexpected transport death to the consumer.

        The ``None`` sentinel makes a pending :meth:`watch_ohlcv` raise
        ``ConnectionError``, which sends the framework into its
        disconnect/connect cycle.
        """
        queue = self._update_queue
        if queue is not None:
            queue.put_nowait(None)
        if self._data_ready is not None:
            self._data_ready.set()

    # --- Watchdog -------------------------------------------------------------

    async def _feed_watchdog_loop(self) -> None:
        """Force-close the WS when inbound frames stop arriving entirely.

        The ping loop elicits a pong every ~20 s even on a tradeless
        symbol, so :data:`WS_STALE_THRESHOLD_S` of total silence means a
        half-open transport. Closing the socket makes the receive loop
        exit, which triggers :meth:`_on_ws_closed` and the framework's
        reconnect path.
        """
        try:
            while True:
                await asyncio.sleep(1.0)
                ws = self._public_ws
                if ws is None:
                    return
                if epoch_time() - ws.last_message_ts <= WS_STALE_THRESHOLD_S:
                    continue
                logger.warning(
                    "Bybit WS stream silent for >%.0fs — forcing reconnect",
                    WS_STALE_THRESHOLD_S,
                )
                self._public_ws = None
                await ws.close()
                await self._on_ws_closed()
                return
        except asyncio.CancelledError:
            pass
