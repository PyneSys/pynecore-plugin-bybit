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

Consumer pattern mirrors the Coinbase plugin: closed bars (and reconnect
backfill bars) travel through an ordered queue, the forming bar coalesces
into a single snapshot slot so a busy symbol cannot flood memory, and an
event wakes :meth:`watch_ohlcv`. A staleness watchdog force-closes the
transport when even pongs stop arriving; the framework then drives the
``disconnect()`` -> ``connect()`` -> ``on_reconnect()`` cycle, and
``on_reconnect`` REST-backfills the bars missed while offline.

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
        # On a RE-connect, hold WS-delivered closed bars back until
        # :meth:`on_reconnect` has backfilled the offline gap: the core
        # live generator drops any closed bar older than the newest one it
        # has seen, so letting a fresh WS close overtake the backfill would
        # permanently lose the gap bars. ``None`` = no backfill pending
        # (initial connect, nothing to resume from).
        self._pending_closed = [] if (self._last_closed_bar_ts is not None
                                      or self._stream_bar_ts is not None) else None

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
        self._pending_closed = None
        # Drop the pooled HTTP client too: on a final shutdown it must not
        # linger, and a reconnect cycle only pays one extra TLS handshake.
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

        Drains the authoritative queue (closed bars, backfill, the
        force-close sentinel) before the coalesced forming snapshot, so an
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
    async def on_reconnect(self) -> None:
        """REST-backfill the bars missed while the stream was down.

        Bybit does not replay missed WS frames on a new connection, so the
        gap between the last closed bar and now is filled from the kline
        endpoint and injected into the queue as closed bars, and only THEN
        are the WS closes held back since :meth:`connect` released — the
        core live generator drops out-of-order closed bars, so this order
        is what keeps the gap bars alive.

        A REST failure mid-backfill must NOT let the fresh stream go live.
        The live runner logs ``Reconnect failed`` and goes straight back
        into its disconnect/backoff/connect cycle WITHOUT reading the
        update queue, and that ``disconnect()`` throws the queue away — so
        every bar an earlier backfill page already enqueued dies there,
        with the cursor sitting past them. The cursor is therefore rewound
        to where the backfill started, which makes the retry re-fetch the
        whole gap. The transport is force-closed and the closure sentinel
        queued on top, so should the runner ever read the queue instead,
        ``watch_ohlcv`` raises ``ConnectionError`` and drives the same
        cycle rather than letting the fresh stream advance over the gap.
        """
        resume_from = self._last_closed_bar_ts
        try:
            await self._backfill_gap()
        except BybitError:
            # Rewind past the pages that never reached the runner, and
            # discard the held bars — the retried backfill re-fetches
            # everything from REST.
            self._last_closed_bar_ts = resume_from
            self._pending_closed = None
            # Kill the transport so the stream cannot go live without the
            # gap (see docstring). If the socket already died, its receive
            # loop has queued the sentinel itself.
            ws = self._public_ws
            self._public_ws = None
            if ws is not None:
                await ws.close()
                await self._on_ws_closed()
            raise
        self._release_pending_closed()

    async def _backfill_gap(self) -> None:
        """Fetch and enqueue the closed bars missed while offline.

        Walks the gap in windows of at most :data:`KLINE_LIMIT` bars (the
        endpoint's per-request cap; a longer window would silently return
        only the NEWEST candles and lose the head of the gap), the same
        chunking ``download_ohlcv`` uses. Any :class:`BybitError` from the
        REST layer propagates to :meth:`on_reconnect`.
        """
        last_closed = self._last_closed_bar_ts
        if self.timeframe is None:
            return
        queue = self._update_queue
        if queue is None:
            return
        interval = self.to_exchange_timeframe(self.timeframe)
        if last_closed is not None:
            cursor = add_interval(last_closed, interval, 1)
        elif self._stream_bar_ts is not None:
            # The stream has not closed a bar yet, so there is no cursor —
            # but the bar seen forming is exactly the first one that can
            # have closed while offline, and everything before it came
            # from the warmup history.
            cursor = self._stream_bar_ts
        else:
            return
        now_ts = int(epoch_time() * 1000)
        if bar_close_ts(cursor, interval) > now_ts:
            return
        market = await asyncio.to_thread(self._get_market)

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
            wrote = False
            for row in sorted(result.get('list') or [], key=lambda r: int(r[0])):
                bar_start = int(row[0])
                if bar_start <= (self._last_closed_bar_ts or 0):
                    continue
                if bar_close_ts(bar_start, interval) > now_ts:
                    continue
                self._enqueue_closed(queue, OHLCV(
                    timestamp=bar_start,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    is_closed=True,
                ))
                wrote = True
            if wrote:
                assert self._last_closed_bar_ts is not None
                cursor = add_interval(self._last_closed_bar_ts, interval, 1)
            else:
                cursor = max(add_interval(chunk_end, interval, 1),
                             add_interval(cursor, interval, 1))
        if self._data_ready is not None:
            self._data_ready.set()

    async def backfill_closed_bars(
            self, symbol: str, timeframe: str, since_ms: int,
    ) -> list[OHLCV]:
        """Fetch klines that closed between the warmup history and the stream.

        Same endpoint and chunking as :meth:`_backfill_gap`, but the bars
        never touch the update queue: the framework owns them and splices
        them in ahead of the first live one. The closed-bar cursor DOES
        advance to the newest bar handed over (``since_ms`` when the gap
        was empty), because that is the newest closed bar the run holds —
        without it a drop before the stream's own first close would leave
        the reconnect backfill with nothing to resume from and the whole
        outage window would be lost.

        :param symbol: Provider-format symbol (unused; the plugin instance is
            already bound to its instrument).
        :param timeframe: Timeframe in TradingView format.
        :param since_ms: Opening of the last bar the framework already holds.
        :return: Closed bars strictly after ``since_ms``, ascending.
        """
        interval = self.to_exchange_timeframe(timeframe)
        now_ts = int(epoch_time() * 1000)
        self._advance_closed_cursor(since_ms)
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
            newest: int | None = None
            for row in sorted(result.get('list') or [], key=lambda r: int(r[0])):
                bar_start = int(row[0])
                if bar_start <= since_ms:
                    continue
                if recovered and bar_start <= recovered[-1].timestamp:
                    continue
                if bar_close_ts(bar_start, interval) > now_ts:
                    # Still forming — the stream will deliver it.
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
        if recovered:
            self._advance_closed_cursor(recovered[-1].timestamp)
        return recovered

    def _advance_closed_cursor(self, bar_ts: int) -> None:
        """Move the closed-bar cursor forward to ``bar_ts``, never backwards.

        The stream runs concurrently with the startup-gap query and may
        already have closed a newer bar of its own.
        """
        if self._last_closed_bar_ts is None or self._last_closed_bar_ts < bar_ts:
            self._last_closed_bar_ts = bar_ts

    def _release_pending_closed(self) -> None:
        """Flush the closed bars held back during backfill into the queue."""
        pending = self._pending_closed
        self._pending_closed = None
        queue = self._update_queue
        if not pending or queue is None:
            return
        for bar in sorted(pending, key=lambda b: b.timestamp):
            if self._last_closed_bar_ts is not None \
                    and bar.timestamp <= self._last_closed_bar_ts:
                continue
            self._enqueue_closed(queue, bar)
        if self._data_ready is not None:
            self._data_ready.set()

    def _enqueue_closed(self, queue: 'asyncio.Queue[OHLCV | None]', bar: OHLCV) -> None:
        """Queue one authoritative closed bar and advance the closed cursor.

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
            # Every push carries the instrument's latest trade price —
            # feeds the spot position mark and the market-order
            # minimum-notional pre-check.
            self._last_price = bar.close
            if self._stream_bar_ts is None or bar.timestamp > self._stream_bar_ts:
                self._stream_bar_ts = bar.timestamp
            if bar.is_closed:
                if self._pending_closed is not None:
                    # Reconnect backfill still pending — hold the bar back
                    # so it cannot overtake the older gap bars (see connect).
                    self._pending_closed.append(bar)
                    continue
                # Duplicate guard: Bybit occasionally re-pushes the confirm
                # snapshot of an already-closed bar.
                if self._last_closed_bar_ts is not None \
                        and bar.timestamp <= self._last_closed_bar_ts:
                    continue
                self._enqueue_closed(queue, bar)
            else:
                self._latest_snapshot = bar
        if self._data_ready is not None:
            self._data_ready.set()

    async def _on_ws_closed(self) -> None:
        """Surface an unexpected transport death to the consumer.

        The ``None`` sentinel makes a pending :meth:`watch_ohlcv` raise
        ``ConnectionError``, which sends the framework into its
        disconnect/connect/on_reconnect cycle.
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
