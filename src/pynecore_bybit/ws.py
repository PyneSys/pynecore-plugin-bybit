"""Thin asyncio WebSocket client for the Bybit v5 streams.

One class serves both stream families: the public market streams
(``/v5/public/{category}``) used by the live provider and — with the
``auth`` hook — the private stream (``/v5/private``) the broker mix-ins
will consume in M2. It owns the transport concerns only: connect, the
20-second ``{"op": "ping"}`` cadence, subscription tracking with automatic
re-subscribe after :meth:`open` (the caller drives reconnects), inbound
dispatch to a callback, and clean shutdown. Everything payload-shaped
(kline parsing, order events) stays in the consuming mix-ins.

The message callback runs on the event loop inside the receive task —
keep it cheap (parse -> enqueue) or the receive loop stalls.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Awaitable, Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from .exceptions import BybitConnectionError
from .helpers import WS_PING_INTERVAL_S

logger = logging.getLogger(__name__)

#: How long to wait for the ``auth`` / ``subscribe`` acknowledgement frames.
_ACK_TIMEOUT_S = 15.0


class BybitWebSocket:
    """One Bybit v5 WebSocket connection with ping keepalive and resubscribe.

    :param url: Full ``wss://`` URL of the stream.
    :param on_message: Callback invoked with every decoded non-ack JSON
        frame. Runs on the event loop — must not block.
    :param on_closed: Async callback invoked once when the connection dies
        (receive loop exit for any reason other than :meth:`close`).
    """

    def __init__(self, url: str,
                 on_message: Callable[[dict], None],
                 on_closed: Callable[[], Awaitable[None]] | None = None):
        self.url = url
        self._on_message = on_message
        self._on_closed = on_closed
        self._ws: ClientConnection | None = None
        self._recv_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._topics: list[str] = []
        self._closing = False
        # Acknowledgement futures keyed by the ``req_id`` echoed back by
        # Bybit; resolved by the receive loop, awaited by :meth:`subscribe`.
        self._pending_acks: dict[str, asyncio.Future] = {}
        self._req_seq = 0
        #: Wall-clock stamp of the most recent inbound frame (any kind,
        #: pongs included). Consumers use it for staleness watchdogs.
        self.last_message_ts: float = 0.0

    @property
    def is_open(self) -> bool:
        """Whether the transport is connected and the receive loop alive."""
        return (self._ws is not None
                and self._recv_task is not None
                and not self._recv_task.done())

    async def open(self, *, api_key: str = '', api_secret: str = '') -> None:
        """Connect, optionally authenticate, and re-subscribe known topics.

        :param api_key: API key for the private stream; empty for public.
        :param api_secret: Secret paired with ``api_key``.
        :raises BybitConnectionError: On handshake / auth / subscribe failure.
        """
        self._closing = False
        try:
            # Bybit's own application-level ping/pong is used for liveness;
            # disable the library's protocol pings so a middlebox that
            # strips control frames cannot kill an otherwise healthy feed.
            self._ws = await connect(self.url, ping_interval=None)
        except (OSError, WebSocketException) as e:
            raise BybitConnectionError(
                f"Bybit WS connect failed ({self.url}): {e}"
            ) from e
        self.last_message_ts = time.time()

        if api_key:
            expires = int((time.time() + 10) * 1000)
            signature = hmac.new(
                api_secret.encode(), f"GET/realtime{expires}".encode(), hashlib.sha256,
            ).hexdigest()
            ack = await self._request({'op': 'auth', 'args': [api_key, expires, signature]})
            if not ack.get('success'):
                raise BybitConnectionError(
                    f"Bybit WS auth rejected: {ack.get('ret_msg', '')!r}"
                )

        self._recv_task = asyncio.create_task(self._recv_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

        if self._topics:
            await self.subscribe(self._topics, _resubscribe=True)

    async def subscribe(self, topics: list[str], *, _resubscribe: bool = False) -> None:
        """Subscribe to ``topics`` and remember them for later re-opens.

        Waits for Bybit's acknowledgement frame (correlated via the echoed
        ``req_id``, resolved by the receive loop) — a rejected or unanswered
        subscription must fail the connect instead of leaving a silently
        topicless stream: pongs would keep the transport watchdog happy
        while no data ever arrives.

        :raises BybitConnectionError: On send failure, a rejected
            subscription, transport death, or acknowledgement timeout.
        """
        if not _resubscribe:
            self._topics.extend(t for t in topics if t not in self._topics)
        self._req_seq += 1
        req_id = f"sub-{self._req_seq}"
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_acks[req_id] = future
        try:
            await self._send({'op': 'subscribe', 'req_id': req_id, 'args': list(topics)})
            try:
                ack: dict = await asyncio.wait_for(future, _ACK_TIMEOUT_S)
            except TimeoutError:
                raise BybitConnectionError(
                    "Bybit WS subscribe acknowledgement timed out"
                ) from None
        finally:
            self._pending_acks.pop(req_id, None)
        if not ack.get('success'):
            raise BybitConnectionError(
                f"Bybit WS subscribe rejected: {ack.get('ret_msg', '')!r}"
            )

    async def close(self) -> None:
        """Cancel the loops and close the transport (idempotent)."""
        self._closing = True
        for task in (self._ping_task, self._recv_task):
            if task is not None:
                task.cancel()
        self._ping_task = None
        self._recv_task = None
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                await ws.close()
            except (OSError, WebSocketException):
                pass

    # --- internals -----------------------------------------------------------

    async def _send(self, frame: dict) -> None:
        ws = self._ws
        if ws is None:
            raise BybitConnectionError("Bybit WS is not connected")
        try:
            await ws.send(json.dumps(frame))
        except (OSError, WebSocketException) as e:
            raise BybitConnectionError(f"Bybit WS send failed: {e}") from e

    async def _request(self, frame: dict) -> dict:
        """Send ``frame`` and read frames inline until its acknowledgement.

        Only used before the receive loop starts (the ``auth`` handshake);
        data frames cannot arrive yet because nothing is subscribed.
        """
        await self._send(frame)
        ws = self._ws
        assert ws is not None
        deadline = time.time() + _ACK_TIMEOUT_S
        while (timeout := deadline - time.time()) > 0:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout)
            except TimeoutError:
                continue
            except (OSError, WebSocketException) as e:
                raise BybitConnectionError(f"Bybit WS receive failed: {e}") from e
            self.last_message_ts = time.time()
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(data, dict):
                continue
            if data.get('op') == frame.get('op'):
                return data
        raise BybitConnectionError(
            f"Bybit WS {frame.get('op')!r} acknowledgement timed out"
        )

    async def _recv_loop(self) -> None:
        ws = self._ws
        assert ws is not None
        try:
            async for raw in ws:
                self.last_message_ts = time.time()
                try:
                    data = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if not isinstance(data, dict):
                    continue
                op = data.get('op')
                if op in ('ping', 'pong') or data.get('ret_msg') == 'pong':
                    continue
                if op in ('subscribe', 'unsubscribe', 'auth'):
                    future = self._pending_acks.get(str(data.get('req_id') or ''))
                    if future is not None and not future.done():
                        future.set_result(data)
                    elif not data.get('success', True):
                        logger.error("Bybit WS %s rejected: %s", op, data)
                    continue
                self._on_message(data)
        except (OSError, WebSocketException, asyncio.CancelledError):
            pass
        finally:
            # Fail outstanding acknowledgement waiters immediately instead
            # of letting them run into their timeout on a dead transport.
            for future in self._pending_acks.values():
                if not future.done():
                    future.set_exception(BybitConnectionError(
                        "Bybit WS closed before the acknowledgement arrived"
                    ))
            # The callback is plugin-internal (sentinel enqueue + event set)
            # and must not raise; anything it does raise surfaces on this
            # task, which is already terminating.
            if not self._closing and self._on_closed is not None:
                await self._on_closed()

    async def _ping_loop(self) -> None:
        """Send the application-level ping every :data:`WS_PING_INTERVAL_S`."""
        try:
            while True:
                await asyncio.sleep(WS_PING_INTERVAL_S)
                try:
                    await self._send({'op': 'ping'})
                except BybitConnectionError:
                    return
        except asyncio.CancelledError:
            pass
