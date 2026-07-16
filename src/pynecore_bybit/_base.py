"""Type-only shared base class for the Bybit plugin's mix-ins.

Mirrors the Capital.com / Coinbase plugin layout — every mix-in inherits
from :class:`_BybitBase` so static analysers (PyCharm, pyright) can resolve
``self.<attr>`` and Bybit-private cross-mix-in method calls without
warnings. Instance attribute annotations and Bybit-private method
signatures grow here as additional mix-ins land (the broker surface
arrives in M2; the base then moves under ``BrokerPlugin``).
"""
import asyncio
from typing import TYPE_CHECKING

from pynecore.core.plugin.live_provider import LiveProviderPlugin
from pynecore.types.ohlcv import OHLCV

from .config import BybitConfig

if TYPE_CHECKING:
    import httpx

    from .hosts import BybitHosts
    from .models import InstrumentInfo
    from .ws import BybitWebSocket


class _BybitBase(LiveProviderPlugin[BybitConfig]):
    """Shared instance state + Bybit-private cross-mix-in surface.

    Concrete implementations live in the individual mix-ins; this class
    declares the attribute and method surface so static analysis resolves
    ``self.<x>`` references uniformly across mix-ins.
    """

    plugin_name = "Bybit"
    Config = BybitConfig
    timezone = 'UTC'

    # The kline topic pushes only on trade activity (plus the confirm
    # snapshot at bar close), so a quiet instrument legitimately produces
    # no ``watch_ohlcv`` updates for several bars while the transport-level
    # ping/pong watchdog keeps covering dead sockets. The framework default
    # (3 bars) would reconnect-churn on every tradeless stretch; 30 bars
    # keeps the framework's dead-feed safety net (the only guard against a
    # lost subscription while pongs keep flowing) with rare false positives.
    feed_timeout_bars = 30

    # Narrow the base ``ProviderPlugin.config: ConfigT | None`` — the
    # runtime ``__init__`` raises unless the value is a ``BybitConfig``,
    # so every method can treat it as non-``None``.
    config: BybitConfig

    # --- Host / REST state (rest.py) ---
    # Host triple resolved from ``(config.region, config.demo)`` at
    # construction time; never changes over the instance's lifetime.
    _hosts: 'BybitHosts'
    # Pooled HTTP client, built lazily on first request so plugin
    # discovery / config generation never opens sockets.
    _http_client: 'httpx.Client | None'

    # --- Instrument resolution state (provider.py) ---
    # Normalized instrument-rule cache keyed by ``(category, symbol)``,
    # shared by symbol resolution, SymInfo synthesis and (from M2) order
    # quantization.
    _instruments: 'dict[tuple[str, str], InstrumentInfo]'
    # The chart symbol's resolved instrument, pinned on first use.
    _market: 'InstrumentInfo | None'

    # --- WebSocket state (live_provider.py) ---
    # Public market-stream connection. ``None`` until ``connect()`` has run
    # (and again after ``disconnect()`` / a watchdog force-close).
    _public_ws: 'BybitWebSocket | None'
    # Async queue carrying authoritative ``OHLCV`` events — closed bars and
    # reconnect-backfill bars in emission order — plus a ``None`` sentinel
    # when the stream is force-closed so ``watch_ohlcv`` can re-raise.
    # Intra-bar (forming) snapshots do NOT go here; see ``_latest_snapshot``.
    _update_queue: 'asyncio.Queue[OHLCV | None] | None'
    # Coalesced forming-bar snapshot: only the newest intra-bar update
    # carries value, so the dispatcher overwrites this single slot instead
    # of flooding the queue. ``watch_ohlcv`` drains the queue first, so a
    # forming snapshot can never leapfrog a bar close.
    _latest_snapshot: 'OHLCV | None'
    # Wake signal for ``watch_ohlcv``; the data always lives in the queue /
    # snapshot slot, so a missed or spurious set cannot lose an update.
    _data_ready: 'asyncio.Event | None'
    # Stale-feed watchdog task. Cancelled on disconnect.
    _watchdog_task: 'asyncio.Task | None'
    # Most recent closed bar's open timestamp (epoch seconds). Sizes the
    # reconnect REST backfill window and guards closed-bar duplicates.
    _last_closed_bar_ts: int | None
    # Holding pen for WS closed bars while a reconnect backfill is pending
    # (``None`` = no backfill pending). See ``connect()`` for the ordering
    # invariant it protects.
    _pending_closed: 'list[OHLCV] | None'

    # ------------------------------------------------------------------
    # Bybit-private cross-mix-in method surface.
    # Implementation lives in one of the mix-ins; declared here so other
    # mix-ins can call ``self.<name>(...)`` without analyser warnings.
    # ------------------------------------------------------------------

    # --- REST core (rest.py) ---
    def __call__(self, endpoint: str, params: dict | None = None, *,
                 method: str = 'get', body: dict | None = None,
                 auth: bool = False) -> dict: ...

    async def _call(self, endpoint: str, params: dict | None = None, *,
                    method: str = 'get', body: dict | None = None,
                    auth: bool = False) -> dict: ...

    def _get_http_client(self) -> 'httpx.Client': ...

    def _sign_headers(self, payload: str) -> dict[str, str]: ...

    def _close_http_client(self) -> None: ...

    # --- Instrument resolution (provider.py) ---
    def _fetch_instrument(self, category: str, symbol: str) -> 'InstrumentInfo | None': ...

    def _resolve_market(self, symbol: str) -> 'InstrumentInfo': ...

    def _get_market(self) -> 'InstrumentInfo': ...

    # --- Live streaming (live_provider.py) ---
    def _on_ws_message(self, data: dict) -> None: ...

    async def _on_ws_closed(self) -> None: ...

    async def _feed_watchdog_loop(self) -> None: ...

    async def _backfill_gap(self) -> None: ...

    def _release_pending_closed(self) -> None: ...

    def _enqueue_closed(self, queue: 'asyncio.Queue[OHLCV | None]', bar: OHLCV) -> None: ...
