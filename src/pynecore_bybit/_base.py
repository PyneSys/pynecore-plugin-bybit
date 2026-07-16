"""Type-only shared base class for the Bybit plugin's mix-ins.

Mirrors the Capital.com / cTrader plugin layout — every mix-in inherits
from :class:`_BybitBase` so static analysers (PyCharm, pyright) can resolve
``self.<attr>`` and Bybit-private cross-mix-in method calls without
warnings. The class declares:

* Every instance attribute the constructor sets, as a class-level type
  annotation. The runtime ``__init__`` of the final :class:`Bybit` class
  assigns concrete values; the annotations exist purely for the type
  system.

* Bybit-private method signatures that one mix-in calls on another (with
  ``...`` body). The implementation lives in whichever mix-in owns the
  concern.

Since M2 the base derives from :class:`~pynecore.core.plugin.broker.BrokerPlugin`
(which extends the live-provider surface), so the provider mix-ins keep
working unchanged while the broker mix-ins add the execution side.
"""
import asyncio
from typing import TYPE_CHECKING

from pynecore.core.broker.models import LegType
from pynecore.core.plugin.broker import BrokerPlugin
from pynecore.types.ohlcv import OHLCV

from .config import BybitConfig

if TYPE_CHECKING:
    from decimal import Decimal
    from typing import Callable

    import httpx

    from pynecore.core.broker.models import (
        DispatchEnvelope,
        EntryIntent,
        ExchangeOrder,
        ExchangePosition,
        PositionLeg,
    )
    from pynecore.core.broker.spot_inventory import SpotInventoryManager

    from .hosts import BybitHosts
    from .inventory import _BybitSpotPort
    from .models import InstrumentInfo
    from .ws import BybitWebSocket


class _BybitBase(BrokerPlugin[BybitConfig]):
    """Shared instance state + Bybit-private cross-mix-in surface.

    Concrete implementations live in the individual mix-ins; this class
    declares the attribute and method surface so static analysis resolves
    ``self.<x>`` references uniformly across mix-ins.
    """

    plugin_name = "Bybit"
    Config = BybitConfig
    timezone = 'UTC'

    # ``orderLinkId`` accepts up to 36 characters (letters, digits, dash,
    # underscore) — wider than the canonical 30, so canonical ids pass
    # through unshortened.
    client_order_id_max_len = 36

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
    # shared by symbol resolution, SymInfo synthesis and order quantization.
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
    # Most recent trade price observed on the kline stream (forming or
    # closed bar close). Feeds the spot position mark and the market-order
    # minimum-notional pre-check; ``None`` before the first push.
    _last_price: float | None

    # --- Broker state (state.py / execution.py / events.py) ---
    # Private-stream connection, owned by the ``watch_orders`` loop.
    _private_ws: 'BybitWebSocket | None'
    # Raw private-stream frames (order / execution / wallet topics),
    # produced by the WS callback, consumed by ``watch_orders``. ``None``
    # sentinel = the private transport died (reconnect needed).
    _private_events: 'asyncio.Queue[dict | None] | None'
    # Core spot inventory manager, constructed on the first broker call
    # after ``store_ctx`` is available. ``None`` without persistence.
    _spot_manager: 'SpotInventoryManager | None'
    # The inventory port instance behind ``_spot_manager`` — the event
    # stream reuses its execution-row parser. ``None`` alongside the manager.
    _spot_port: '_BybitSpotPort | None'
    # One-shot guard + lock around the broker-startup sequence
    # (manager construction + ``startup()``), shared by every entry point
    # on the broker event loop.
    _broker_started: bool
    _broker_start_lock: asyncio.Lock
    # In-memory Pine-identity index for dispatched orders, keyed by the
    # ``orderLinkId``: ``(pine_id, from_entry, leg_type)``. The BrokerStore
    # rows are the durable copy; this map serves the persistence-off test
    # paths and saves a store read per event.
    _order_identity: 'dict[str, tuple[str | None, str | None, LegType]]'
    # ``execId`` values already booked/emitted, bounded replay dedup for
    # the private execution stream (the ledger dedups durably; this saves
    # the store round-trip on the common echo).
    _seen_exec_ids: 'set[str]'
    # Dispatch quantity + cumulative fill per ``orderLinkId`` — the
    # in-memory partial-vs-filled discriminator behind the BrokerStore's
    # durable ``filled_qty`` cursor (and its stand-in when persistence
    # is off).
    _dispatch_qty: 'dict[str, float]'
    _filled_cum: 'dict[str, float]'
    # Account position mode of the linear category, detected once by
    # ``_ensure_broker_started`` (``positions.POSITION_MODE_*``). ``None``
    # before broker startup and on spot runs.
    _position_mode: str | None
    # Last-known venue position size per ``positionIdx`` (linear only),
    # fed by the private WS ``position`` topic and the reconcile snapshot.
    # ``None`` until the first snapshot — the entry-row flat sweep must
    # never fire on ignorance.
    _linear_sizes: 'dict[int, float] | None'

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

    # --- Broker lifecycle (state.py) ---
    def _spot_market(self) -> 'InstrumentInfo': ...

    def _broker_market(self) -> 'InstrumentInfo': ...

    async def _ensure_broker_started(self) -> None: ...

    async def _fetch_wallet_coin(self, coin: str) -> dict: ...

    # --- Linear position path (positions.py) ---
    async def _detect_position_mode(self, market: 'InstrumentInfo') -> str: ...

    async def _fetch_position_rows(self, market: 'InstrumentInfo') -> list[dict]: ...

    def _ingest_position_sizes(self, rows: list[dict]) -> None: ...

    def _linear_is_flat(self) -> bool: ...

    async def _fetch_linear_position(
            self, market: 'InstrumentInfo',
    ) -> 'ExchangePosition | None': ...

    # PositionPort transport primitives (hedge-mode one-way emulation) —
    # declared on the base so ``position_port = self`` satisfies the core
    # Protocol from any mix-in (the cTrader plugin's pattern).
    async def fetch_raw_positions(self, symbol: str) -> 'list[PositionLeg]': ...

    async def get_volume_quantizer(
            self, symbol: str,
    ) -> 'Callable[[float], int]': ...

    async def close_leg(
            self, symbol: str, leg_id: str, volume: int, coid: str,
    ) -> None: ...

    async def reject_out_of_range(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> None: ...

    async def place_leg(
            self, envelope: 'DispatchEnvelope', qty: float,
    ) -> 'list[ExchangeOrder]': ...

    async def amend_bracket(
            self, symbol: str, leg_id: str, *,
            side: str,
            tp_price: float | None,
            sl_price: float | None,
            trail_offset: float | None,
            coid: str,
    ) -> None: ...

    # --- Execution internals (execution.py) ---
    def _record_identity(self, coid: str, *, pine_id: str | None,
                         from_entry: str | None, leg_type: LegType,
                         qty: float) -> None: ...

    def _resolve_identity(
            self, order_link_id: str | None, order_id: str | None,
    ) -> 'tuple[str | None, str | None, LegType | None]': ...

    async def _order_post(self, endpoint: str, body: dict, *,
                          coid: str, context: str) -> dict: ...

    async def _lookup_order_by_coid(self, coid: str) -> dict | None: ...

    def _quantize_or_skip(self, market: 'InstrumentInfo', qty: float, *,
                          intent_key: str, label: str) -> 'Decimal': ...

    def _preflight_order(self, market: 'InstrumentInfo', qty: 'Decimal', *,
                         is_market: bool, price: 'Decimal | None',
                         intent_key: str, label: str) -> None: ...

    async def _place_entry_order(
            self, envelope: 'DispatchEnvelope', intent: 'EntryIntent',
            market: 'InstrumentInfo', qty: 'Decimal',
    ) -> 'list[ExchangeOrder]': ...
