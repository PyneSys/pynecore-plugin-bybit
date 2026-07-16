"""Bybit v5 integration for PyneCore.

Implementation follows the plan in
``docs/pynecore/plugin-system/broker/bybit-broker-plan.md`` — a thin
``httpx`` + ``websockets`` client over the Bybit v5 Open API (no vendor
SDK), composed from concern-scoped mix-ins on top of a shared
:class:`._base._BybitBase`, mirroring the Capital.com / cTrader layout.

The provider side serves historical OHLCV download, ``SymInfo`` synthesis
and live WebSocket kline streaming for the ``spot``, ``linear`` and
``inverse`` categories. The broker side (M2) covers spot order execution
— entries, SOFTWARE exit brackets, cancels, in-place amends, the private
order/execution event stream and the core spot-inventory integration;
the derivatives execution models land in the next milestones.
"""
import asyncio
from pathlib import Path

from ._base import _BybitBase
from .config import BybitConfig
from .events import _EventStreamMixin
from .execution import _ExecutionMixin
from .hosts import resolve_hosts
from .live_provider import _LiveProviderMixin
from .provider import _ProviderMixin
from .rest import _RestMixin
from .state import _StateMixin

__all__ = [
    'Bybit',
]


class Bybit(
    _EventStreamMixin,
    _ExecutionMixin,
    _StateMixin,
    _LiveProviderMixin,
    _ProviderMixin,
    _RestMixin,
    _BybitBase,
):
    """Bybit v5 plugin for PyneCore.

    Provides historical OHLCV download, ``SymInfo`` and live WebSocket
    market-data streaming for Bybit spot, linear and inverse instruments,
    plus live spot order execution over the same key pair. Spot pairs use
    the plain symbol (``BTCUSDT``), perpetuals the ``.P`` suffix
    (``BTCUSDT.P``), dated futures their native Bybit name.

    Spot execution integrates the core spot-inventory layer: the plugin
    exposes a :class:`~pynecore.core.broker.spot_inventory.SpotInventoryPort`
    over ``/v5/execution/list`` + ``/v5/account/wallet-balance`` and the
    core :class:`~pynecore.core.broker.spot_inventory.SpotInventoryManager`
    owns the fill ledger, the balance invariant and the position
    synthesis. Order idempotency is exchange-native via ``orderLinkId``.
    """

    def __init__(self, *, symbol: str | None = None, timeframe: str | None = None,
                 ohlcv_dir: Path | None = None, config: BybitConfig | None = None):
        """
        :param symbol: The Bybit symbol in user-facing notation.
        :param timeframe: The timeframe in TradingView format.
        :param ohlcv_dir: The directory to save OHLCV data.
        :param config: Pre-loaded :class:`BybitConfig` instance.
        """
        super().__init__(symbol=symbol, timeframe=timeframe,
                         ohlcv_dir=ohlcv_dir, config=config)
        # Explicit raise instead of ``assert``: the check is a startup
        # config-contract gate and must survive ``python -O``.
        if not isinstance(self.config, BybitConfig):
            raise TypeError(
                f"BybitConfig is required, got {type(self.config).__name__}"
            )
        self._hosts = resolve_hosts(
            self.config.region,
            demo=self.config.demo,
            rest_host=self.config.rest_host,
            ws_public_host=self.config.ws_public_host,
            ws_private_host=self.config.ws_private_host,
        )

        # REST state (built lazily by ``_RestMixin``).
        self._http_client = None

        # Instrument resolution caches.
        self._instruments = {}
        self._market = None

        # Live streaming state (set on connect by ``_LiveProviderMixin``).
        self._public_ws = None
        self._update_queue = None
        self._latest_snapshot = None
        self._data_ready = None
        self._watchdog_task = None
        self._last_closed_bar_ts = None
        self._pending_closed = None
        self._last_price = None

        # Broker state (driven by the state/execution/events mix-ins).
        self._private_ws = None
        self._private_events = None
        self._spot_manager = None
        self._spot_port = None
        self._broker_started = False
        self._broker_start_lock = asyncio.Lock()
        self._order_identity = {}
        self._seen_exec_ids = set()
        self._dispatch_qty = {}
        self._filled_cum = {}
