"""Bybit v5 integration for PyneCore.

Implementation follows the plan in
``docs/pynecore/plugin-system/broker/bybit-broker-plan.md`` — a thin
``httpx`` + ``websockets`` client over the Bybit v5 Open API (no vendor
SDK), composed from concern-scoped mix-ins on top of a shared
:class:`._base._BybitBase`, mirroring the Capital.com / Coinbase layout.

The provider side (this milestone) serves historical OHLCV download,
``SymInfo`` synthesis and live WebSocket kline streaming for the ``spot``,
``linear`` and ``inverse`` categories; the broker side (order execution
over the same key pair) lands in the next milestones.
"""
from pathlib import Path

from ._base import _BybitBase
from .config import BybitConfig
from .hosts import resolve_hosts
from .live_provider import _LiveProviderMixin
from .provider import _ProviderMixin
from .rest import _RestMixin

__all__ = [
    'Bybit',
]


class Bybit(
    _LiveProviderMixin,
    _ProviderMixin,
    _RestMixin,
    _BybitBase,
):
    """Bybit v5 plugin for PyneCore.

    Provides historical OHLCV download, ``SymInfo`` and live WebSocket
    market-data streaming for Bybit spot, linear and inverse instruments.
    Spot pairs use the plain symbol (``BTCUSDT``), perpetuals the ``.P``
    suffix (``BTCUSDT.P``), dated futures their native Bybit name.
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
