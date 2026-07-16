"""Configuration dataclass for the Bybit v5 plugin.

One credential block (``api_key`` / ``api_secret``) serves both the
data-ingest and the order-execution side. The live/demo axis and the
regional-entity axis are independent (see :mod:`pynecore_bybit.hosts`):
``demo`` picks the paper-trade environment, ``region`` picks the legal
entity / host family, and the ``(region, demo)`` pair resolves to a REST +
public-WS + private-WS host triple.

Internal tuning knobs (WS ping cadence, reconnect timing, request timeouts,
instrument-rule cache TTLs) live as module-level constants in
:mod:`pynecore_bybit.helpers`, deliberately NOT in this dataclass: they have
no user-facing reason to be touched, and exposing them as config fields
balloons the user TOML with knobs the user does not understand.
"""
from dataclasses import dataclass

from pynecore.core.plugin import LiveProviderConfig


@dataclass
class BybitConfig(LiveProviderConfig):
    """Bybit v5 plugin configuration.

    Covers both the data-ingest and the order-execution side across the
    spot, linear and inverse categories; one API key pair serves both.
    ``symbol_map`` (TradingView key -> native Bybit symbol, e.g.
    ``"BYBIT:BTCUSDT" = "BTCUSDT"``) is inherited from
    :class:`LiveProviderConfig`.
    """

    demo: bool = True
    """Use the demo environment (``api-demo.*`` hosts) instead of live. Demo
    runs on real mainnet prices with simulated balances — the supported
    paper-trade path. Set to ``false`` only to trade with real funds."""

    region: str = "global"
    """Legal-entity / host family for your account, decided by your account's
    residency — it is NOT auto-detected. One of: ``global`` (api.bybit.com),
    ``eu`` (api.bybit.eu, Bybit EU / MiCA), ``nl``, ``tr``, ``kz``, ``ge``,
    ``ae``, ``id``, or ``testnet``. EEA residents use ``eu``."""

    api_key: str = ""
    """API key from Bybit (API Management). For demo, generate it while the
    account is switched to Demo Trading."""

    api_secret: str = ""
    """API secret paired with ``api_key``, used for HMAC-SHA256 request
    signing."""

    rest_host: str = ""
    """Optional REST host override (e.g. ``api.bytick.com``). Leave empty to
    resolve the host from the ``(region, demo)`` pair. Escape hatch for
    unlisted regional domains without a table change."""

    ws_public_host: str = ""
    """Optional public-WebSocket host override. Leave empty to resolve from
    ``(region, demo)``."""

    ws_private_host: str = ""
    """Optional private-WebSocket host override. Leave empty to resolve from
    ``(region, demo)``."""
