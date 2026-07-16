"""Static lookup tables and pure time helpers for the Bybit plugin.

Kept separate from ``provider.py`` so timeframe conversions and chunking
constants are reachable without importing the full provider mix-in. Internal
tuning knobs live here as module constants, deliberately NOT in the config
dataclass (see the ``config.py`` module docstring).
"""
from datetime import UTC, datetime

# TradingView timeframe -> Bybit kline ``interval`` value. Bybit v5 serves
# minute intervals as plain numbers plus D/W/M; there is no 45-minute or
# 4-day bucket, so anything absent here is rejected at startup with
# ``BybitUnsupportedTimeframeError``.
TIMEFRAMES: dict[str, str] = {
    '1': '1',
    '3': '3',
    '5': '5',
    '15': '15',
    '30': '30',
    '60': '60',
    '120': '120',
    '240': '240',
    '360': '360',
    '720': '720',
    '1D': 'D',
    '1W': 'W',
    '1M': 'M',
}

TIMEFRAMES_INV: dict[str, str] = {v: k for k, v in TIMEFRAMES.items()}

# Bar duration in seconds per Bybit interval. ``M`` is absent on purpose:
# calendar months vary in length, use :func:`add_interval` /
# :func:`bar_close_ts` instead of a fixed step.
INTERVAL_SECONDS: dict[str, int] = {
    '1': 60,
    '3': 180,
    '5': 300,
    '15': 900,
    '30': 1800,
    '60': 3600,
    '120': 7200,
    '240': 14400,
    '360': 21600,
    '720': 43200,
    'D': 86400,
    'W': 604800,
}

# ``GET /v5/market/kline`` serves at most this many candles per request.
KLINE_LIMIT: int = 1000

# Bybit v5 product categories the plugin serves. ``option`` is excluded:
# the kline endpoint does not accept it, so there is no data-provider path.
CATEGORY_SPOT: str = 'spot'
CATEGORY_LINEAR: str = 'linear'
CATEGORY_INVERSE: str = 'inverse'
CATEGORIES: tuple[str, ...] = (CATEGORY_SPOT, CATEGORY_LINEAR, CATEGORY_INVERSE)

# ``GET /v5/market/instruments-info`` page size (server maximum).
INSTRUMENTS_PAGE_LIMIT: int = 1000

# REST request timeout in seconds. Public market endpoints answer in well
# under a second; the generous value covers transient slowness without
# stalling the caller forever.
REST_TIMEOUT_S: float = 30.0

# ``X-BAPI-RECV-WINDOW`` for signed requests, in milliseconds.
RECV_WINDOW_MS: int = 5000

# Outbound ``{"op": "ping"}`` cadence on WS connections. Bybit documents a
# 20-second ping recommendation; the server drops silent connections.
WS_PING_INTERVAL_S: float = 20.0

# Seconds of total inbound WS silence past which the stream is treated as
# half-open and force-closed. With pings answered every ~20 s, 60 s of
# silence means three missed pongs — a dead transport by Bybit's cadence.
WS_STALE_THRESHOLD_S: float = 60.0


def add_interval(ts: int, interval: str, n: int) -> int:
    """Return the epoch-seconds timestamp ``n`` bars after ``ts``.

    Fixed-length intervals advance arithmetically; calendar months advance
    on UTC month boundaries (Bybit ``M`` bars open on the first of the
    month, 00:00 UTC).

    :param ts: Bar-open epoch seconds (for ``M`` it must be a month start).
    :param interval: Bybit kline interval value.
    :param n: Number of bars to advance (may be 0).
    :return: Epoch seconds of the bar open ``n`` bars later.
    """
    seconds = INTERVAL_SECONDS.get(interval)
    if seconds is not None:
        return ts + n * seconds
    dt = datetime.fromtimestamp(ts, UTC)
    month_index = dt.year * 12 + (dt.month - 1) + n
    return int(datetime(month_index // 12, month_index % 12 + 1, 1, tzinfo=UTC).timestamp())


def bar_close_ts(bar_start: int, interval: str) -> int:
    """Return the epoch-seconds close time of the bar opening at ``bar_start``.

    :param bar_start: Bar-open epoch seconds.
    :param interval: Bybit kline interval value.
    :return: Epoch seconds at which the bar closes (== next bar's open).
    """
    return add_interval(bar_start, interval, 1)
