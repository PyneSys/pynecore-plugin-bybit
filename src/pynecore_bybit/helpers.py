"""Static lookup tables, pure time helpers and exact decimal grid helpers
for the Bybit plugin.

Kept separate from ``provider.py`` so timeframe conversions and chunking
constants are reachable without importing the full provider mix-in. Internal
tuning knobs live here as module constants, deliberately NOT in the config
dataclass (see the ``config.py`` module docstring).
"""
from datetime import UTC, datetime
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal

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

# Unified trading account type — the only account type the broker mix-ins
# support (verified on the global demo: ``unifiedMarginStatus: 3``).
ACCOUNT_TYPE_UNIFIED: str = 'UNIFIED'

# ``GET /v5/execution/list`` accepts at most a 7-day ``startTime``/``endTime``
# window per request; the inventory catch-up walks longer gaps in windows of
# this size (milliseconds).
EXECUTION_WINDOW_MS: int = 7 * 86_400 * 1000

# ``GET /v5/execution/list`` page size (server maximum).
EXECUTION_PAGE_LIMIT: int = 100

# Overlap subtracted from the execution-history time cursor on every read
# (milliseconds). A time-scoped cursor can race a fill landing in the same
# millisecond as the previous read's newest row; the ledger dedups on
# ``execId``, so re-reading a short overlap is free and closes the race.
EXECUTION_CURSOR_OVERLAP_MS: int = 60_000

# ``GET /v5/order/realtime`` page size (server maximum for one page).
OPEN_ORDERS_PAGE_LIMIT: int = 50

# Cadence of the spot-inventory reconcile pass (lease heartbeat + balance
# invariant), piggybacked onto the ``watch_orders`` loop.
SPOT_RECONCILE_CADENCE_S: float = 10.0

# Private-stream topics the broker mix-ins consume.
PRIVATE_WS_TOPICS: tuple[str, ...] = ('order', 'execution', 'wallet')

# Reconnect backoff schedule for the private WS (seconds); the last value
# repeats. The public stream's reconnects stay framework-driven — this is
# only for the plugin-owned private stream inside ``watch_orders``.
PRIVATE_WS_BACKOFF_S: tuple[float, ...] = (1.0, 2.0, 5.0, 10.0, 30.0)


def format_decimal(value: Decimal) -> str:
    """Serialize a decimal to Bybit's plain-string wire form.

    Exponent-free, trailing zeros stripped, ``-0`` collapsed — one value,
    one string, exactly the shape Bybit's JSON number-strings use.
    """
    if value == 0:
        return '0'
    text = format(value, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text


def quantize_qty(qty: float, step_str: str) -> Decimal:
    """Floor ``qty`` onto the instrument's base-quantity grid, exactly.

    Uses the raw grid *string* (spot ``basePrecision`` / derivatives
    ``qtyStep``) so the arithmetic is exact for every grid Bybit quotes.
    Flooring (never rounding up) keeps a sell inside the held inventory
    and an entry inside the account's buying power.

    :param qty: Pine-side quantity (base units).
    :param step_str: Raw grid string from ``instruments-info``.
    :return: The floored quantity as an exact :class:`~decimal.Decimal`.
    """
    step = Decimal(step_str)
    if step <= 0:
        return Decimal(str(qty))
    return (Decimal(str(qty)) / step).to_integral_value(ROUND_DOWN) * step


def round_price(price: float, tick_str: str) -> Decimal:
    """Snap ``price`` to the instrument's tick grid (nearest, half up).

    :param price: Pine-side absolute price.
    :param tick_str: Raw ``priceFilter.tickSize`` string.
    :return: The snapped price as an exact :class:`~decimal.Decimal`.
    """
    tick = Decimal(tick_str)
    if tick <= 0:
        return Decimal(str(price))
    return (Decimal(str(price)) / tick).to_integral_value(ROUND_HALF_UP) * tick


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
