"""Provider-side surface: timeframe converters, symbol/category resolution,
symbol metadata, REST OHLCV download.

Carries everything that produces *historical* market data and ``SymInfo``
through the Bybit v5 market endpoints (all public — no credentials needed
for the data-provider path). Live WebSocket streaming lives in the
``live_provider`` mix-in.

Symbol notation
---------------

One Bybit name can exist in several categories at once (``BTCUSDT`` is both
a spot pair and a linear perpetual), so the user-facing symbol carries the
disambiguation, mirroring TradingView's convention:

* plain name (``BTCUSDT``) — spot first, then linear, then inverse;
* ``.P`` suffix (``BTCUSDT.P``, ``BTCUSD.P``) — perpetual: linear first,
  then inverse;
* dated futures use their native Bybit names (``BTCUSDT-26JUN26``, ...) —
  these exist in a single category, the plain probe order finds them;
* an optional leading ``BYBIT:`` TradingView prefix is stripped.

Resolution consults ``config.symbol_map`` first (inherited translation
table; mapped values are treated as already-native names and still accept
the ``.P`` suffix), then probes ``instruments-info`` per category in the
order above. The first category that lists the name wins.

State touched: ``_instruments`` (per ``(category, symbol)``
:class:`~.models.InstrumentInfo` cache), ``_market`` (the chart symbol's
resolved instrument, pinned on first use).
"""
from datetime import UTC, datetime, time
from typing import Callable, Literal

from pynecore.core.plugin import override
from pynecore.core.syminfo import SymInfo, SymInfoInterval, SymInfoSession
from pynecore.types.ohlcv import OHLCV

from ._base import _BybitBase
from .exceptions import (
    BybitAPIError,
    BybitSymbolError,
    BybitUnsupportedTimeframeError,
)
from .helpers import (
    CATEGORIES,
    CATEGORY_INVERSE,
    CATEGORY_LINEAR,
    CATEGORY_SPOT,
    INSTRUMENTS_PAGE_LIMIT,
    KLINE_LIMIT,
    TIMEFRAMES,
    TIMEFRAMES_INV,
    add_interval,
    bar_close_ts,
)
from .models import InstrumentInfo, parse_instrument

#: ``retCode`` Bybit answers when a query parameter (including an unknown
#: symbol on some categories) fails validation — treated as "not listed in
#: this category" during resolution probes.
_PARAMS_ERROR_CODE = 10001

# Crypto trades 24/7. The framework's ``_check_session`` treats
# ``start == end`` as a zero-length, closed-day window, so encode every
# weekday as ``00:00 - 23:59:59`` — the same convention the Coinbase and
# Capital.com plugins use. The session anchors must not be empty either:
# ``pynecore.lib.timeframe`` helpers scan ``_session_starts`` to locate the
# per-day session anchor, and an empty list hangs ``timeframe.change("D")``.
_24_7_OPENING_HOURS: tuple[SymInfoInterval, ...] = tuple(
    SymInfoInterval(day=day, start=time(0, 0, 0), end=time(23, 59, 59))
    for day in range(7)
)
_24_7_SESSION_STARTS: tuple[SymInfoSession, ...] = tuple(
    SymInfoSession(day=day, time=time(0, 0, 0)) for day in range(7)
)
_24_7_SESSION_ENDS: tuple[SymInfoSession, ...] = tuple(
    SymInfoSession(day=day, time=time(23, 59, 59)) for day in range(7)
)


class _ProviderMixin(_BybitBase):
    """Provider mix-in: timeframe converters, symbol metadata, REST OHLCV."""

    # --- timeframe helpers --------------------------------------------------

    @classmethod
    @override
    def to_tradingview_timeframe(cls, timeframe: str) -> str:
        """Convert a Bybit kline ``interval`` value to TradingView format."""
        try:
            return TIMEFRAMES_INV[timeframe]
        except KeyError:
            raise BybitUnsupportedTimeframeError(
                f"Unsupported Bybit kline interval: {timeframe!r}",
            )

    @classmethod
    @override
    def to_exchange_timeframe(cls, timeframe: str) -> str:
        """Convert a TradingView timeframe to a Bybit kline ``interval`` value."""
        interval = TIMEFRAMES.get(timeframe)
        if interval is None:
            raise BybitUnsupportedTimeframeError(
                f"Unsupported timeframe for Bybit: {timeframe!r}. "
                f"Supported: {', '.join(TIMEFRAMES)}",
            )
        return interval

    # --- symbol / category resolution ---------------------------------------

    def _fetch_instrument(self, category: str, symbol: str) -> InstrumentInfo | None:
        """Probe one category for ``symbol`` via ``instruments-info``.

        :return: The normalized instrument, or ``None`` when the category
            does not list the symbol. Bybit signals "unknown symbol" as an
            empty ``result.list`` on some categories and as a
            ``retCode=10001`` params error on others — both map to ``None``;
            every other API error propagates.
        """
        cached = self._instruments.get((category, symbol))
        if cached is not None:
            return cached
        try:
            result = self('/v5/market/instruments-info',
                          {'category': category, 'symbol': symbol})
        except BybitAPIError as e:
            if e.ret_code == _PARAMS_ERROR_CODE:
                return None
            raise
        entries = result.get('list') or []
        if not entries:
            return None
        info = parse_instrument(category, entries[0])
        self._instruments[(category, info.symbol)] = info
        return info

    def _resolve_market(self, symbol: str) -> InstrumentInfo:
        """Resolve a user-facing symbol to its Bybit ``(category, name)``.

        Applies the notation rules from the module docstring. The result for
        the chart symbol is pinned on ``self._market`` by the callers, but
        this method itself is stateless apart from the instrument cache, so
        ``request.security`` symbols resolve through the same path.

        :param symbol: Symbol as configured by the user or mapped through
            ``symbol_map``.
        :return: The resolved instrument record.
        :raises BybitSymbolError: When no supported category lists the symbol.
        """
        mapped = self.resolve_symbol(symbol)
        name = mapped.strip()
        if name.upper().startswith('BYBIT:'):
            name = name[len('BYBIT:'):]
        name = name.upper()

        if name.endswith('.P'):
            name = name[:-2]
            probe_order = (CATEGORY_LINEAR, CATEGORY_INVERSE)
        else:
            probe_order = CATEGORIES

        for category in probe_order:
            info = self._fetch_instrument(category, name)
            if info is not None:
                return info
        raise BybitSymbolError(
            f"Symbol {symbol!r} is not listed on Bybit in any supported "
            f"category ({', '.join(probe_order)}). Spot pairs use the plain "
            f"name (BTCUSDT), perpetuals the .P suffix (BTCUSDT.P), dated "
            f"futures their native Bybit name (BTCUSDT-26JUN26)."
        )

    def _get_market(self) -> InstrumentInfo:
        """Return the chart symbol's resolved instrument, pinning on first use."""
        market = self._market
        if market is None:
            if not self.symbol:
                raise BybitSymbolError("Bybit provider has no symbol configured")
            market = self._resolve_market(self.symbol)
            self._market = market
        return market

    # --- symbol metadata -----------------------------------------------------

    @override
    def get_list_of_symbols(self, *args, category: str | None = None) -> list[str]:
        """Return all tradable Bybit symbols in user-facing notation.

        Walks ``instruments-info`` with cursor pagination. Spot pairs are
        listed under their plain name, perpetuals with the ``.P`` suffix and
        dated futures under their native name — the same notation
        :meth:`_resolve_market` accepts, so every listed entry round-trips.
        Non-``Trading`` instruments are excluded.

        :param category: Restrict to one Bybit category
            (``spot`` / ``linear`` / ``inverse``); ``None`` lists all three.
        """
        categories = (category,) if category else CATEGORIES
        out: list[str] = []
        for cat in categories:
            cursor: str | None = None
            while True:
                result = self('/v5/market/instruments-info', {
                    'category': cat,
                    'limit': INSTRUMENTS_PAGE_LIMIT,
                    'cursor': cursor,
                })
                for entry in result.get('list') or []:
                    info = parse_instrument(cat, entry)
                    if info.status != 'Trading' or not info.symbol:
                        continue
                    out.append(f"{info.symbol}.P" if info.is_perpetual else info.symbol)
                cursor = result.get('nextPageCursor') or None
                if not cursor:
                    break
        out.sort()
        return out

    @override
    def update_symbol_info(self) -> SymInfo:
        """Fetch instrument metadata and synthesize a PyneCore :class:`SymInfo`.

        Uses ``self.symbol`` / ``self.timeframe`` populated by
        :class:`~pynecore.core.plugin.provider.ProviderPlugin.__init__`.
        All Bybit categories trade 24/7 (dated futures too — crypto has no
        exchange sessions), so the schedule is the flat 24/7 template.
        """
        assert self.timeframe is not None, "Bybit.timeframe must be set before update_symbol_info"
        market = self._get_market()
        if market.status != 'Trading':
            raise BybitSymbolError(
                f"Bybit instrument {market.symbol!r} ({market.category}) is "
                f"not tradable (status={market.status!r}) — cannot generate SymInfo"
            )
        if market.tick_size <= 0.0:
            raise BybitSymbolError(
                f"Bybit instrument {market.symbol!r} reports no tickSize — "
                f"cannot derive mintick"
            )
        minmove, pricescale = market.price_grid()

        ticker = f"{market.symbol}.P" if market.is_perpetual else market.symbol
        # Inverse contracts are denominated in whole USD contracts and settle
        # in the base coin; ``pointvalue`` stays 1.0 for M1 (data-only) —
        # the settle-coin accounting model is an M4 (broker) concern.
        return SymInfo(
            prefix='BYBIT',
            description=f"{market.base_coin}/{market.quote_coin}"
                        f"{' Perpetual' if market.is_perpetual else ''}",
            ticker=ticker,
            currency=market.quote_coin,
            basecurrency=market.base_coin or None,
            period=self.timeframe,
            type=_syminfo_type_for(market),
            # Kline volume is base-denominated for spot and linear, but
            # QUOTE-denominated (whole-USD contracts) for inverse contracts
            # — verified live: inverse volume/turnover ~= price.
            volumetype='quote' if market.category == CATEGORY_INVERSE else 'base',
            mintick=market.tick_size,
            pricescale=pricescale,
            minmove=minmove,
            pointvalue=1.0,
            # The order-quantity grid (basePrecision / qtyStep); 0.0 lets the
            # provider chain fall back to estimation when Bybit omits it.
            mincontract=market.qty_step,
            opening_hours=list(_24_7_OPENING_HOURS),
            session_starts=list(_24_7_SESSION_STARTS),
            session_ends=list(_24_7_SESSION_ENDS),
            timezone=self.timezone,
            expiration_date=market.delivery_time,
        )

    # --- OHLCV download ------------------------------------------------------

    @override
    def download_ohlcv(self, time_from: datetime, time_to: datetime,
                       on_progress: Callable[[datetime], None] | None = None,
                       limit: int | None = None, with_extra: bool = False):
        """Download OHLCV candles between ``time_from`` and ``time_to``.

        ``with_extra`` is ignored; Bybit klines have no extra per-bar fields.

        ``GET /v5/market/kline`` serves at most :data:`~.helpers.KLINE_LIMIT`
        candles per request and, when a window holds more, returns the
        NEWEST ones — so the window is walked forward in chunks sized to
        exactly ``limit`` bar opens (calendar-aware for monthly bars).
        Bybit returns candles newest-first with millisecond epoch strings;
        they are re-sorted ascending before writing. A still-forming
        trailing bar is dropped so the warmup file only contains closed
        candles — otherwise the first live update would arrive for an
        already-written timestamp and freeze ``bar_index`` at the boundary.
        """
        assert self.timeframe is not None
        market = self._get_market()
        interval = self.xchg_timeframe or self.to_exchange_timeframe(self.timeframe)

        effective_limit = limit if limit is not None and limit > 0 else KLINE_LIMIT
        effective_limit = max(2, min(effective_limit, KLINE_LIMIT))

        # Naive datetimes are UTC by framework convention.
        start_dt = time_from.replace(tzinfo=UTC) if time_from.tzinfo is None \
            else time_from.astimezone(UTC)
        end_dt = time_to.replace(tzinfo=UTC) if time_to.tzinfo is None \
            else time_to.astimezone(UTC)
        end_dt = min(end_dt, datetime.now(UTC))

        cursor = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        now_ts = int(datetime.now(UTC).timestamp())
        last_written: int | None = None

        while cursor < end_ts:
            chunk_end = min(add_interval(cursor, interval, effective_limit - 1), end_ts)
            if on_progress is not None:
                on_progress(datetime.fromtimestamp(cursor, UTC).replace(tzinfo=None))

            result = self('/v5/market/kline', {
                'category': market.category,
                'symbol': market.symbol,
                'interval': interval,
                'start': cursor * 1000,
                'end': chunk_end * 1000,
                'limit': effective_limit,
            })
            rows = result.get('list') or []
            if not rows:
                # No candles in this window (pre-listing gap or sparse dated
                # future). Advance so the loop terminates instead of spinning.
                cursor = max(chunk_end, add_interval(cursor, interval, 1))
                continue

            # Bybit returns newest-first; ascend before writing.
            for row in sorted(rows, key=lambda r: int(r[0])):
                bar_start = int(row[0]) // 1000
                if bar_start < cursor:
                    # Defensive: stay strictly within the requested window.
                    continue
                if bar_close_ts(bar_start, interval) > now_ts:
                    # Still-forming bar — do not persist as historical data.
                    continue
                if last_written is not None and bar_start <= last_written:
                    # Boundary bucket served by both adjacent windows.
                    continue
                self.save_ohlcv_data(OHLCV(
                    timestamp=bar_start,
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                ))
                last_written = bar_start

            if last_written is not None and last_written >= cursor:
                cursor = add_interval(last_written, interval, 1)
            else:
                cursor = max(chunk_end, add_interval(cursor, interval, 1))

        if on_progress is not None:
            on_progress(end_dt.replace(tzinfo=None))


def _syminfo_type_for(market: InstrumentInfo) -> Literal['crypto', 'swap', 'futures']:
    """Map an instrument to the PyneCore ``SymInfo.type`` literal.

    Spot pairs are ``"crypto"`` (matching the Coinbase plugin and the
    ``default_mincontract`` heuristic), perpetual contracts ``"swap"`` and
    dated futures ``"futures"`` — the TradingView bucket names.
    """
    if market.category == CATEGORY_SPOT:
        return 'crypto'
    return 'swap' if market.is_perpetual else 'futures'
