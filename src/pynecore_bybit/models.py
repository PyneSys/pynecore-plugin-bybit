"""Internal models for the Bybit plugin.

Plugin-private dataclasses that normalize ``GET /v5/market/instruments-info``
response shapes into stable internal types. Nothing in here leaks to the
PyneCore plugin surface — those return core types (``OHLCV``, ``SymInfo``).

The three categories carry different rule fields (spot:
``lotSizeFilter.basePrecision`` / ``minOrderAmt`` / ``maxLimitOrderQty`` /
``maxMarketOrderQty``; linear/inverse: ``qtyStep`` / ``minOrderQty`` /
``maxOrderQty`` / ``maxMktOrderQty`` / ``minNotionalValue`` +
``contractType`` / ``settleCoin`` / ``deliveryTime``), normalized here into
one record so downstream code never touches raw JSON field names.
"""
from dataclasses import dataclass
from math import gcd

from .exceptions import BybitSymbolError
from .helpers import CATEGORY_SPOT


@dataclass(slots=True)
class InstrumentInfo:
    """Normalized snapshot of one Bybit instrument's tick / lot / contract rules.

    Built from one ``instruments-info`` list entry and cached on the plugin
    instance under ``(category, symbol)``. Money/qty fields arrive as JSON
    strings; they are parsed to float here, with the raw ``tickSize`` string
    kept so ``minmove``/``pricescale`` can be derived exactly from its
    decimal representation (see :meth:`price_grid`).

    :ivar category: Bybit v5 category (``spot`` / ``linear`` / ``inverse``).
    :ivar symbol: Native Bybit symbol (e.g. ``"BTCUSDT"``).
    :ivar base_coin: Base asset — fills ``SymInfo.basecurrency``.
    :ivar quote_coin: Quote asset — fills ``SymInfo.currency``.
    :ivar settle_coin: Settlement asset (derivatives; ``""`` for spot).
    :ivar status: Raw instrument status (``"Trading"`` when tradable).
    :ivar tick_size_str: Raw ``priceFilter.tickSize`` string.
    :ivar tick_size: ``tick_size_str`` parsed as float — ``SymInfo.mintick``.
    :ivar qty_step: Order-quantity grid: ``lotSizeFilter.basePrecision``
        (spot) or ``lotSizeFilter.qtyStep`` (derivatives). Drives
        ``SymInfo.mincontract``.
    :ivar min_order_qty: Minimum order quantity in base units (derivatives;
        0.0 for spot, where the minimum is quote-denominated instead).
    :ivar min_order_amt: Minimum order amount in QUOTE units (spot
        ``minOrderAmt``; 0.0 for derivatives).
    :ivar min_notional: Minimum order notional in quote units (derivatives
        ``minNotionalValue``; 0.0 for spot).
    :ivar max_limit_order_qty: Per-order qty ceiling for limit orders
        (spot ``maxLimitOrderQty`` / derivatives ``maxOrderQty``).
    :ivar max_market_order_qty: Per-order qty ceiling for market orders
        (spot ``maxMarketOrderQty`` / derivatives ``maxMktOrderQty``).
    :ivar contract_type: Raw ``contractType`` for derivatives
        (``"LinearPerpetual"``, ``"LinearFutures"``, ``"InversePerpetual"``,
        ``"InverseFutures"``); ``""`` for spot.
    :ivar delivery_time: Contract delivery epoch seconds for dated futures;
        ``None`` for spot and perpetuals (Bybit sends ``"0"``).
    """

    category: str
    symbol: str
    base_coin: str
    quote_coin: str
    settle_coin: str
    status: str
    tick_size_str: str
    tick_size: float
    qty_step: float
    min_order_qty: float
    min_order_amt: float
    min_notional: float
    max_limit_order_qty: float
    max_market_order_qty: float
    contract_type: str
    delivery_time: int | None

    @property
    def is_perpetual(self) -> bool:
        """Whether this is a perpetual contract (linear or inverse)."""
        return self.contract_type.endswith('Perpetual')

    def price_grid(self) -> tuple[int, int]:
        """Derive the exact ``(minmove, pricescale)`` pair from the tick string.

        PyneCore requires ``mintick == minmove / pricescale``. Deriving the
        pair from the decimal *string* representation is exact for every
        tick Bybit quotes — ``"0.10"`` -> (10, 100) -> reduced (1, 10),
        ``"0.5"`` -> (5, 10), ``"5"`` -> (5, 1) — including ticks that are
        not integer-reciprocal (where a float round-trip would fail).

        :return: ``(minmove, pricescale)`` reduced to lowest terms.
        :raises BybitSymbolError: If the tick string is not a positive
            decimal number.
        """
        text = self.tick_size_str.strip()
        if not text or not text.replace('.', '', 1).isdigit():
            raise BybitSymbolError(
                f"Bybit instrument {self.symbol!r} reports unparsable "
                f"tickSize {self.tick_size_str!r}"
            )
        int_part, _, frac_part = text.partition('.')
        pricescale = 10 ** len(frac_part)
        minmove = int(int_part or '0') * pricescale + int(frac_part or '0')
        if minmove <= 0:
            raise BybitSymbolError(
                f"Bybit instrument {self.symbol!r} reports non-positive "
                f"tickSize {self.tick_size_str!r}"
            )
        divisor = gcd(minmove, pricescale)
        return minmove // divisor, pricescale // divisor


def parse_instrument(category: str, entry: dict) -> InstrumentInfo:
    """Normalize one raw ``instruments-info`` list entry.

    :param category: The category the entry was fetched under.
    :param entry: One element of ``result.list``.
    :return: The normalized :class:`InstrumentInfo`.
    """
    price_filter = entry.get('priceFilter') or {}
    lot = entry.get('lotSizeFilter') or {}
    tick_str = str(price_filter.get('tickSize') or '')
    if category == CATEGORY_SPOT:
        qty_step = _to_float(lot.get('basePrecision'))
        min_order_qty = 0.0
        min_order_amt = _to_float(lot.get('minOrderAmt'))
        min_notional = 0.0
        max_limit_qty = _to_float(lot.get('maxLimitOrderQty'))
        max_market_qty = _to_float(lot.get('maxMarketOrderQty'))
    else:
        qty_step = _to_float(lot.get('qtyStep'))
        min_order_qty = _to_float(lot.get('minOrderQty'))
        min_order_amt = 0.0
        min_notional = _to_float(lot.get('minNotionalValue'))
        max_limit_qty = _to_float(lot.get('maxOrderQty'))
        max_market_qty = _to_float(lot.get('maxMktOrderQty'))

    delivery_ms = _to_float(entry.get('deliveryTime'))
    return InstrumentInfo(
        category=category,
        symbol=str(entry.get('symbol') or ''),
        base_coin=str(entry.get('baseCoin') or ''),
        quote_coin=str(entry.get('quoteCoin') or ''),
        settle_coin=str(entry.get('settleCoin') or ''),
        status=str(entry.get('status') or ''),
        tick_size_str=tick_str,
        tick_size=_to_float(tick_str),
        qty_step=qty_step,
        min_order_qty=min_order_qty,
        min_order_amt=min_order_amt,
        min_notional=min_notional,
        max_limit_order_qty=max_limit_qty,
        max_market_order_qty=max_market_qty,
        contract_type=str(entry.get('contractType') or ''),
        delivery_time=int(delivery_ms / 1000) if delivery_ms > 0 else None,
    )


def _to_float(value: str | float | int | None) -> float:
    """Parse a JSON-string number into ``float``, treating ``None``/empty as 0.0."""
    if value is None or value == '':
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
