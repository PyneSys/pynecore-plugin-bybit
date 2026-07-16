"""
@pyne
"""
import asyncio
from datetime import UTC, datetime

import pytest

from pynecore.core.ohlcv_file import OHLCVReader
from pynecore.types.ohlcv import OHLCV
import pynecore_bybit.live_provider as live_provider_module
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import (
    BybitConnectionError,
    BybitSymbolError,
    BybitUnsupportedTimeframeError,
)
from pynecore_bybit.helpers import KLINE_LIMIT, add_interval, bar_close_ts
from pynecore_bybit.models import InstrumentInfo, parse_instrument


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


def _instrument(category: str, symbol: str, **overrides) -> InstrumentInfo:
    """Build a minimal valid InstrumentInfo for tests."""
    values = dict(
        category=category,
        symbol=symbol,
        base_coin='BTC',
        quote_coin='USDT',
        settle_coin='' if category == 'spot' else 'USDT',
        status='Trading',
        tick_size_str='0.10',
        tick_size=0.1,
        qty_step_str='0.001',
        qty_step=0.001,
        min_order_qty=0.0,
        min_order_amt=1.0,
        min_notional=0.0,
        max_limit_order_qty=100.0,
        max_market_order_qty=50.0,
        contract_type='' if category == 'spot' else 'LinearPerpetual',
        delivery_time=None,
    )
    values.update(overrides)
    return InstrumentInfo(**values)


class _FakeRestBybit(Bybit):
    """Bybit with the REST dispatcher replaced by a canned-response fake."""

    def __init__(self, responses=None, **kwargs):
        kwargs.setdefault('config', BybitConfig())
        super().__init__(**kwargs)
        #: list of (endpoint, params) actually requested
        self.calls = []
        #: queue of canned ``result`` payloads returned in order
        self._responses = list(responses or [])

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or {})))
        if not self._responses:
            raise AssertionError(f"Unexpected REST call: {endpoint} {params}")
        return self._responses.pop(0)


def __test_bybit_timeframe_conversion__():
    """TradingView <-> Bybit kline interval conversion"""
    assert Bybit.to_exchange_timeframe('1') == '1'
    assert Bybit.to_exchange_timeframe('240') == '240'
    assert Bybit.to_exchange_timeframe('1D') == 'D'
    assert Bybit.to_exchange_timeframe('1W') == 'W'
    assert Bybit.to_exchange_timeframe('1M') == 'M'
    assert Bybit.to_tradingview_timeframe('D') == '1D'
    assert Bybit.to_tradingview_timeframe('60') == '60'

    with pytest.raises(BybitUnsupportedTimeframeError):
        Bybit.to_exchange_timeframe('45')
    with pytest.raises(ValueError):
        Bybit.to_exchange_timeframe('invalid')
    with pytest.raises(ValueError):
        Bybit.to_tradingview_timeframe('1D')  # exchange side expects 'D'


def __test_bybit_price_grid__():
    """minmove/pricescale derived exactly from the tickSize string"""
    info = _instrument('spot', 'BTCUSDT')
    cases = {
        '0.10': (1, 10),
        '0.5': (1, 2),
        '0.25': (1, 4),
        '0.01': (1, 100),
        '0.000001': (1, 1000000),
        '5': (5, 1),
        '1': (1, 1),
    }
    for tick, expected in cases.items():
        info.tick_size_str = tick
        minmove, pricescale = info.price_grid()
        assert (minmove, pricescale) == expected, tick
        assert minmove / pricescale == pytest.approx(float(tick))

    for bad in ('', '0', '-0.1', 'abc', '0.0'):
        info.tick_size_str = bad
        with pytest.raises(BybitSymbolError):
            info.price_grid()


def __test_bybit_parse_instrument__():
    """Category-specific rule fields normalize into one record"""
    spot = parse_instrument('spot', {
        'symbol': 'BTCUSDT', 'baseCoin': 'BTC', 'quoteCoin': 'USDT',
        'status': 'Trading',
        'priceFilter': {'tickSize': '0.01'},
        'lotSizeFilter': {'basePrecision': '0.000001', 'minOrderAmt': '1',
                          'maxLimitOrderQty': '71.7', 'maxMarketOrderQty': '30'},
    })
    assert spot.qty_step == 0.000001
    assert spot.min_order_amt == 1.0
    assert spot.min_notional == 0.0
    assert spot.max_market_order_qty == 30.0
    assert not spot.is_perpetual
    assert spot.delivery_time is None

    linear = parse_instrument('linear', {
        'symbol': 'BTCUSDT', 'baseCoin': 'BTC', 'quoteCoin': 'USDT',
        'settleCoin': 'USDT', 'status': 'Trading',
        'contractType': 'LinearPerpetual',
        'priceFilter': {'tickSize': '0.10'},
        'lotSizeFilter': {'qtyStep': '0.001', 'minOrderQty': '0.001',
                          'maxOrderQty': '1500', 'maxMktOrderQty': '150',
                          'minNotionalValue': '5'},
        'deliveryTime': '0',
    })
    assert linear.qty_step == 0.001
    assert linear.min_order_qty == 0.001
    assert linear.min_notional == 5.0
    assert linear.is_perpetual
    assert linear.delivery_time is None

    dated = parse_instrument('linear', {
        'symbol': 'BTCUSDT-26JUN26', 'baseCoin': 'BTC', 'quoteCoin': 'USDT',
        'settleCoin': 'USDT', 'status': 'Trading',
        'contractType': 'LinearFutures',
        'priceFilter': {'tickSize': '0.10'},
        'lotSizeFilter': {'qtyStep': '0.001', 'minOrderQty': '0.001'},
        'deliveryTime': '1782518400000',
    })
    assert not dated.is_perpetual
    assert dated.delivery_time == 1782518400

    # Bybit serves the inverse contract's row under a linear query too
    # (measured live) — the contractType corrects the category label.
    mislabeled = parse_instrument('linear', {
        'symbol': 'BTCUSD', 'baseCoin': 'BTC', 'quoteCoin': 'USD',
        'settleCoin': 'BTC', 'status': 'Trading',
        'contractType': 'InversePerpetual',
        'priceFilter': {'tickSize': '0.10'},
        'lotSizeFilter': {'qtyStep': '1', 'minOrderQty': '1',
                          'minNotionalValue': '5'},
        'deliveryTime': '0',
    })
    assert mislabeled.category == 'inverse'
    assert mislabeled.is_inverse and mislabeled.is_perpetual


def __test_bybit_interval_math__():
    """Fixed intervals advance arithmetically, months on calendar boundaries"""
    jan = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp())
    assert add_interval(jan, '60', 3) == jan + 3 * 3600
    assert add_interval(jan, 'W', 2) == jan + 2 * 604800
    assert datetime.fromtimestamp(add_interval(jan, 'M', 1), UTC) \
        == datetime(2026, 2, 1, tzinfo=UTC)
    assert datetime.fromtimestamp(add_interval(jan, 'M', 13), UTC) \
        == datetime(2027, 2, 1, tzinfo=UTC)
    dec = int(datetime(2025, 12, 1, tzinfo=UTC).timestamp())
    assert datetime.fromtimestamp(bar_close_ts(dec, 'M'), UTC) \
        == datetime(2026, 1, 1, tzinfo=UTC)


def __test_bybit_symbol_resolution__():
    """Plain names probe spot first, .P probes linear then inverse"""
    listed: dict[tuple[str, str], InstrumentInfo] = {
        ('spot', 'BTCUSDT'): _instrument('spot', 'BTCUSDT'),
        ('linear', 'BTCUSDT'): _instrument('linear', 'BTCUSDT'),
        ('inverse', 'BTCUSD'): _instrument('inverse', 'BTCUSD',
                                           contract_type='InversePerpetual',
                                           quote_coin='USD', settle_coin='BTC'),
        ('linear', 'ETHUSDT-26JUN26'): _instrument('linear', 'ETHUSDT-26JUN26',
                                                   contract_type='LinearFutures'),
    }

    class _Resolver(_FakeRestBybit):
        def _fetch_instrument(self, category, symbol):
            return listed.get((category, symbol))

    plugin = _Resolver(symbol='BTCUSDT', timeframe='60')
    assert plugin._resolve_market('BTCUSDT').category == 'spot'
    assert plugin._resolve_market('BTCUSDT.P').category == 'linear'
    assert plugin._resolve_market('BTCUSD.P').category == 'inverse'
    assert plugin._resolve_market('bybit:btcusdt.p').category == 'linear'
    assert plugin._resolve_market('ETHUSDT-26JUN26').category == 'linear'
    with pytest.raises(BybitSymbolError):
        plugin._resolve_market('NOSUCHPAIR')

    # symbol_map translation applies before the notation rules
    plugin.config.symbol_map['BINANCE:BTCUSDTPERP'] = 'BTCUSDT.P'
    assert plugin._resolve_market('BINANCE:BTCUSDTPERP').category == 'linear'


def __test_bybit_update_symbol_info__():
    """SymInfo synthesis: type / grid / mincontract per category"""
    plugin = _FakeRestBybit(symbol='BTCUSD.P', timeframe='1D')
    plugin._market = _instrument(
        'inverse', 'BTCUSD', contract_type='InversePerpetual',
        quote_coin='USD', settle_coin='BTC',
        tick_size_str='0.5', tick_size=0.5, qty_step_str='1', qty_step=1.0,
    )
    # Inverse mincontract mirrors the linear sibling's lot (TV-measured);
    # the sibling resolves through the instrument cache.
    plugin._instruments[('linear', 'BTCUSDT')] = _instrument('linear', 'BTCUSDT')
    si = plugin.update_symbol_info()
    # TV reports "crypto" for crypto perpetuals too (measured).
    assert si.type == 'crypto'
    assert si.ticker == 'BTCUSD.P'
    assert si.currency == 'USD'
    # Inverse kline volume is quote-denominated (whole-USD contracts).
    assert si.volumetype == 'quote'
    assert si.mintick == 0.5
    assert (si.minmove, si.pricescale) == (1, 2)
    assert si.mincontract == 0.001
    assert len(si.opening_hours) == 7
    assert len(si.session_starts) == 7

    # Without a linear sibling the grid falls back to 0.0 (estimation).
    plugin_nosib = _FakeRestBybit(symbol='XYZUSD.P', timeframe='1D',
                                  responses=[{'list': []}])
    plugin_nosib._market = _instrument(
        'inverse', 'XYZUSD', contract_type='InversePerpetual',
        base_coin='XYZ', quote_coin='USD', settle_coin='XYZ',
        tick_size_str='0.5', tick_size=0.5, qty_step_str='1', qty_step=1.0,
    )
    assert plugin_nosib.update_symbol_info().mincontract == 0.0

    plugin._market = _instrument('spot', 'BTCUSDT', status='PreLaunch')
    with pytest.raises(BybitSymbolError):
        plugin.update_symbol_info()


def __test_bybit_download_paging__(tmp_path):
    """Forward paging: ascending writes, forming-bar drop, boundary dedup"""
    now = int(datetime.now(UTC).timestamp()) // 60 * 60
    start = now - 10 * 60

    def _row(ts):
        return [str(ts * 1000), '100', '101', '99', '100.5', '2.5', '250']

    # Two pages, newest-first as Bybit serves them; the second page repeats
    # the boundary bar and includes the still-forming current minute.
    page1 = {'list': [_row(ts) for ts in range(start + 4 * 60, start - 60, -60)]}
    page2 = {'list': [_row(ts) for ts in range(now, start + 3 * 60, -60)]}

    plugin = _FakeRestBybit([page1, page2], symbol='BTCUSDT', timeframe='1',
                            ohlcv_dir=tmp_path)
    plugin._market = _instrument('spot', 'BTCUSDT')
    # Chunk size 5 forces two requests over the 10-bar window.
    with plugin:
        plugin.download_ohlcv(
            datetime.fromtimestamp(start, UTC), datetime.fromtimestamp(now, UTC),
            limit=5,
        )

    with OHLCVReader(str(plugin.ohlcv_path)) as reader:
        bars = list(reader)
    timestamps = [b.timestamp for b in bars]
    # 10 closed bars, strictly ascending, no duplicates, forming bar absent.
    assert timestamps == list(range(start, now, 60))
    assert all(b.close == 100.5 for b in bars)
    assert plugin.calls[0][0] == '/v5/market/kline'
    assert plugin.calls[0][1]['category'] == 'spot'


def __test_bybit_ws_dispatch__():
    """Kline pushes: confirm -> queue, forming -> snapshot slot, pending hold"""

    def _push(ts, confirm, close='101'):
        return {
            'topic': 'kline.1.BTCUSDT',
            'data': [{'start': str(ts * 1000), 'open': '100', 'high': '102',
                      'low': '99', 'close': close, 'volume': '3',
                      'confirm': confirm}],
        }

    async def scenario():
        plugin = _FakeRestBybit(symbol='BTCUSDT', timeframe='1')
        plugin._update_queue = asyncio.Queue()
        plugin._data_ready = asyncio.Event()

        plugin._on_ws_message(_push(60, False, close='100.7'))
        assert plugin._update_queue.empty()
        snapshot = plugin._latest_snapshot
        assert snapshot is not None and not snapshot.is_closed
        assert snapshot.close == 100.7

        plugin._on_ws_message(_push(60, True))
        closed = plugin._update_queue.get_nowait()
        assert closed.is_closed and closed.timestamp == 60
        assert plugin._last_closed_bar_ts == 60
        # The forming snapshot of the just-closed bar is stale — cleared so
        # it cannot be emitted as an intra-bar tick after its own close.
        assert plugin._latest_snapshot is None

        # Duplicate confirm is dropped.
        plugin._on_ws_message(_push(60, True))
        assert plugin._update_queue.empty()

        # Pending mode (reconnect backfill in flight) holds closed bars back,
        # then releases them in order after the backfill.
        plugin._pending_closed = []
        plugin._on_ws_message(_push(240, True))
        plugin._on_ws_message(_push(180, True))
        assert plugin._update_queue.empty()
        # A forming bar NEWER than every released close must survive the
        # release; the stale-snapshot clearing only drops same-or-older ones.
        plugin._on_ws_message(_push(300, False))
        plugin._release_pending_closed()
        assert plugin._update_queue.get_nowait().timestamp == 180
        assert plugin._update_queue.get_nowait().timestamp == 240
        assert plugin._pending_closed is None
        assert plugin._latest_snapshot is not None
        assert plugin._latest_snapshot.timestamp == 300

        # Sentinel surfacing: watch_ohlcv raises ConnectionError.
        await plugin._on_ws_closed()
        with pytest.raises(ConnectionError):
            await plugin.watch_ohlcv('BTCUSDT', '1')

    asyncio.run(scenario())


def __test_bybit_reconnect_backfill__(monkeypatch):
    """Backfill pages beyond one kline request; a REST failure keeps the gap"""
    # Freeze the backfill clock mid-minute so a wall-clock minute boundary
    # during the test cannot open an extra (uncanned) request window.
    now = int(datetime.now(UTC).timestamp()) // 60 * 60
    monkeypatch.setattr(live_provider_module, 'epoch_time', lambda: now + 30)

    def _row(ts):
        return [str(ts * 1000), '100', '101', '99', '100.5', '2.5', '250']

    async def paged_scenario():
        gap_bars = KLINE_LIMIT + 2
        last_closed = now - (gap_bars + 1) * 60
        first_window = range(last_closed + 60, last_closed + 60 + KLINE_LIMIT * 60, 60)
        second_window = range(last_closed + 60 + KLINE_LIMIT * 60, now, 60)

        plugin = _FakeRestBybit(
            [{'list': [_row(ts) for ts in reversed(first_window)]},
             {'list': [_row(ts) for ts in reversed(second_window)]}],
            symbol='BTCUSDT', timeframe='1',
        )
        plugin._market = _instrument('spot', 'BTCUSDT')
        plugin._update_queue = asyncio.Queue()
        plugin._data_ready = asyncio.Event()
        plugin._last_closed_bar_ts = last_closed
        plugin._pending_closed = []

        await plugin.on_reconnect()

        assert len(plugin.calls) == 2
        timestamps = []
        while not plugin._update_queue.empty():
            timestamps.append(plugin._update_queue.get_nowait().timestamp)
        assert timestamps == list(range(last_closed + 60, now, 60))
        assert plugin._last_closed_bar_ts == now - 60
        assert plugin._pending_closed is None

    async def failing_scenario():
        class _FailingRest(_FakeRestBybit):
            def __call__(self, endpoint, params=None, *, method='get',
                         body=None, auth=False):
                raise BybitConnectionError("REST down")

        class _FakeWS:
            def __init__(self):
                self.closed = False

            async def close(self):
                self.closed = True

        now = int(datetime.now(UTC).timestamp()) // 60 * 60
        plugin = _FailingRest(symbol='BTCUSDT', timeframe='1')
        plugin._market = _instrument('spot', 'BTCUSDT')
        plugin._update_queue = asyncio.Queue()
        plugin._data_ready = asyncio.Event()
        plugin._last_closed_bar_ts = now - 600
        held = OHLCV(timestamp=now - 60, open=100.0, high=101.0, low=99.0,
                     close=100.5, volume=1.0, is_closed=True)
        plugin._pending_closed = [held]
        ws = _FakeWS()
        plugin._public_ws = ws

        # The failure must propagate WITHOUT advancing the closed-bar cursor
        # or releasing held bars — otherwise the gap would be silently lost
        # forever. The live runner swallows the on_reconnect error and
        # resumes watching, so the transport must be force-closed and the
        # sentinel queued: watch_ohlcv then raises ConnectionError, which
        # drives the runner into another full reconnect + backfill cycle.
        with pytest.raises(BybitConnectionError):
            await plugin.on_reconnect()
        assert plugin._last_closed_bar_ts == now - 600
        assert plugin._pending_closed is None
        assert ws.closed and plugin._public_ws is None
        # The queued sentinel makes the consumer raise, which is what sends
        # the runner into another full reconnect + backfill cycle.
        with pytest.raises(ConnectionError):
            await plugin.watch_ohlcv('BTCUSDT', '1')
        assert plugin._last_closed_bar_ts == now - 600

    asyncio.run(paged_scenario())
    asyncio.run(failing_scenario())


def __test_bybit_ohlcv_path__(tmp_path):
    """Provider filename derives from the entry-point-style class name"""
    path = Bybit.get_ohlcv_path('BTCUSDT.P', '240', tmp_path)
    assert path.name == 'bybit_BTCUSDT.P_240.ohlcv'
    assert path.with_suffix('.toml').name == 'bybit_BTCUSDT.P_240.toml'
