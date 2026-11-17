"""
@pyne
"""
import asyncio
from datetime import UTC, datetime

import pytest

from pynecore.core.ohlcv import OHLCVReader
import pynecore_bybit.live_provider as live_provider_module
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import (
    BybitAPIError,
    BybitConnectionError,
    BybitError,
    BybitSymbolError,
    BybitUnsupportedTimeframeError,
)
from pynecore_bybit.helpers import KLINE_LIMIT, add_interval, bar_close_ts
from pynecore_bybit.models import InstrumentInfo, parse_instrument
from pynecore_bybit.ws import BybitWebSocket


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
    jan = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp()) * 1000
    assert add_interval(jan, '60', 3) == jan + 3 * 3_600_000
    assert add_interval(jan, 'W', 2) == jan + 2 * 604_800_000
    assert datetime.fromtimestamp(add_interval(jan, 'M', 1) / 1000, UTC) \
        == datetime(2026, 2, 1, tzinfo=UTC)
    assert datetime.fromtimestamp(add_interval(jan, 'M', 13) / 1000, UTC) \
        == datetime(2027, 2, 1, tzinfo=UTC)
    dec = int(datetime(2025, 12, 1, tzinfo=UTC).timestamp()) * 1000
    assert datetime.fromtimestamp(bar_close_ts(dec, 'M') / 1000, UTC) \
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


def __test_bybit_market_cache_follows_symbol__():
    """A reused provider resolves the market again when its symbol changes."""
    listed = {
        ('spot', '0GUSDT'): _instrument(
            'spot', '0GUSDT', base_coin='0G', tick_size_str='0.0001',
            tick_size=0.0001,
        ),
        ('linear', '1000XECUSDT'): _instrument(
            'linear', '1000XECUSDT', base_coin='1000XEC',
            tick_size_str='0.00001', tick_size=0.00001,
        ),
    }

    class _Resolver(_FakeRestBybit):
        def _fetch_instrument(self, category, symbol):
            return listed.get((category, symbol))

    plugin = _Resolver(symbol='0GUSDT', timeframe='1D')
    first = plugin.update_symbol_info()
    plugin.symbol = '1000XECUSDT.P'
    second = plugin.update_symbol_info()

    assert first.ticker == '0GUSDT'
    assert first.basecurrency == '0G'
    assert second.ticker == '1000XECUSDT.P'
    assert second.basecurrency == '1000XEC'
    assert second.mintick == 0.00001


def __test_bybit_download_paging__(tmp_path):
    """Forward paging: ascending writes, forming-bar drop, boundary dedup"""
    now = int(datetime.now(UTC).timestamp()) // 60 * 60 * 1000
    start = now - 10 * 60_000

    def _row(ts):
        return [str(ts), '100', '101', '99', '100.5', '2.5', '250']

    # Two pages, newest-first as Bybit serves them; the second page repeats
    # the boundary bar and includes the still-forming current minute.
    page1 = {'list': [_row(ts) for ts in range(start + 4 * 60_000, start - 60_000, -60_000)]}
    page2 = {'list': [_row(ts) for ts in range(now, start + 3 * 60_000, -60_000)]}

    plugin = _FakeRestBybit([page1, page2], symbol='BTCUSDT', timeframe='1',
                            ohlcv_dir=tmp_path)
    plugin._market = _instrument('spot', 'BTCUSDT')
    # Chunk size 5 forces two requests over the 10-bar window.
    with plugin:
        plugin.download_ohlcv(
            datetime.fromtimestamp(start / 1000, UTC),
            datetime.fromtimestamp(now / 1000, UTC),
            limit=5,
        )

    with OHLCVReader(str(plugin.ohlcv_path)) as reader:
        bars = list(reader)
    timestamps = [b.timestamp for b in bars]
    # 10 closed bars, strictly ascending, no duplicates, forming bar absent.
    assert timestamps == list(range(start, now, 60_000))
    assert all(b.close == 100.5 for b in bars)
    assert plugin.calls[0][0] == '/v5/market/kline'
    assert plugin.calls[0][1]['category'] == 'spot'


def __test_bybit_ws_dispatch__():
    """Kline pushes: confirm -> queue, forming -> snapshot slot, duplicate drop"""

    def _push(ts, confirm, close='101'):
        return {
            'topic': 'kline.1.BTCUSDT',
            'data': [{'start': str(ts), 'open': '100', 'high': '102',
                      'low': '99', 'close': close, 'volume': '3',
                      'confirm': confirm}],
        }

    async def scenario():
        plugin = _FakeRestBybit(symbol='BTCUSDT', timeframe='1')
        plugin._update_queue = asyncio.Queue()
        plugin._data_ready = asyncio.Event()

        plugin._on_ws_message(_push(60_000, False, close='100.7'))
        assert plugin._update_queue.empty()
        snapshot = plugin._latest_snapshot
        assert snapshot is not None and not snapshot.is_closed
        assert snapshot.close == 100.7

        plugin._on_ws_message(_push(60_000, True))
        closed = plugin._update_queue.get_nowait()
        assert closed.is_closed and closed.timestamp == 60_000
        assert plugin._last_closed_bar_ts == 60_000
        # The forming snapshot of the just-closed bar is stale — cleared so
        # it cannot be emitted as an intra-bar tick after its own close.
        assert plugin._latest_snapshot is None

        # Duplicate confirm is dropped.
        plugin._on_ws_message(_push(60_000, True))
        assert plugin._update_queue.empty()

        # A close goes on at once, in arrival order, never held back.
        plugin._on_ws_message(_push(120_000, True))
        assert plugin._update_queue.get_nowait().timestamp == 120_000
        # A forming bar NEWER than the close must survive it; the
        # stale-snapshot clearing only drops same-or-older ones.
        plugin._on_ws_message(_push(240_000, False))
        plugin._on_ws_message(_push(180_000, True))
        assert plugin._update_queue.get_nowait().timestamp == 180_000
        forming = await plugin.watch_ohlcv('BTCUSDT', '1')
        assert not forming.is_closed and forming.timestamp == 240_000
        # A close older than the newest one already handed on is dropped.
        plugin._on_ws_message(_push(120_000, True))
        assert plugin._update_queue.empty()
        assert plugin._last_closed_bar_ts == 180_000

        # Sentinel surfacing: watch_ohlcv raises ConnectionError.
        await plugin._on_ws_closed()
        with pytest.raises(ConnectionError):
            await plugin.watch_ohlcv('BTCUSDT', '1')

    asyncio.run(scenario())


def __test_bybit_ws_stale_forming_repush_is_dropped__():
    """A ``confirm: false`` re-push of an already-closed slot never becomes a snapshot.

    Measured live (linear ETHUSDT, 2026-10-05): the venue re-pushed a
    forming snapshot for the bar it had closed a minute earlier, and later
    one for a bar nine minutes old. Forwarded, either reaches the core as
    an intra-bar update under a closed bar's timestamp; the provider drops
    them at the door and does not take their close as the latest trade.
    """

    def _push(ts, confirm, close='101'):
        return {
            'topic': 'kline.1.BTCUSDT',
            'data': [{'start': str(ts), 'open': '100', 'high': '102',
                      'low': '99', 'close': close, 'volume': '3',
                      'confirm': confirm}],
        }

    plugin = _FakeRestBybit(symbol='BTCUSDT', timeframe='1')
    plugin._update_queue = asyncio.Queue()
    plugin._data_ready = asyncio.Event()

    plugin._on_ws_message(_push(120_000, True, close='100.5'))
    assert plugin._update_queue.get_nowait().timestamp == 120_000
    assert plugin._last_price == 100.5

    # The just-closed slot and a much older one: both dropped whole.
    plugin._on_ws_message(_push(120_000, False, close='107'))
    plugin._on_ws_message(_push(0, False, close='93'))
    assert plugin._latest_snapshot is None
    assert plugin._update_queue.empty()
    assert plugin._last_price == 100.5

    # The genuinely forming next bar still lands in the snapshot slot.
    plugin._on_ws_message(_push(180_000, False, close='101.2'))
    assert plugin._latest_snapshot is not None
    assert plugin._latest_snapshot.timestamp == 180_000
    assert plugin._last_price == 101.2


def __test_bybit_ws_stale_confirm_repush_keeps_current_price__():
    """A re-pushed confirm of a settled bar leaves the price cache and the snapshot alone.

    The confirm is dropped from the stream; its close is an old trade, so it
    must not replace the forming bar's price that the market-order
    minimum-notional check and the spot position mark read.
    """

    def _push(ts, confirm, close):
        return {
            'topic': 'kline.1.BTCUSDT',
            'data': [{'start': str(ts), 'open': '100', 'high': '160',
                      'low': '99', 'close': close, 'volume': '3',
                      'confirm': confirm}],
        }

    plugin = _FakeRestBybit(symbol='BTCUSDT', timeframe='1')
    plugin._update_queue = asyncio.Queue()
    plugin._data_ready = asyncio.Event()

    plugin._on_ws_message(_push(120_000, True, close='100'))
    assert plugin._update_queue.get_nowait().timestamp == 120_000
    plugin._on_ws_message(_push(180_000, False, close='150'))
    assert plugin._last_price == 150.0

    plugin._on_ws_message(_push(120_000, True, close='100'))
    plugin._on_ws_message(_push(60_000, True, close='90'))
    assert plugin._update_queue.empty()
    assert plugin._last_price == 150.0
    snapshot = plugin._latest_snapshot
    assert snapshot is not None and snapshot.timestamp == 180_000
    assert snapshot.close == 150.0


def __test_bybit_ws_close_awaits_tasks__():
    """close() cancels AND awaits the loops so none are left pending"""

    class _FakeConn:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    async def scenario():
        ws = BybitWebSocket('wss://example/v5/private', on_message=lambda _d: None)
        conn = _FakeConn()
        ws._ws = conn

        async def _idle():
            await asyncio.Event().wait()

        ws._recv_task = asyncio.create_task(_idle())
        ws._ping_task = asyncio.create_task(_idle())
        recv_task, ping_task = ws._recv_task, ws._ping_task

        await ws.close()

        # The transport was closed and both loops are settled (cancelled and
        # awaited) rather than left pending for the loop teardown to destroy.
        assert conn.closed
        assert recv_task.done() and ping_task.done()
        assert recv_task.cancelled() and ping_task.cancelled()
        assert ws._recv_task is None and ws._ping_task is None

        # Idempotent: a second close with nothing tracked is a no-op.
        await ws.close()

    asyncio.run(scenario())


def __test_bybit_ws_open_maps_handshake_parser_errors__(monkeypatch):
    """A garbage HTTP response on connect is a BybitConnectionError, not a raw parser error.

    websockets surfaces its HTTP response parser's own EOFError / ValueError
    as the handshake failure. The private-stream reconnect loop only
    retries on BybitError, so an unmapped parser error killed the whole
    order-event stream (bybit-spot cycle 186 traded blind for four hours
    after one garbage handshake response)."""
    from pynecore_bybit import ws as ws_module
    from pynecore_bybit.exceptions import BybitConnectionError

    async def scenario(exc: BaseException):
        async def _connect(*_a, **_k):
            raise exc

        monkeypatch.setattr(ws_module, "connect", _connect)
        ws = BybitWebSocket('wss://example/v5/private', on_message=lambda _d: None)
        try:
            await ws.open()
        except BybitConnectionError as e:
            assert e.__cause__ is exc
            assert ws._ws is None
            return
        raise AssertionError("open() did not map the handshake error")

    asyncio.run(scenario(EOFError("connection closed while reading HTTP status line")))
    asyncio.run(scenario(ValueError("invalid HTTP status line: garbage")))


def _kline_row(ts):
    return [str(ts), '100', '101', '99', '100.5', '2.5', '250']


def _frozen_minute(monkeypatch) -> int:
    """Freeze the plugin clock 30 s into the current minute; return the minute's open.

    A wall-clock minute boundary during the test cannot open an extra
    (uncanned) request window then.
    """
    now_s = int(datetime.now(UTC).timestamp()) // 60 * 60
    monkeypatch.setattr(live_provider_module, 'epoch_time', lambda: now_s + 30)
    return now_s * 1000


class _ScriptedRestBybit(_FakeRestBybit):
    """Canned REST fake whose response list may also hold exceptions to raise."""

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        response = super().__call__(endpoint, params, method=method, body=body, auth=auth)
        if isinstance(response, Exception):
            raise response
        return response


def _live_plugin(responses, *, last_closed: int | None) -> _ScriptedRestBybit:
    """A connected-looking plugin whose stream has closed ``last_closed``."""
    plugin = _ScriptedRestBybit(responses, symbol='BTCUSDT', timeframe='1')
    plugin._market = _instrument('spot', 'BTCUSDT')
    plugin._update_queue = asyncio.Queue()
    plugin._data_ready = asyncio.Event()
    plugin._last_closed_bar_ts = last_closed
    return plugin


def _stream_state(plugin) -> tuple:
    """Every piece of plugin state the live stream reads or writes."""
    queue = plugin._update_queue
    return (plugin._last_closed_bar_ts, plugin._latest_snapshot, plugin._last_price,
            None if queue is None else queue.qsize(), plugin._public_ws)


def __test_bybit_backfill_closed_bars_pages_ascending__(monkeypatch):
    """Closed bars strictly after ``since_ms``, ascending, across request pages.

    The venue serves each page newest first; the first page re-serves the
    ``since_ms`` bar itself and the last one carries the still-forming bar.
    Neither comes back.
    """
    now = _frozen_minute(monkeypatch)
    since = now - (KLINE_LIMIT + 3) * 60_000
    first_page = range(since, since + (KLINE_LIMIT + 1) * 60_000, 60_000)
    second_page = range(since + (KLINE_LIMIT + 1) * 60_000, now + 60_000, 60_000)
    plugin = _live_plugin(
        [{'list': [_kline_row(ts) for ts in reversed(first_page)]},
         {'list': [_kline_row(ts) for ts in reversed(second_page)]}],
        last_closed=None,
    )

    bars = asyncio.run(plugin.backfill_closed_bars('BTCUSDT', '1', since))

    assert [b.timestamp for b in bars] == list(range(since + 60_000, now, 60_000))
    assert all(b.is_closed for b in bars)
    assert len(plugin.calls) == 2
    for endpoint, params in plugin.calls:
        assert endpoint == '/v5/market/kline'
        assert params['end'] - params['start'] <= (KLINE_LIMIT - 1) * 60_000
    assert plugin.calls[0][1]['start'] == since + 60_000


def __test_bybit_backfill_closed_bars_nothing_closed_yet__(monkeypatch):
    """With only the forming bar after ``since_ms`` the answer is empty, no request sent."""
    now = _frozen_minute(monkeypatch)
    plugin = _live_plugin([], last_closed=None)
    assert asyncio.run(plugin.backfill_closed_bars('BTCUSDT', '1', now - 60_000)) == []
    assert plugin.calls == []


def __test_bybit_backfill_closed_bars_is_a_pure_query__(monkeypatch):
    """Repeated calls return the same bars and leave the stream state untouched.

    Holds wherever the stream's own closed-bar cursor stands: unset, behind
    the query window, or past it.
    """
    now = _frozen_minute(monkeypatch)
    since = now - 4 * 60_000
    page = {'list': [_kline_row(ts) for ts in range(now, since - 60_000, -60_000)]}
    for last_closed in (None, since - 60_000, now - 60_000):
        plugin = _live_plugin([page, page, page], last_closed=last_closed)
        before = _stream_state(plugin)

        async def scenario():
            return [await plugin.backfill_closed_bars('BTCUSDT', '1', since)
                    for _ in range(3)]

        answers = asyncio.run(scenario())

        assert [b.timestamp for b in answers[0]] == [since + 60_000, since + 120_000,
                                                     since + 180_000]
        assert answers[1] == answers[0] and answers[2] == answers[0]
        assert _stream_state(plugin) == before


def __test_bybit_backfill_closed_bars_raises_unanswered_query__(monkeypatch):
    """A request the venue did not answer raises; no partial answer, no state change.

    An empty list would tell the framework the venue has no bar there, and it
    would go on over a gap it never saw.
    """
    now = _frozen_minute(monkeypatch)
    since = now - (KLINE_LIMIT + 3) * 60_000
    first_page = {'list': [_kline_row(ts) for ts in
                           range(since + KLINE_LIMIT * 60_000, since, -60_000)]}
    cases = {
        'transport': [BybitConnectionError("REST down")],
        'venue error': [BybitAPIError("busy", ret_code=10006)],
        'no bar list': [{}],
        'second page': [first_page, BybitConnectionError("REST down")],
    }
    for name, responses in cases.items():
        plugin = _live_plugin(responses, last_closed=since)
        before = _stream_state(plugin)
        with pytest.raises(BybitError):
            asyncio.run(plugin.backfill_closed_bars('BTCUSDT', '1', since))
        assert _stream_state(plugin) == before, name
        assert plugin._update_queue.empty(), name


def __test_bybit_history_attributes__():
    """The measured venue properties the framework's stream recovery works with."""
    assert Bybit.history_has_idle_bars is True
    assert Bybit.history_publish_lag == 3.0


def __test_bybit_ohlcv_path__(tmp_path):
    """Provider filename derives from the entry-point-style class name"""
    path = Bybit.get_ohlcv_path('BTCUSDT.P', '240', tmp_path)
    assert path.name == 'bybit_BTCUSDT.P_240.ohlcv'
    assert path.with_suffix('.toml').name == 'bybit_BTCUSDT.P_240.toml'
