"""
@pyne

Closes of a handed-over entry's position: an entry the startup recovery handed
to the engine (``request_execution_hold``) may have been closed on the venue
while the bot was down. The adoption baseline folds that close without an
event, so when the running engine books the held opening
(``fetch_order_execution``), the plugin books and queues what the venue closed
of it as the entry's close fills, behind the opening, once per ``execId``.
"""
import asyncio
from time import time as epoch_time

import pytest

from pynecore.core.broker.models import LegType
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore, FillReceipt
from pynecore.core.broker.store_helpers import ENTRY_KIND_WORKING

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitConnectionError
from pynecore_bybit.helpers import ENTRY_EXECUTION_HOLD_EXTRA, EXECUTION_HOLD_CLOSE_EVENT
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_HEDGE, POSITION_MODE_ONE_WAY


def main():
    """
    Dummy main function to be a valid Pyne script
    """
    pass


#: The pending entry row an earlier process left behind.
__test_helper_COID = 'coid-pending-entry'

#: Venue time of the held order's execution, and of the adoption floor.
__test_helper_OPEN_MS = int(epoch_time() * 1000) - 600_000
__test_helper_FLOOR_MS = int(epoch_time() * 1000) - 60_000


class __test_helper_Venue(Bybit):
    """A Bybit derivative whose REST answers per endpoint from queued responses.

    ``answers`` maps an endpoint to a list of results or exceptions, taken in
    order; an endpoint without one answers an empty list.
    """

    def __init__(self, market: InstrumentInfo, answers: dict[str, list], mode: str):
        super().__init__(config=BybitConfig(), symbol=market.symbol, timeframe='1')
        self._market = market
        self._broker_started = True
        self._position_mode = mode
        self.answers = answers
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or body or {})))
        queue = self.answers.get(endpoint)
        if not queue:
            return {'list': [], 'nextPageCursor': ''}
        answer = queue.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def __test_helper_market(category: str) -> InstrumentInfo:
    """The chart instrument (linear BTCUSDT or inverse BTCUSD)."""
    if category == 'inverse':
        return InstrumentInfo(
            category='inverse', symbol='BTCUSD', base_coin='BTC', quote_coin='USD',
            settle_coin='BTC', status='Trading', tick_size_str='0.10', tick_size=0.1,
            qty_step_str='1', qty_step=1.0, min_order_qty=1.0, min_order_amt=0.0,
            min_notional=5.0, max_limit_order_qty=25_000_000.0,
            max_market_order_qty=5_000_000.0, contract_type='InversePerpetual',
            delivery_time=None,
        )
    return InstrumentInfo(
        category='linear', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='USDT', status='Trading', tick_size_str='0.10', tick_size=0.1,
        qty_step_str='0.001', qty_step=0.001, min_order_qty=0.001, min_order_amt=0.0,
        min_notional=5.0, max_limit_order_qty=1500.0, max_market_order_qty=150.0,
        contract_type='LinearPerpetual', delivery_time=None,
    )


def __test_helper_order(symbol: str, side: str, qty: str) -> dict:
    """The venue order object of the pending entry: ended ``Filled``."""
    return {
        'orderId': '901', 'orderLinkId': __test_helper_COID, 'symbol': symbol,
        'side': side, 'orderType': 'Market', 'qty': qty, 'price': '0',
        'orderStatus': 'Filled', 'cumExecQty': qty,
        'createdTime': str(__test_helper_OPEN_MS - 1_000),
    }


def __test_helper_execution(exec_id: str, *, symbol: str = 'BTCUSDT', side: str,
                            qty: str, price: str, closed: str = '0', fee: str = '0.02',
                            ms: int, order_id: str, link_id: str = '', seq: int = 1,
                            exec_type: str = 'Trade', fee_currency: str = '') -> dict:
    """One ``/v5/execution/list`` row."""
    return {
        'symbol': symbol, 'execType': exec_type, 'execId': exec_id, 'orderId': order_id,
        'orderLinkId': link_id, 'side': side, 'execQty': qty, 'execPrice': price,
        'execFee': fee, 'feeCurrency': fee_currency, 'execTime': str(ms),
        'closedSize': closed, 'seq': str(seq),
    }


def __test_helper_opening(symbol: str = 'BTCUSDT', side: str = 'Buy',
                          qty: str = '0.004', price: str = '100000') -> dict:
    """The held order's own execution, which the startup recovery could not read."""
    return __test_helper_execution(
        'e-open', symbol=symbol, side=side, qty=qty, price=price,
        ms=__test_helper_OPEN_MS, order_id='901', link_id=__test_helper_COID, seq=10,
    )


def __test_helper_handed_over(tmp_path, name: str, answers: dict[str, list], *,
                              category: str = 'linear', mode: str = POSITION_MODE_ONE_WAY,
                              side: str = 'buy', qty: float = 0.004,
                              anchor: str | None = None):
    """A restarted run whose startup recovery handed the pending entry to the engine.

    The order ended ``Filled`` while the bot was down; its executions could not
    be read at startup, so the recovery requested the engine's hold. The
    engine then confirmed the row (its next sync), the adoption baseline
    latched at the floor, and the running engine reads the order's execution.
    """
    store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name='bybit')
    market = __test_helper_market(category)
    identity = RunIdentity(strategy_id="hold", symbol=market.symbol, timeframe="1",
                           account_id=f"hold-{name}")
    first = store.open_run(identity, script_source="// hold")
    extras: dict = {'kind': ENTRY_KIND_WORKING}
    if anchor is not None:
        extras['anchor'] = anchor
    first.upsert_order(
        __test_helper_COID, symbol=market.symbol, side=side, qty=qty, filled_qty=0.0,
        state='submitted', intent_key='Long', pine_entry_id='Long', extras=extras,
    )
    first.close()
    wire_side = 'Buy' if side == 'buy' else 'Sell'
    startup = {
        '/v5/order/realtime': [{'list': [__test_helper_order(market.symbol, wire_side,
                                                             repr(qty))]}],
        '/v5/execution/list': [BybitConnectionError("HTTP 503")],
    }
    plugin = __test_helper_Venue(market, startup, mode)
    plugin.store_ctx = store.open_run(identity, script_source="// hold")
    asyncio.run(plugin._recover_in_flight_submissions())
    assert set(plugin.store_ctx.execution_hold_requests()) == {__test_helper_COID}
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and (row.extras or {}).get(ENTRY_EXECUTION_HOLD_EXTRA) is True
    # The engine's consumption of the request (``_consume_execution_hold_requests``).
    plugin.store_ctx.upsert_order(__test_helper_COID, state='confirmed',
                                  exchange_order_id='901')
    plugin._adoption_baselined = True
    plugin._deriv_exec_floor_ms = __test_helper_FLOOR_MS
    plugin.answers = answers
    plugin.calls.clear()
    return plugin


def __test_helper_delivered(plugin) -> list[str]:
    """The ``execId`` of every close delivery record of the run."""
    return [str(payload.get('exec_id')) for _ik, _coid, _eoid, payload
            in plugin.store_ctx.iter_events_by_kind_for_run_id(EXECUTION_HOLD_CLOSE_EVENT)]


def __test_helper_position(side: str, size: str, idx: int = 0) -> dict:
    """One ``/v5/position/list`` row."""
    return {'symbol': 'BTCUSDT', 'side': side, 'size': size, 'positionIdx': idx,
            'updatedTime': str(int(epoch_time() * 1000))}


def __test_closed_position_is_delivered_behind_the_opening_once__(tmp_path):
    """One-way linear: a venue stop closed the held entry while the bot was down.

    The running engine's read books the opening and the stop's close; the
    outbox carries the opening first, then the close as the entry's ``CLOSE``
    fill with the stop's own ``execId``, price and fee. A second read delivers
    nothing again.
    """
    stop = __test_helper_execution(
        'e-stop', side='Sell', qty='0.004', price='98000', closed='0.004', fee='0.03',
        ms=__test_helper_OPEN_MS + 60_000, order_id='stop-1', seq=20)
    funding = __test_helper_execution(
        'e-funding', side='Sell', qty='0.004', price='98500', closed='0', fee='0.001',
        ms=__test_helper_OPEN_MS + 30_000, order_id='funding', seq=15,
        exec_type='Funding')
    plugin = __test_helper_handed_over(tmp_path, "stop", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            {'list': [stop, funding, __test_helper_opening()], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}],
    })

    execution = asyncio.run(plugin.fetch_order_execution('901', None))

    assert execution is not None and execution.fills is not None
    assert [fill.fill_id for fill in execution.fills] == ['e-open']
    window = plugin.calls[-1]
    assert window[0] == '/v5/execution/list'
    assert (window[1]['startTime'], window[1]['endTime']) == (
        __test_helper_OPEN_MS, __test_helper_FLOOR_MS)
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open', 'e-stop']
    close = plugin._event_outbox[1]
    assert (close.leg_type, close.pine_id, close.from_entry) == (LegType.CLOSE, None, 'Long')
    assert (close.fill_qty, close.fill_price, close.journal_fill_qty) == (
        pytest.approx(0.004), 98000.0, pytest.approx(0.004))
    assert close.order.side == 'sell'
    assert close.fee == pytest.approx(0.03)
    assert __test_helper_delivered(plugin) == ['e-stop']
    assert plugin._run_owned_signed_size() == pytest.approx(0.0)

    plugin._event_outbox.clear()
    plugin.answers = {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [{'list': [__test_helper_opening()], 'nextPageCursor': ''}],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}],
    }
    again = asyncio.run(plugin.fetch_order_execution('901', None))
    assert again is not None and [fill.fill_id for fill in again.fills or ()] == ['e-open']
    assert plugin._event_outbox == []
    assert __test_helper_delivered(plugin) == ['e-stop']
    plugin.store_ctx.close()


def __test_position_the_venue_still_holds_owes_no_close__(tmp_path):
    """The venue holds the held execution: the opening only, no history window is read."""
    plugin = __test_helper_handed_over(tmp_path, "held", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [{'list': [__test_helper_opening()], 'nextPageCursor': ''}],
        '/v5/position/list': [{'list': [__test_helper_position('Buy', '0.004')]}],
    })
    execution = asyncio.run(plugin.fetch_order_execution('901', None))
    assert execution is not None
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open']
    assert [endpoint for endpoint, _ in plugin.calls].count('/v5/execution/list') == 1
    assert __test_helper_delivered(plugin) == []
    assert plugin._run_owned_signed_size() == pytest.approx(0.004)
    plugin.store_ctx.close()


def __test_close_is_cut_to_the_held_share_and_own_orders_are_skipped__(tmp_path):
    """Only the shortfall the held entry explains is delivered, oldest first.

    The run's other entry kept 0.001 after the baseline's clamp, the venue
    holds 0.001: 0.004 of the held execution is gone. A close of the run's own
    journal (it reduced the other entry) is skipped; a manual 0.005 sell is cut
    to 0.004 with its fee pro rata.
    """
    plugin = __test_helper_handed_over(tmp_path, "cut", {})
    plugin.store_ctx.upsert_order(
        'coid-other-entry', symbol='BTCUSDT', side='buy', qty=0.001, filled_qty=0.001,
        state='confirmed', intent_key='Other', pine_entry_id='Other',
        exchange_order_id='800', extras={'kind': ENTRY_KIND_WORKING},
    )
    plugin.store_ctx.upsert_order(
        'coid-own-close', symbol='BTCUSDT', side='sell', qty=0.001, filled_qty=0.001,
        state='confirmed', intent_key='Other', exchange_order_id='801',
        extras={'kind': 'close', 'close_of_entry': 'Other'},
    )
    own = __test_helper_execution(
        'e-own', side='Sell', qty='0.001', price='99500', closed='0.001',
        ms=__test_helper_OPEN_MS + 10_000, order_id='801', link_id='coid-own-close', seq=12)
    manual = __test_helper_execution(
        'e-manual', side='Sell', qty='0.005', price='99000', closed='0.005', fee='0.05',
        ms=__test_helper_OPEN_MS + 20_000, order_id='manual-1', seq=13)
    plugin.answers = {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            {'list': [manual, own], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [__test_helper_position('Buy', '0.001')]}],
    }
    asyncio.run(plugin.fetch_order_execution('901', None))
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open', 'e-manual']
    close = plugin._event_outbox[1]
    assert close.fill_qty == pytest.approx(0.004)
    assert close.fee == pytest.approx(0.04)
    assert close.from_entry == 'Long'
    assert __test_helper_delivered(plugin) == ['e-manual']
    assert plugin._run_owned_signed_size() == pytest.approx(0.001)
    plugin.store_ctx.close()


def __test_unreadable_history_is_no_answer_and_books_nothing__(tmp_path):
    """A history window that fails to drain: no answer, nothing booked or queued; the
    engine's next read delivers the opening and the close."""
    stop = __test_helper_execution(
        'e-stop', side='Sell', qty='0.004', price='98000', closed='0.004',
        ms=__test_helper_OPEN_MS + 60_000, order_id='stop-1', seq=20)
    plugin = __test_helper_handed_over(tmp_path, "unreadable", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}] * 2,
        '/v5/execution/list': [
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            BybitConnectionError("HTTP 503"),
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            {'list': [stop], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}] * 2,
    })
    assert asyncio.run(plugin.fetch_order_execution('901', None)) is None
    assert plugin._event_outbox == []
    row = plugin.store_ctx.get_order(__test_helper_COID)
    assert row is not None and row.filled_qty == 0.0
    assert 'e-open' not in plugin._seen_exec_ids

    assert asyncio.run(plugin.fetch_order_execution('901', None)) is not None
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open', 'e-stop']
    plugin.store_ctx.close()


def __test_hedge_inverse_short_leg_close_converts_at_the_held_anchor__(tmp_path):
    """Hedge inverse: a liquidation closed the held short leg; an opening buy of the long
    leg (``closedSize`` 0) is no close. The close converts to the base the opening
    booked, its settle-coin fee to the quote currency."""
    opening = __test_helper_opening(symbol='BTCUSD', side='Sell', qty='200', price='100000')
    opening['execFee'] = '0.000001'
    opening['feeCurrency'] = 'BTC'
    long_open = __test_helper_execution(
        'e-long', symbol='BTCUSD', side='Buy', qty='50', price='96000', closed='0',
        fee='0.0000005', fee_currency='BTC', ms=__test_helper_OPEN_MS + 10_000,
        order_id='foreign-1', seq=11)
    liquidation = __test_helper_execution(
        'e-liq', symbol='BTCUSD', side='Buy', qty='200', price='105000', closed='200',
        fee='0.000002', fee_currency='BTC', ms=__test_helper_OPEN_MS + 20_000,
        order_id='liq-1', seq=12, exec_type='BustTrade')
    plugin = __test_helper_handed_over(tmp_path, "hedge-inverse", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSD', 'Sell', '200')]}],
        '/v5/execution/list': [
            {'list': [opening], 'nextPageCursor': ''},
            {'list': [liquidation, long_open], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [
            {**__test_helper_position('Buy', '50', 1), 'symbol': 'BTCUSD'},
            {**__test_helper_position('', '0', 2), 'symbol': 'BTCUSD'},
        ]}],
    }, category='inverse', mode=POSITION_MODE_HEDGE, side='sell', qty=200.0,
        anchor='100000')

    execution = asyncio.run(plugin.fetch_order_execution('901', None))

    assert execution is not None
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open', 'e-liq']
    opened, close = plugin._event_outbox
    assert opened.fill_qty == pytest.approx(200 / 100000)
    assert (close.leg_type, close.from_entry, close.order.side) == (LegType.CLOSE, 'Long', 'buy')
    assert close.fill_qty == pytest.approx(opened.fill_qty)
    assert close.journal_fill_qty == pytest.approx(200.0)
    assert close.fill_price == 105000.0
    assert close.fee == pytest.approx(0.000002 * 105000)
    assert close.fee_currency == 'USD'
    assert __test_helper_delivered(plugin) == ['e-liq']
    plugin.store_ctx.close()


def __test_helper_restart(plugin, tmp_path, name: str, answers: dict[str, list],
                          monkeypatch, *, floor_ms: int):
    """The next process of the run: a fresh plugin over the same journal, its baseline run.

    The earlier process ended before its engine drained the outbox; the
    adoption baseline of the new one reads a flat venue at ``floor_ms``.
    """
    plugin.store_ctx.close()
    store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name='bybit')
    identity = RunIdentity(strategy_id="hold", symbol='BTCUSDT', timeframe="1",
                           account_id=f"hold-{name}")
    fresh = __test_helper_Venue(__test_helper_market('linear'), answers, POSITION_MODE_ONE_WAY)
    fresh.store_ctx = store.open_run(identity, script_source="// hold")
    order = __test_helper_order('BTCUSDT', 'Buy', '0.004')

    async def venue_time() -> int:
        return floor_ms

    async def lookup(_market, _coid):
        return order, True

    async def fills(_market, _order_id, _from_ms, until_ms=None):
        return {'e-open'}, 0.004, True, [__test_helper_opening()]

    async def flat(_market):
        return [__test_helper_position('', '0')]

    monkeypatch.setattr(fresh, '_venue_time_ms', venue_time)
    monkeypatch.setattr(fresh, '_confirm_lookup', lookup)
    monkeypatch.setattr(fresh, '_recovery_fill_ids', fills)
    monkeypatch.setattr(fresh, '_fetch_position_rows', flat)
    rows = [__test_helper_position('', '0')]
    asyncio.run(fresh._apply_adoption_baseline(fresh._market, rows))
    assert fresh._adoption_baselined
    return fresh


def __test_close_lost_with_its_process_is_redelivered_once_after_the_restart__(
        tmp_path, monkeypatch):
    """Booked, then the process ended before the engine drained the outbox.

    The journal holds the opening and the close (the close netted the entry
    row), and the next process's engine starts from that position, so both
    go out once more flagged ``journaled_before_restart``: the close from its
    delivery record, with its own ``execId``, price and fee, by the next
    backfill pass; the opening in the engine's next read of the held order.
    Neither is booked into the journal again, no further close is owed, and
    once the engine's receipts exist a later restart delivers nothing.
    """
    stop = __test_helper_execution(
        'e-stop', side='Sell', qty='0.004', price='98000', closed='0.004', fee='0.03',
        ms=__test_helper_OPEN_MS + 60_000, order_id='stop-1', seq=20)
    plugin = __test_helper_handed_over(tmp_path, "restart", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            {'list': [stop], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}],
    })
    asyncio.run(plugin.fetch_order_execution('901', None))
    assert [event.fill_id for event in plugin._event_outbox] == ['e-open', 'e-stop']
    lost = plugin._event_outbox[1]
    later_floor = int(epoch_time() * 1000)

    # Process B: nothing of the outbox reached the engine.
    fresh = __test_helper_restart(plugin, tmp_path, "restart", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [{'list': [__test_helper_opening()], 'nextPageCursor': ''}],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}],
    }, monkeypatch, floor_ms=later_floor)
    closes = [event for event in fresh._inherited_fill_events if event.fill_id == 'e-stop']
    assert len(closes) == 1
    (close,) = closes
    assert close.journaled_before_restart is True
    assert (close.leg_type, close.from_entry, close.pine_id) == (LegType.CLOSE, 'Long', None)
    assert (close.fill_qty, close.fill_price, close.fee, close.fee_currency) == (
        pytest.approx(lost.fill_qty), lost.fill_price, pytest.approx(lost.fee),
        lost.fee_currency)
    assert (close.journal_fill_qty, close.order.side) == (pytest.approx(0.004), 'sell')
    owned = fresh._run_owned_signed_size()

    execution = asyncio.run(fresh.fetch_order_execution('901', None))
    assert execution is not None and execution.fills is not None
    assert [(fill.fill_id, fill.journaled_before_restart) for fill in execution.fills] == [
        ('e-open', True)]
    assert all(event.fill_id != 'e-stop' for event in fresh._event_outbox)
    assert __test_helper_delivered(fresh) == ['e-stop']
    assert fresh._run_owned_signed_size() == pytest.approx(owned) == pytest.approx(0.0)
    assert [endpoint for endpoint, _ in fresh.calls].count('/v5/execution/list') == 1

    # The engine took both (its receipts); the next restart owes nothing.
    fresh.store_ctx.record_fill_receipts([
        FillReceipt('e-open', booked=True, fee=0.02),
        FillReceipt('e-stop', booked=True, fee=0.03),
    ])
    third = __test_helper_restart(fresh, tmp_path, "restart", {}, monkeypatch,
                                  floor_ms=later_floor + 1_000)
    assert all(event.fill_id not in ('e-open', 'e-stop')
               for event in third._inherited_fill_events)
    third.store_ctx.close()


def __test_close_the_engine_took_is_not_redelivered_after_a_restart__(tmp_path, monkeypatch):
    """The engine booked the opening and the close before the process ended: the
    restart finds their receipts and delivers neither again."""
    stop = __test_helper_execution(
        'e-stop', side='Sell', qty='0.004', price='98000', closed='0.004',
        ms=__test_helper_OPEN_MS + 60_000, order_id='stop-1', seq=20)
    plugin = __test_helper_handed_over(tmp_path, "taken", {
        '/v5/order/realtime': [{'list': [__test_helper_order('BTCUSDT', 'Buy', '0.004')]}],
        '/v5/execution/list': [
            {'list': [__test_helper_opening()], 'nextPageCursor': ''},
            {'list': [stop], 'nextPageCursor': ''},
        ],
        '/v5/position/list': [{'list': [__test_helper_position('', '0')]}],
    })
    asyncio.run(plugin.fetch_order_execution('901', None))
    plugin.store_ctx.record_fill_receipts([
        FillReceipt('e-open', booked=True, fee=0.02),
        FillReceipt('e-stop', booked=True, fee=0.02),
    ])
    fresh = __test_helper_restart(plugin, tmp_path, "taken", {}, monkeypatch,
                                  floor_ms=int(epoch_time() * 1000))
    assert all(event.fill_id not in ('e-open', 'e-stop')
               for event in fresh._inherited_fill_events)
    fresh.store_ctx.close()
