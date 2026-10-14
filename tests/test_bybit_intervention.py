"""
@pyne
"""
import asyncio

from pynecore.core.broker.idempotency import KIND_CLOSE, KIND_EXIT_SL, KIND_EXIT_TP
from pynecore.core.broker.models import (
    CancelIntent,
    DispatchEnvelope,
    ExitIntent,
    QuarantineKind,
)
from pynecore.core.broker.protection_guard import PROTECTION_LEVELS_EXTRA
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    create_bracket_ownership_row,
)

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError
from pynecore_bybit.helpers import wire_link_id
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_HEDGE, POSITION_MODE_ONE_WAY


def main():
    """Dummy main to make this a valid Pyne script."""
    pass


# === Fixtures ==============================================================

def _linear_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category='linear', symbol='BTCUSDT', base_coin='BTC', quote_coin='USDT',
        settle_coin='USDT', status='Trading', tick_size_str='0.10', tick_size=0.1,
        qty_step_str='0.001', qty_step=0.001, min_order_qty=0.001, min_order_amt=0.0,
        min_notional=5.0, max_limit_order_qty=1500.0, max_market_order_qty=150.0,
        contract_type='LinearPerpetual', delivery_time=None,
    )


class _InterventionFake(Bybit):
    """Linear Bybit with canned REST answers and an in-request hook.

    ``responses`` are returned (or raised) in order. ``during`` maps an
    endpoint to a callable run inside the request, before the answer —
    a venue push that arrives while the write is in flight.
    """

    def __init__(self, responses=None, *, mode: str = POSITION_MODE_ONE_WAY):
        super().__init__(config=BybitConfig(), symbol='BTCUSDT', timeframe='1')
        self._market = _linear_instrument()
        self._broker_started = True
        self._position_mode = mode
        self.responses = list(responses or [])
        self.during: dict = {}
        self.calls: list = []
        self.quarantines: list = []
        self.on_unexpected_cancel = 'stop'
        self.quarantine_sink = (
            lambda reason, ctx, *, kind: self.quarantines.append((reason, ctx, kind))
        )

    def __call__(self, endpoint, params=None, *, method='get', body=None, auth=False):
        self.calls.append((endpoint, dict(params or {}), dict(body or {})))
        hook = self.during.get(endpoint)
        if hook is not None:
            hook(dict(body or {}))
        if not self.responses:
            raise AssertionError(f"Unexpected REST call: {endpoint} {params} {body}")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _attach_store(plugin, tmp_path, name: str) -> None:
    store = BrokerStore(tmp_path / f"{name}.sqlite", plugin_name=plugin.plugin_name)
    identity = RunIdentity(
        strategy_id="intervention", symbol=plugin._market.symbol, timeframe="1",
        account_id=f"intervention-{name}",
    )
    plugin.store_ctx = store.open_run(identity, script_source="// intervention")


def _restarted(plugin, *, mode: str = POSITION_MODE_ONE_WAY) -> '_InterventionFake':
    """A fresh plugin instance on the same journal (a process restart)."""
    fresh = _InterventionFake(mode=mode)
    fresh.store_ctx = plugin.store_ctx
    return fresh


def _bracket_envelope(*, tp=None, sl=90000.0, qty=0.002,
                      bar_ts_ms=1_752_600_000_000) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=ExitIntent(
            pine_id='TP/SL', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=qty, tp_price=tp, sl_price=sl,
        ),
        run_tag='t3st', bar_ts_ms=bar_ts_ms, coid_max_len=36,
    )


def _trail_envelope(*, bar_ts_ms=1_752_600_000_000) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=ExitIntent(
            pine_id='Trail-X', from_entry='Long', symbol='BTCUSDT', side='sell',
            qty=0.002, tp_price=110000.0, sl_price=90000.0,
            trail_price=105000.0, trail_offset=50.0,
        ),
        run_tag='t3st', bar_ts_ms=bar_ts_ms, coid_max_len=36,
    )


def _order_push(coid: str, *, status='Untriggered', trigger='90000', price='0',
                qty='0.002', order_id='611') -> dict:
    return {'topic': 'order', 'data': [{
        'symbol': 'BTCUSDT', 'orderId': order_id, 'orderLinkId': coid,
        'orderStatus': status, 'side': 'Sell', 'orderType': 'Market',
        'qty': qty, 'cumExecQty': '0', 'price': price, 'triggerPrice': trigger,
        'createdTime': '1752600000000',
    }]}


def _position_push(*, idx=0, side='Buy', size='0.002', tp='110000', sl='90000',
                   trail='5', seq='10') -> dict:
    return {'topic': 'position', 'data': [{
        'symbol': 'BTCUSDT', 'category': 'linear', 'positionIdx': idx,
        'side': side, 'size': size, 'takeProfit': tp, 'stopLoss': sl,
        'trailingStop': trail, 'updatedTime': '1752600000000', 'seq': seq,
    }]}


def _push(plugin, frame) -> list:
    return plugin._translate_private_frame(frame, plugin._market)


def _kinds(plugin) -> list:
    return [kind for _, _, kind in plugin.quarantines]


# === Order-based exit legs =================================================

def __test_moved_sl_leg_trigger_reports_once__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "moved-sl")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)

    _push(plugin, _order_push(coid, trigger='90000'))
    assert plugin.quarantines == []

    _push(plugin, _order_push(coid, trigger='88000'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['protection_key'] == coid
    assert context['observed'][0] == 88000.0

    _push(plugin, _order_push(coid, trigger='87000'))
    assert len(plugin.quarantines) == 1, "one report per protection"


def __test_moved_tp_leg_price_reports__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '610'}])
    _attach_store(plugin, tmp_path, "moved-tp")
    asyncio.run(plugin.execute_exit(_bracket_envelope(tp=110000.0, sl=None)))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_TP)

    _push(plugin, _order_push(coid, status='New', trigger='', price='110000'))
    assert plugin.quarantines == []
    _push(plugin, _order_push(coid, status='New', trigger='', price='115000'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_own_amend_push_before_ack_and_late_push_are_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}, {'orderId': '611'}])
    _attach_store(plugin, tmp_path, "own-amend")
    old = _bracket_envelope()
    asyncio.run(plugin.execute_exit(old))
    coid = old.client_order_id(KIND_EXIT_SL)

    # The venue pushes the amended trigger before the amend ACK returns.
    plugin.during['/v5/order/amend'] = lambda body: _push(
        plugin, _order_push(coid, trigger=body['triggerPrice']))
    new = _bracket_envelope(sl=91000.0)
    asyncio.run(plugin.modify_exit(old, new))
    assert [call[0] for call in plugin.calls][-1] == '/v5/order/amend'
    assert plugin.quarantines == []

    # A late push still carrying the previous own level.
    _push(plugin, _order_push(coid, trigger='90000'))
    assert plugin.quarantines == []
    _push(plugin, _order_push(coid, trigger='91000'))
    assert plugin.quarantines == []


def __test_reduce_only_qty_squeeze_is_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "squeeze")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)

    _push(plugin, _order_push(coid, trigger='90000', qty='0.001'))
    assert plugin.quarantines == []


def __test_moved_sl_leg_is_detected_after_a_restart__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "restart")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    row = plugin.store_ctx.get_order(coid)
    assert PROTECTION_LEVELS_EXTRA in (row.extras or {})

    fresh = _restarted(plugin)
    # The first push after a restart is not relabelled 'amended' in-process.
    _push(fresh, _order_push(coid, trigger='88000'))
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_closed_leg_is_forgotten__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}, {}])
    _attach_store(plugin, tmp_path, "closed-leg")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    plugin._retire_cancelled_row(coid)
    assert not plugin._protection_guard().known(coid)

    _push(plugin, _order_push(coid, trigger='88000'))
    assert plugin.quarantines == []


# === Position trading stop =================================================

def __test_removed_trading_stop_reports_on_the_second_view__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-removed")
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    _push(plugin, _position_push())
    assert plugin.quarantines == []
    removed = _position_push(tp='0', sl='0', trail='0')
    _push(plugin, removed)
    assert plugin.quarantines == [], "a position push needs a confirmation"
    _push(plugin, removed)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['protection_key'] == 'trading-stop:0'


def __test_trailing_stop_movement_is_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-trailing")
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    for stop in ('90500', '91000', '91500'):
        _push(plugin, _position_push(sl=stop))
    assert plugin.quarantines == []


def __test_moved_trading_stop_tp_reports__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-tp")
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    _push(plugin, _position_push(tp='120000'))
    _push(plugin, _position_push(tp='120000'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_flat_position_forgets_the_trading_stop__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-flat")
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    _push(plugin, _position_push(size='0', side='', tp='0', sl='0', trail='0'))
    reopened = _position_push(tp='0', sl='0', trail='0')
    _push(plugin, reopened)
    _push(plugin, reopened)
    assert plugin.quarantines == []


def __test_flipped_position_forgets_the_trading_stop__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-flip")
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    flipped = _position_push(side='Sell', tp='0', sl='0', trail='0')
    _push(plugin, flipped)
    _push(plugin, flipped)
    assert plugin.quarantines == []


def __test_trading_stop_push_during_own_write_is_not_judged__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-inflight")
    removed = _position_push(tp='0', sl='0', trail='0')

    def during(_body) -> None:
        _push(plugin, removed)
        _push(plugin, removed)

    plugin.during['/v5/position/trading-stop'] = during
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    assert plugin.quarantines == []

    # The same view after the write settled is an edit.
    del plugin.during['/v5/position/trading-stop']
    _push(plugin, removed)
    _push(plugin, removed)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_own_trading_stop_clear_is_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{}, {}])
    _attach_store(plugin, tmp_path, "ts-clear")
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    asyncio.run(plugin.execute_cancel(DispatchEnvelope(
        intent=CancelIntent(pine_id='Trail-X', from_entry='Long', symbol='BTCUSDT'),
        run_tag='t3st', bar_ts_ms=1_752_600_060_000, coid_max_len=36,
    )))
    cleared = _position_push(tp='0', sl='0', trail='0')
    _push(plugin, cleared)
    _push(plugin, cleared)
    assert plugin.quarantines == []


def __test_stale_snapshot_read_is_skipped__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-stale")
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    rows = _position_push(tp='0', sl='0', trail='0')['data']

    # Both snapshot reads started before the attach was settled.
    for _ in range(2):
        plugin._observe_position_rows(rows, plugin._market, read_started_at=0.0)
    assert plugin.quarantines == []


def __test_flat_view_older_than_the_attach_keeps_the_stop_guarded__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-stale-flat")
    flat = _position_push(size='0', side='', tp='0', sl='0', trail='0', seq='5')

    # A flat push while the attach is in flight, and a flat snapshot whose
    # read started before it: neither shows the protected position.
    plugin.during['/v5/position/trading-stop'] = lambda _body: _push(plugin, flat)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    del plugin.during['/v5/position/trading-stop']
    plugin._observe_position_rows(flat['data'], plugin._market, read_started_at=0.0)
    assert plugin._protection_guard().known('trading-stop:0')

    removed = _position_push(tp='0', sl='0', trail='0', seq='20')
    _push(plugin, removed)
    _push(plugin, removed)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_delayed_flat_or_flipped_push_keeps_the_stop_guarded__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-delayed-flat")
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    _push(plugin, _position_push(seq='20'))

    # Pushes older than the seq-20 view of the protected position arrive late.
    _push(plugin, _position_push(size='0', side='', tp='0', sl='0', trail='0', seq='5'))
    _push(plugin, _position_push(side='Sell', tp='0', sl='0', trail='0', seq='6'))
    assert plugin._protection_guard().known('trading-stop:0')
    assert plugin.quarantines == []

    removed = _position_push(tp='0', sl='0', trail='0', seq='30')
    _push(plugin, removed)
    _push(plugin, removed)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_helper_refused_tp_amend(tmp_path, name: str, error: BybitAPIError):
    """Attach the trailing bracket, then have the venue answer a TP move with ``error``."""
    plugin = _InterventionFake(responses=[{}, error])
    _attach_store(plugin, tmp_path, name)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    try:
        asyncio.run(plugin.amend_bracket(
            'BTCUSDT', '0', side='sell', tp_price=120000.0, sl_price=90000.0,
            trail_offset=5.0, coid=_trail_envelope().client_order_id(KIND_EXIT_SL),
        ))
    except Exception:  # noqa: BLE001 - the refusal itself is not under test
        pass
    else:
        raise AssertionError("the refused amend must raise")
    return plugin


def __test_refused_trading_stop_levels_stop_being_acceptable__(tmp_path):
    plugin = __test_helper_refused_tp_amend(tmp_path, "ts-refused", BybitAPIError(
        "Bybit API error on /v5/position/trading-stop: retCode=10001 "
        "TakeProfit set for Buy position should higher than base_price",
        ret_code=10001,
    ))
    # The venue refused the 120000 take-profit: seen there, someone else set it.
    moved = _position_push(tp='120000')
    _push(plugin, moved)
    _push(plugin, moved)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_unknown_trading_stop_write_stays_acceptable__(tmp_path):
    plugin = __test_helper_refused_tp_amend(tmp_path, "ts-unknown", BybitAPIError(
        "Bybit API error on /v5/position/trading-stop: retCode=10016 Server error",
        ret_code=10016,
    ))
    # A server-side failure may have landed the write: its levels are own.
    landed = _position_push(tp='120000')
    _push(plugin, landed)
    _push(plugin, landed)
    assert plugin.quarantines == []


class _GatedInterventionFake(_InterventionFake):
    """``_InterventionFake`` whose REST calls, once ``gated``, wait for the test.

    Each gated request is queued as ``(body, future)``; the request returns
    (or raises) whatever the test sets on the future, so overlapping writes
    can be answered in any order.
    """

    def __init__(self, responses=None, *, mode: str = POSITION_MODE_ONE_WAY):
        super().__init__(responses, mode=mode)
        self.gated = False
        self.requests: asyncio.Queue | None = None

    async def _call(self, endpoint, params=None, *, method='get', body=None, auth=False):
        if not self.gated:
            return await super()._call(endpoint, params, method=method, body=body, auth=auth)
        assert self.requests is not None
        answer = asyncio.get_running_loop().create_future()
        await self.requests.put((dict(body or {}), answer))
        result = await answer
        if isinstance(result, Exception):
            raise result
        return result


def __test_overlapping_same_level_writes_settle_by_write_id__(tmp_path):
    plugin = _GatedInterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "ts-overlap")
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    coid = _trail_envelope().client_order_id(KIND_EXIT_SL)

    def amend(tp: float):
        return plugin.amend_bracket(
            'BTCUSDT', '0', side='sell', tp_price=tp, sl_price=90000.0,
            trail_offset=5.0, coid=coid,
        )

    async def overlap():
        plugin.gated = True
        plugin.requests = asyncio.Queue()
        # Three writes in flight at once: 120000, 115000, then 120000 again.
        tasks = []
        answers = []
        for tp in (120000.0, 115000.0, 120000.0):
            tasks.append(asyncio.create_task(amend(tp)))
            body, answer = await plugin.requests.get()
            assert body['takeProfit'] == str(int(tp))
            answers.append(answer)
        early, middle, late = zip(tasks, answers)
        # Answered out of order: the later 120000 lands first, then 115000,
        # and the venue refuses the earlier 120000 last.
        late[1].set_result({})
        await late[0]
        middle[1].set_result({})
        await middle[0]
        early[1].set_result(BybitAPIError(
            "Bybit API error on /v5/position/trading-stop: retCode=10001 "
            "TakeProfit set for Buy position should higher than base_price",
            ret_code=10001,
        ))
        try:
            await early[0]
        except Exception:  # noqa: BLE001 - the refusal itself is not under test
            pass
        else:
            raise AssertionError("the refused amend must raise")

    asyncio.run(overlap())

    # The later 120000 write landed last: the venue carries it, no report.
    _push(plugin, _position_push(tp='120000', seq='40'))
    assert plugin.quarantines == []
    # Seen on the venue, the landed write makes 115000 history at once: seen
    # again, someone else set it.
    _push(plugin, _position_push(tp='115000', seq='41'))
    _push(plugin, _position_push(tp='115000', seq='42'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_hedge_moved_leg_stop_reports_after_restart__(tmp_path):
    plugin = _InterventionFake(responses=[{}], mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "hedge")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:S-X\x00S:2", symbol='BTCUSDT.P', side='buy',
        qty=0.02, intent_key="S-X\x00S", pine_entry_id='S-X', from_entry='S',
        leg_id='2', attach_coid='attach-s', tp_price=1879.0, sl_price=1887.0,
        trail_price=None, trail_offset=None,
    )
    asyncio.run(plugin.amend_bracket(
        'BTCUSDT', '2', side='buy', tp_price=1879.0, sl_price=1887.0,
        trail_offset=None, coid='attach-s',
    ))
    row = plugin.store_ctx.get_order("bo:S-X\x00S:2")
    assert (row.extras or {})[PROTECTION_LEVELS_EXTRA]['key'] == 'trading-stop:2'

    fresh = _restarted(plugin, mode=POSITION_MODE_HEDGE)
    carried = _position_push(idx=2, side='Sell', size='0.02', tp='1879',
                             sl='1887', trail='0')
    _push(fresh, carried)
    assert fresh.quarantines == []
    moved = _position_push(idx=2, side='Sell', size='0.02', tp='1879',
                           sl='1900', trail='0')
    _push(fresh, moved)
    _push(fresh, moved)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


# === Manual close ==========================================================

def _seed_long(plugin, *, qty=0.02) -> None:
    plugin.store_ctx.upsert_order(
        'entry-1', symbol='BTCUSDT', side='buy', qty=qty, filled_qty=qty,
        state='confirmed', intent_key='Long', pine_entry_id='Long',
        extras={'kind': ENTRY_KIND_POSITION},
    )


def _manual_plugin(tmp_path, name: str, *, physical=0.02) -> '_InterventionFake':
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, name)
    _seed_long(plugin)
    plugin._adoption_baselined = True
    # The last position view before the closes (``seq`` 100 and later).
    _push(plugin, _position_push(size=str(physical), tp='0', sl='0', trail='0',
                                 seq='50'))
    return plugin


def _close_exec(**overrides) -> dict:
    row = {
        'symbol': 'BTCUSDT', 'category': 'linear', 'execType': 'Trade',
        'execId': 'm-1', 'orderId': 'manual-1', 'orderLinkId': '',
        'side': 'Sell', 'execQty': '0.02', 'execPrice': '30000',
        'execFee': '0', 'execTime': '1752600000500', 'closedSize': '0.02',
        'stopOrderType': '', 'createType': 'CreateByClosing', 'seq': '100',
    }
    row.update(overrides)
    return row


def _exec_push(*entries) -> dict:
    return {'topic': 'execution', 'data': list(entries)}


def __test_manual_close_reports_once__(tmp_path):
    plugin = _manual_plugin(tmp_path, "manual")
    events = _push(plugin, _exec_push(_close_exec()))
    assert events == [], "the engine adopts the shrink; nothing is booked"
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['create_type'] == 'CreateByClosing'
    assert context['position_idx'] == 0

    # The backfill overlap re-reads the same execution.
    plugin._backfill_one_execution(_close_exec(), plugin._market)
    assert len(plugin.quarantines) == 1


def __test_manual_close_from_the_backfill_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "manual-backfill")
    events = plugin._backfill_one_execution(
        _close_exec(createType='CreateByUser'), plugin._market)
    assert events == []
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_venue_closes_are_never_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "venue")
    for index, overrides in enumerate((
            {'createType': 'CreateByLiq'},
            {'createType': 'CreateByTakeOver_PassThrough'},
            {'createType': 'CreateByAdl_PassThrough'},
            {'createType': 'CreateByAdminClosing'},
            {'createType': 'CreateBySettle'},
            {'execType': 'AdlTrade', 'createType': 'CreateByUser'},
            {'execType': 'BustTrade', 'createType': 'CreateByUser'},
            {'createType': 'CreateByStopLoss', 'stopOrderType': 'StopLoss'},
            {'createType': ''},
            {'createType': 'CreateBySomethingNew'},
    )):
        _push(plugin, _exec_push(_close_exec(execId=f"v-{index}", **overrides)))
    assert plugin.quarantines == []


def __test_bot_shaped_link_id_is_never_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "own-link")
    coid = _bracket_envelope().client_order_id(KIND_CLOSE)
    _push(plugin, _exec_push(
        _close_exec(execId='o-1', orderLinkId=coid, createType='CreateByUser'),
        _close_exec(execId='o-2', orderLinkId=wire_link_id(f"{coid}:1"),
                    createType='CreateByUser'),
    ))
    assert plugin.quarantines == []


def __test_ambiguous_bracket_attribution_is_never_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "ambiguous")
    for exit_id, from_entry in (('A-X', 'A'), ('B-X', 'B')):
        create_bracket_ownership_row(
            plugin.store_ctx, coid=f"attach-{exit_id}:0", symbol='BTCUSDT.P',
            side='sell', qty=0.01, intent_key=f"{exit_id}\x00{from_entry}",
            pine_entry_id=exit_id, from_entry=from_entry, leg_id=str(0),
            attach_coid=f"attach-{exit_id}", tp_price=None, sl_price=29000.0,
            trail_price=None, trail_offset=None,
        )
    _push(plugin, _exec_push(_close_exec(
        execQty='0.015', closedSize='0.015', stopOrderType='StopLoss',
        createType='CreateByStopLoss')))
    assert plugin.quarantines == []
    assert plugin._attribute_venue_bracket_execution(_close_exec(
        execQty='0.015', closedSize='0.015', stopOrderType='StopLoss'),
        plugin._market) == ([], True)


def __test_close_of_foreign_exposure_is_not_reported__(tmp_path):
    # The leg holds more than the run owns: the close may have taken only
    # the foreign share.
    plugin = _manual_plugin(tmp_path, "foreign", physical=0.05)
    _push(plugin, _exec_push(_close_exec()))
    assert plugin.quarantines == []


def __test_close_after_a_foreign_open_waits_for_a_position_view__(tmp_path):
    plugin = _manual_plugin(tmp_path, "foreign-open")
    # A foreign buy of 0.03 lands after the last position view...
    _push(plugin, _exec_push(_close_exec(
        execId='f-1', side='Buy', closedSize='0', execQty='0.03',
        createType='CreateByUser', seq='101')))
    # ... and its own close takes it back out: not the run's exposure.
    _push(plugin, _exec_push(_close_exec(
        execId='f-2', execQty='0.03', closedSize='0.03', seq='102')))
    assert plugin.quarantines == []

    # Once a position view covers the foreign fill, a close of the run's
    # exposure is reported again.
    _push(plugin, _position_push(size='0.02', tp='0', sl='0', trail='0', seq='102'))
    _push(plugin, _exec_push(_close_exec(execId='f-3', seq='103')))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_view_after_a_foreign_close_does_not_prove_ownership__(tmp_path):
    plugin = _manual_plugin(tmp_path, "post-close-view")
    _push(plugin, _exec_push(_close_exec(
        execId='f-1', side='Buy', closedSize='0', execQty='0.03',
        createType='CreateByUser', seq='101')))
    _push(plugin, _position_push(size='0.05', tp='0', sl='0', trail='0', seq='101'))
    # The view the foreign close caused arrives before its execution: it
    # already shows the run's 0.02 alone, but the leg held 0.05 before.
    _push(plugin, _position_push(size='0.02', tp='0', sl='0', trail='0', seq='102'))
    _push(plugin, _exec_push(_close_exec(
        execId='f-2', execQty='0.03', closedSize='0.03', seq='102')))
    assert plugin.quarantines == []


def __test_manual_close_after_the_flat_sweep_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "post-sweep")
    # The flat view of the manual close arrives first and the sweep retires
    # the run's entry row; the execution comes late (reconnect backfill).
    _push(plugin, _position_push(size='0', side='', tp='0', sl='0', trail='0',
                                 seq='100'))
    entry_row = plugin.store_ctx.get_order('entry-1')
    assert entry_row is not None and entry_row.closed_ts_ms is not None
    assert plugin._backfill_one_execution(_close_exec(), plugin._market) == []
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_flat_run_never_reports_a_close__(tmp_path):
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "flat-run")
    plugin._adoption_baselined = True
    plugin._deriv_sizes = {0: 0.02}
    _push(plugin, _exec_push(_close_exec()))
    assert plugin.quarantines == []


def __test_close_before_the_adoption_baseline_is_not_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "pre-adoption")
    plugin._adoption_baselined = False
    plugin._translate_executions(_exec_push(_close_exec()), plugin._market)
    assert plugin.quarantines == []


# === Own flatten ===========================================================

def __test_helper_own_close_row(plugin, coid: str, *, side: str, qty: float) -> None:
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side=side, qty=qty, filled_qty=0.0,
        state='confirmed', intent_key=f"close-{coid}",
        extras={'kind': 'close', 'close_of_entry': 'Long'},
    )


def __test_helper_own_fill(plugin, coid: str, *, exec_id: str, side: str,
                           qty: str, closed: str) -> list:
    return _push(plugin, _exec_push(_close_exec(
        execId=exec_id, orderId=f"own-{exec_id}", orderLinkId=coid, side=side,
        execQty=qty, closedSize=closed, createType='CreateByUser')))


def __test_own_close_then_same_side_reentry_is_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "own-flatten")
    _seed_long(plugin, qty=0.002)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    assert plugin._protection_guard().known('trading-stop:0')

    __test_helper_own_close_row(plugin, 'close-1', side='sell', qty=0.002)
    events = __test_helper_own_fill(plugin, 'close-1', exec_id='c-1',
                                    side='Sell', qty='0.002', closed='0.002')
    assert len(events) == 1
    assert not plugin._protection_guard().known('trading-stop:0')

    # The bot re-enters long before any flat position view arrived.
    plugin.store_ctx.upsert_order(
        'entry-2', symbol='BTCUSDT', side='buy', qty=0.002, filled_qty=0.002,
        state='confirmed', intent_key='Long-2', pine_entry_id='Long',
        extras={'kind': ENTRY_KIND_POSITION},
    )
    unprotected = _position_push(tp='0', sl='0', trail='0', seq='20')
    _push(plugin, unprotected)
    _push(plugin, unprotected)
    assert plugin.quarantines == []


def __test_own_partial_reduce_keeps_the_trading_stop_guarded__(tmp_path):
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "own-partial")
    _seed_long(plugin, qty=0.002)
    asyncio.run(plugin.execute_exit(_trail_envelope()))

    __test_helper_own_close_row(plugin, 'close-1', side='sell', qty=0.001)
    __test_helper_own_fill(plugin, 'close-1', exec_id='c-1', side='Sell',
                           qty='0.001', closed='0.001')
    assert plugin._protection_guard().known('trading-stop:0')

    moved = _position_push(size='0.001', tp='120000')
    _push(plugin, moved)
    _push(plugin, moved)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_hedge_own_close_forgets_only_its_leg__(tmp_path):
    plugin = _InterventionFake(responses=[{}], mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "hedge-own-flatten")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:S-X\x00S:2", symbol='BTCUSDT.P', side='buy',
        qty=0.02, intent_key="S-X\x00S", pine_entry_id='S-X', from_entry='S',
        leg_id='2', attach_coid='attach-s', tp_price=1879.0, sl_price=1887.0,
        trail_price=None, trail_offset=None,
    )
    asyncio.run(plugin.amend_bracket(
        'BTCUSDT', '2', side='buy', tp_price=1879.0, sl_price=1887.0,
        trail_offset=None, coid='attach-s',
    ))
    _seed_long(plugin, qty=0.01)
    plugin.store_ctx.upsert_order(
        'entry-s', symbol='BTCUSDT', side='sell', qty=0.02, filled_qty=0.02,
        state='confirmed', intent_key='S', pine_entry_id='S',
        extras={'kind': ENTRY_KIND_POSITION},
    )

    # Flattening the buy leg leaves the sell leg's stop guarded.
    __test_helper_own_close_row(plugin, 'close-b', side='sell', qty=0.01)
    __test_helper_own_fill(plugin, 'close-b', exec_id='c-b', side='Sell',
                           qty='0.01', closed='0.01')
    assert plugin._protection_guard().known('trading-stop:2')

    __test_helper_own_close_row(plugin, 'close-s', side='buy', qty=0.02)
    __test_helper_own_fill(plugin, 'close-s', exec_id='c-s', side='Buy',
                           qty='0.02', closed='0.02')
    assert not plugin._protection_guard().known('trading-stop:2')
