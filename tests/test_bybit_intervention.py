"""
@pyne
"""
import asyncio
import logging
import random
import sqlite3
import time
from collections.abc import AsyncGenerator, Callable
from typing import cast

import pytest

from pynecore.core.broker.exceptions import (
    BracketAttachAfterFillRejectedError,
    OrderDispositionUnknownError,
)
from pynecore.core.broker.idempotency import (
    KIND_CLOSE,
    KIND_ENTRY,
    KIND_EXIT_SL,
    KIND_EXIT_TP,
)
from pynecore.core.broker.models import (
    VENUE_DRIVEN_CANCEL_REASONS,
    CancelIntent,
    DispatchEnvelope,
    EntryIntent,
    ExitIntent,
    LegType,
    OrderType,
    QuarantineKind,
)
from pynecore.core.broker.protection_guard import (
    PROTECTION_LEVELS_EXTRA,
    ProtectionLevels,
    rebaseline_after_resume,
)
from pynecore.core.broker.run_identity import RunIdentity
from pynecore.core.broker.storage import BrokerStore
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    create_bracket_ownership_row,
)
from pynecore.core.broker.sync_engine import OrderSyncEngine

from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, BybitConnectionError
from pynecore_bybit.helpers import EXECUTION_CURSOR_OVERLAP_MS, wire_link_id
from pynecore_bybit.ledger import ORIGIN_USER, LegEvent, LegLedger
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
        self.during: dict[str, Callable[[dict], object]] = {}
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
    # Each view stamps its own ``updatedTime``: a repeated one marks an echo.
    return {'topic': 'position', 'data': [{
        'symbol': 'BTCUSDT', 'category': 'linear', 'positionIdx': idx,
        'side': side, 'size': size, 'takeProfit': tp, 'stopLoss': sl,
        'trailingStop': trail, 'updatedTime': str(1752600000000 + int(seq)), 'seq': seq,
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


# === Manual close: the run's share of a shared leg =========================

def _seed_long(plugin, *, qty=0.02, side='buy', coid='entry-1') -> None:
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side=side, qty=qty, filled_qty=qty,
        state='confirmed', intent_key=coid, pine_entry_id='Long',
        extras={'kind': ENTRY_KIND_POSITION},
    )


def __test_helper_view(*, size, idx=0, side=None, seq) -> dict:
    """A position push without any trading stop."""
    if side is None:
        side = ('Buy' if idx in (0, 1) else 'Sell') if float(size) > 0 else ''
    return _position_push(idx=idx, side=side, size=str(size), tp='0', sl='0',
                          trail='0', seq=str(seq))


def _manual_plugin(tmp_path, name: str, *, physical=0.02) -> '_InterventionFake':
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, name)
    _seed_long(plugin)
    plugin._adoption_baselined = True
    # The last position view before the closes (``seq`` 100 and later).
    _push(plugin, __test_helper_view(size=physical, seq=50))
    return plugin


def __test_helper_hedge_plugin(tmp_path, name: str, *, physical=0.02) -> '_InterventionFake':
    """A hedge run owning 0.02 on the Buy leg (``positionIdx`` 1)."""
    plugin = _InterventionFake(mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, name)
    _seed_long(plugin)
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_view(idx=1, size=physical, seq=50))
    _push(plugin, __test_helper_view(idx=2, size=0, seq=50))
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


def __test_helper_foreign_open(exec_id: str, *, side='Buy', qty='0.01', seq) -> dict:
    return _close_exec(execId=exec_id, orderId=f"order-{exec_id}", side=side,
                       execQty=qty, closedSize='0', createType='CreateByUser',
                       seq=str(seq))


def __test_helper_foreign_close(exec_id: str, *, side='Sell', qty, seq, **overrides) -> dict:
    return _close_exec(execId=exec_id, orderId=f"order-{exec_id}", side=side,
                       execQty=qty, closedSize=qty, seq=str(seq), **overrides)


def _exec_push(*entries) -> dict:
    return {'topic': 'execution', 'data': list(entries)}


def __test_helper_audits(plugin, kind: str) -> list:
    return [payload for _, _, _, payload
            in plugin.store_ctx.iter_events_by_kind_for_run_id(kind)]


def __test_helper_clock(monkeypatch) -> list:
    """Freeze the plugin clock; the returned one-element list moves it."""
    clock = [1_752_600_000.0]
    monkeypatch.setattr('pynecore_bybit.protection.epoch_time', lambda: clock[0])
    return clock


def __test_manual_close_reports_once__(tmp_path):
    plugin = _manual_plugin(tmp_path, "manual")
    events = _push(plugin, _exec_push(_close_exec()))
    assert events == [], "the engine adopts the shrink; nothing is booked"
    # The verdict waits for the view the close caused.
    assert plugin.quarantines == []
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['create_type'] == 'CreateByClosing'
    assert context['position_idx'] == 0
    assert context['consumed'] == pytest.approx(0.02)

    # The backfill overlap re-reads the same execution.
    plugin._backfill_one_execution(_close_exec(), plugin._market)
    assert len(plugin.quarantines) == 1


def __test_manual_close_from_the_backfill_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "manual-backfill")
    _push(plugin, __test_helper_view(size=0, seq=100))
    events = plugin._backfill_one_execution(
        _close_exec(createType='CreateByUser'), plugin._market)
    assert events == []
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_venue_closes_are_never_reported__(tmp_path):
    cases = (
        ({'createType': 'CreateByLiq'}, 'own_share_clamped'),
        ({'createType': 'CreateByTakeOver_PassThrough'}, 'own_share_clamped'),
        ({'createType': 'CreateByAdl_PassThrough'}, 'own_share_clamped'),
        ({'createType': 'CreateByAdminClosing'}, 'own_share_clamped'),
        ({'createType': 'CreateBySettle'}, 'own_share_clamped'),
        ({'execType': 'AdlTrade', 'createType': 'CreateByUser'}, 'own_share_clamped'),
        ({'execType': 'BustTrade', 'createType': 'CreateByUser'}, 'own_share_clamped'),
        ({'createType': ''}, 'external_close_unproven'),
        ({'createType': 'CreateBySomethingNew'}, 'external_close_unproven'),
    )
    for index, (overrides, audit) in enumerate(cases):
        plugin = _manual_plugin(tmp_path, f"venue-{index}")
        _push(plugin, _exec_push(_close_exec(execId=f"v-{index}", **overrides)))
        _push(plugin, __test_helper_view(size=0, seq=100))
        assert plugin.quarantines == [], overrides
        # The run's share is gone either way: the journal books the venue
        # truth, with an audit event naming why.
        assert len(__test_helper_audits(plugin, audit)) == 1, overrides
        assert plugin._run_owned_signed_size() == 0.0, overrides


def __test_helper_stop_fire(exec_id: str, stop_type: str, *, side='Sell', qty='0.02',
                            seq='100') -> dict:
    """A fired ``Full`` position TP/SL / trailing stop: no link id, venue-created."""
    return _close_exec(execId=exec_id, orderId=f"order-{exec_id}", side=side,
                       execQty=qty, closedSize=qty, stopOrderType=stop_type,
                       createType=f"CreateBy{stop_type}", seq=seq)


def __test_helper_next_instance(plugin, name: str, *,
                                mode: str = POSITION_MODE_ONE_WAY) -> '_InterventionFake':
    """End the run instance and open the next one of the same run (a restart)."""
    store = plugin.store_ctx._store
    plugin.store_ctx.close()
    identity = RunIdentity(
        strategy_id="intervention", symbol=plugin._market.symbol, timeframe="1",
        account_id=f"intervention-{name}",
    )
    fresh = _InterventionFake(mode=mode)
    fresh.store_ctx = store.open_run(identity, script_source="// intervention")
    fresh._adoption_baselined = True
    return fresh


def __test_full_stop_fire_the_run_never_set_reports__(tmp_path):
    # A protection the bot never set closed the bot's position: someone
    # else placed it.
    for stop_type in ('StopLoss', 'TakeProfit', 'TrailingStop'):
        plugin = _manual_plugin(tmp_path, f"never-set-{stop_type}")
        _push(plugin, _exec_push(__test_helper_stop_fire(f"s-{stop_type}", stop_type)))
        _push(plugin, __test_helper_view(size=0, seq=100))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], stop_type
        _, context, _ = plugin.quarantines[0]
        assert context['stop_order_type'] == stop_type
        assert context['consumed'] == pytest.approx(0.02)
        assert plugin._run_owned_signed_size() == 0.0

    # Hedge: the run's own bracket history on the other leg proves nothing
    # about this one.
    plugin = __test_helper_hedge_plugin(tmp_path, "never-set-hedge")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="attach-s:2", symbol='BTCUSDT.P', side='buy',
        qty=0.02, intent_key="S-X\x00S", pine_entry_id='S-X', from_entry='S',
        leg_id='2', attach_coid='attach-s', tp_price=None, sl_price=31000.0,
        trail_price=None, trail_offset=None,
    )
    plugin.store_ctx.close_order('attach-s:2')
    _push(plugin, _exec_push(__test_helper_stop_fire('s-h', 'StopLoss', qty='0.005')))
    _push(plugin, __test_helper_view(idx=1, size=0.015, seq=100))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['position_idx'] == 1


def __test_full_stop_fire_on_a_position_the_run_once_protected_stays_unproven__(tmp_path):
    for stop_type in ('StopLoss', 'TrailingStop'):
        name = f"once-protected-{stop_type}"
        first = _manual_plugin(tmp_path, name)
        # An earlier instance of the run owned a trailing stop on the one-way
        # position; its row was released before the restart.
        create_bracket_ownership_row(
            first.store_ctx, coid="attach-old:0", symbol='BTCUSDT.P', side='sell',
            qty=0.02, intent_key="X\x00Long", pine_entry_id='X', from_entry='Long',
            leg_id='0', attach_coid='attach-old', tp_price=None, sl_price=29000.0,
            trail_price=None, trail_offset=50.0,
        )
        first.store_ctx.close_order('attach-old:0')
        plugin = __test_helper_next_instance(first, name)
        released = list(plugin.store_ctx.iter_orders_in_state_for_run_id('bracket_own'))
        assert [row.client_order_id for row in released] == ['attach-old:0']
        assert released[0].closed_ts_ms is not None
        assert plugin._run_owned_signed_size() == pytest.approx(0.02)
        _push(plugin, __test_helper_view(size=0.02, seq=50))
        _push(plugin, _exec_push(__test_helper_stop_fire(f"s-{stop_type}", stop_type)))
        _push(plugin, __test_helper_view(size=0, seq=100))
        # The released stop may still have stood: possibly the bot's own.
        assert plugin.quarantines == [], stop_type
        assert len(__test_helper_audits(plugin, 'external_close_unproven')) == 1, stop_type
        assert plugin._run_owned_signed_size() == 0.0, stop_type


def __test_bot_shaped_link_id_is_never_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "own-link")
    coid = _bracket_envelope().client_order_id(KIND_CLOSE)
    _push(plugin, _exec_push(
        _close_exec(execId='o-1', orderLinkId=coid, createType='CreateByUser',
                    execQty='0.01', closedSize='0.01'),
        _close_exec(execId='o-2', orderLinkId=wire_link_id(f"{coid}:1"),
                    createType='CreateByUser', execQty='0.01', closedSize='0.01'),
    ))
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'external_close_unproven')) == 2


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
    _push(plugin, __test_helper_view(size=0.005, seq=100))
    assert plugin.quarantines == []
    assert plugin._attribute_venue_bracket_execution(_close_exec(
        execQty='0.015', closedSize='0.015', stopOrderType='StopLoss'),
        plugin._market) == ([], True)


def __test_close_of_foreign_exposure_is_not_reported__(tmp_path):
    # The leg holds more than the run owns: the close took only the foreign
    # share.
    plugin = _manual_plugin(tmp_path, "foreign", physical=0.05)
    _push(plugin, _exec_push(_close_exec(execQty='0.03', closedSize='0.03')))
    _push(plugin, __test_helper_view(size=0.02, seq=100))
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1


def __test_close_after_a_foreign_open_waits_for_a_position_view__(tmp_path):
    plugin = _manual_plugin(tmp_path, "foreign-open")
    # A foreign buy of 0.03 lands after the last position view...
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=101)))
    # ... and its own close takes it back out: not the run's exposure.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.03', seq=102)))
    assert plugin.quarantines == []
    _push(plugin, __test_helper_view(size=0.02, seq=102))
    assert plugin.quarantines == []

    # A close of the run's exposure is reported once its view arrives.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-3', qty='0.02', seq=103)))
    assert plugin.quarantines == []
    _push(plugin, __test_helper_view(size=0, seq=103))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_view_after_a_foreign_close_does_not_prove_ownership__(tmp_path):
    plugin = _manual_plugin(tmp_path, "post-close-view")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=101)))
    _push(plugin, __test_helper_view(size=0.05, seq=101))
    # The view the foreign close caused arrives before its execution: it
    # already shows the run's 0.02 alone, but the leg held 0.05 before.
    _push(plugin, __test_helper_view(size=0.02, seq=102))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.03', seq=102)))
    assert plugin.quarantines == []


def __test_manual_close_after_the_flat_sweep_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "post-sweep")
    # The flat view of the manual close arrives first; the execution comes
    # late (reconnect backfill). The flat sweep waits for the execution the
    # view needs: it might be the run's own close.
    _push(plugin, __test_helper_view(size=0, seq=100))
    entry_row = plugin.store_ctx.get_order('entry-1')
    assert entry_row is not None and entry_row.closed_ts_ms is None
    assert plugin._backfill_one_execution(_close_exec(), plugin._market) == []
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    entry_row = plugin.store_ctx.get_order('entry-1')
    assert entry_row is not None and entry_row.closed_ts_ms is not None


def __test_reentry_booked_before_a_late_close_execution_stays_owned__(tmp_path):
    plugin = _manual_plugin(tmp_path, "reentry")
    # Someone closes the run's position; its flat view arrives first and the
    # sweep retires the run's entry row.
    _push(plugin, __test_helper_view(size=0, seq=100))
    # The bot re-enters before the close's own execution is delivered.
    plugin.store_ctx.upsert_order(
        'entry-2', symbol='BTCUSDT', side='buy', qty=0.01, filled_qty=0.0,
        state='confirmed', intent_key='Long-2', pine_entry_id='Long',
        exchange_order_id='venue-entry-2', extras={'kind': ENTRY_KIND_POSITION},
    )
    events = _push(plugin, _exec_push(_close_exec(
        execId='own-in', orderId='venue-entry-2', orderLinkId='entry-2',
        side='Buy', execQty='0.01', closedSize='0', seq='120')))
    assert len(events) == 1
    _push(plugin, __test_helper_view(size=0.01, seq=120))
    # The close's execution comes from the backfill.
    plugin._backfill_one_execution(_close_exec(), plugin._market)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)


def __test_flat_run_never_reports_a_close__(tmp_path):
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "flat-run")
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_view(size=0.02, seq=50))
    _push(plugin, _exec_push(_close_exec()))
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert plugin.quarantines == []


def __test_close_before_the_adoption_baseline_is_not_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "pre-adoption")
    plugin._adoption_baselined = False
    plugin._translate_executions(_exec_push(_close_exec()), plugin._market)
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert plugin.quarantines == []


# --- the scenario matrix: add / overclose / close own -----------------------

def __test_add_overclose_reports__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = (__test_helper_hedge_plugin if mode == POSITION_MODE_HEDGE
                  else _manual_plugin)(tmp_path, f"overclose-{mode}")
        _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
        _push(plugin, __test_helper_view(idx=idx, size=0.03, seq=101))
        _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.015', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.015, seq=102))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['position_idx'] == idx
        assert context['physical_after'] == pytest.approx(0.015)
        assert context['owned'] == pytest.approx(0.02)
        assert context['consumed'] == pytest.approx(0.005)
        # The journal books the venue truth: the run owns what is left.
        assert plugin._run_owned_signed_size() == pytest.approx(0.015)


def __test_add_overclose_without_intermediate_view_reports_after_backfill_reread__(tmp_path):
    plugin = _manual_plugin(tmp_path, "overclose-reread")
    foreign_open = __test_helper_foreign_open('f-1', qty='0.01', seq=101)
    _push(plugin, _exec_push(foreign_open))
    # The backfill overlap re-delivers the foreign open: counted once.
    plugin._backfill_one_execution(foreign_open, plugin._market)
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.015', seq=102)))
    _push(plugin, __test_helper_view(size=0.015, seq=102))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_add_overclose_in_two_orders_without_views_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "overclose-two")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.01', seq=102)))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-3', qty='0.005', seq=103)))
    _push(plugin, __test_helper_view(size=0.015, seq=103))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['exec_id'] == 'f-3'
    assert context['consumed'] == pytest.approx(0.005)
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1


def __test_external_execution_is_judged_once_across_the_backfill_overlap__(tmp_path):
    plugin = _manual_plugin(tmp_path, "judged-once")
    foreign_open = __test_helper_foreign_open('f-1', qty='0.01', seq=101)
    foreign_close = __test_helper_foreign_close('f-2', qty='0.01', seq=102)
    _push(plugin, _exec_push(foreign_open))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    _push(plugin, _exec_push(foreign_close))
    _push(plugin, __test_helper_view(size=0.02, seq=102))
    # The backfill overlap re-reads both executions after they were judged.
    for row in (foreign_open, foreign_close):
        assert plugin._backfill_one_execution(row, plugin._market) == []
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1
    assert __test_helper_audits(plugin, 'external_close_unproven') == []


def __test_add_then_close_own_is_not_reported__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = (__test_helper_hedge_plugin if mode == POSITION_MODE_HEDGE
                  else _manual_plugin)(tmp_path, f"close-own-{mode}")
        foreign_open = __test_helper_foreign_open('f-1', qty='0.01', seq=101)
        _push(plugin, _exec_push(foreign_open))
        plugin._backfill_one_execution(foreign_open, plugin._market)
        _push(plugin, __test_helper_view(idx=idx, size=0.03, seq=101))
        _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=102))
        assert plugin.quarantines == [], mode
        assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1, mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.02), mode


def __test_foreign_opposite_order_flip_reports_close_part__(tmp_path):
    plugin = _manual_plugin(tmp_path, "flip")
    # One foreign sell closes the run's 0.02 and opens 0.03 short.
    _push(plugin, _exec_push(_close_exec(execQty='0.05', closedSize='0.02',
                                         createType='CreateByUser')))
    _push(plugin, __test_helper_view(size=0.03, side='Sell', seq=100))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['consumed'] == pytest.approx(0.02)


def __test_hedge_part_close_and_full_close_report__(tmp_path):
    for name, qty, left in (('part', '0.005', 0.015), ('full', '0.02', 0.0)):
        plugin = __test_helper_hedge_plugin(tmp_path, f"hedge-{name}")
        _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty=qty, seq=100)))
        _push(plugin, __test_helper_view(idx=1, size=left, seq=100))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name
        _, context, _ = plugin.quarantines[0]
        assert context['position_idx'] == 1


def __test_hedge_opposite_leg_foreign_trade_is_not_reported__(tmp_path):
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-other-leg")
    # Someone else opens and closes a short on the Sell leg.
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', side='Sell', qty='0.01', seq=101)))
    _push(plugin, __test_helper_view(idx=2, size=0.01, seq=101))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', side='Buy', qty='0.01', seq=102)))
    _push(plugin, __test_helper_view(idx=2, size=0, seq=102))
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == pytest.approx(0.02)


# --- one-way reversals over foreign exposure --------------------------------

def __test_helper_reversal_row(plugin, coid: str, *, side: str, qty: float) -> None:
    """A folded one-way reversal entry: closes the run's side and opens the other."""
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side=side, qty=qty, filled_qty=0.0,
        state='confirmed', intent_key=f"{coid}-key",
        pine_entry_id='Short' if side == 'sell' else 'Long',
        exchange_order_id=f"venue-{coid}", extras={'kind': ENTRY_KIND_POSITION},
    )


def __test_helper_reversal_exec(coid: str, *, exec_id: str, side: str, qty: str,
                                closed: str, seq) -> dict:
    return _close_exec(execId=exec_id, orderId=f"venue-{coid}", orderLinkId=coid,
                       side=side, execQty=qty, closedSize=closed,
                       createType='CreateByUser', seq=str(seq))


def __test_helper_reverse_twice(plugin, *, foreign_open: dict) -> None:
    """The run (long 0.02) reverses short and back over someone's 0.01 add."""
    _push(plugin, _exec_push(foreign_open))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    __test_helper_reversal_row(plugin, 'rev-1', side='sell', qty=0.04)
    _push(plugin, _exec_push(__test_helper_reversal_exec(
        'rev-1', exec_id='o-102', side='Sell', qty='0.04', closed='0.03', seq=102)))
    _push(plugin, __test_helper_view(size=0.01, side='Sell', seq=102))
    assert plugin._run_owned_signed_size() == pytest.approx(-0.02)
    __test_helper_reversal_row(plugin, 'rev-2', side='buy', qty=0.04)
    _push(plugin, _exec_push(__test_helper_reversal_exec(
        'rev-2', exec_id='o-103', side='Buy', qty='0.04', closed='0.01', seq=103)))
    _push(plugin, __test_helper_view(size=0.03, seq=103))
    assert plugin._run_owned_signed_size() == pytest.approx(0.02)


def __test_oneway_folded_reversals_after_a_same_add_never_report_the_cleanup__(tmp_path):
    plugin = _manual_plugin(tmp_path, "reversal-cleanup")
    __test_helper_reverse_twice(
        plugin, foreign_open=__test_helper_foreign_open('f-1', qty='0.01', seq=101))
    # The first reversal netted the foreign 0.01 out of the leg: logged, never
    # charged, the journal is left as the run's fills built it.
    assert len(__test_helper_audits(plugin, 'own_fill_consumed_foreign_exposure')) == 1
    # The foreign party takes its 0.01 back out: the leg keeps the run's 0.02.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.01', seq=104)))
    _push(plugin, __test_helper_view(size=0.02, seq=104))
    assert plugin.quarantines == []
    assert __test_helper_audits(plugin, 'external_close_unproven') == []
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1
    assert plugin._run_owned_signed_size() == pytest.approx(0.02)
    assert plugin._leg_ledgers[0].share == pytest.approx(0.02)


def __test_oneway_reversal_with_a_pyne_peer_logs_no_unproven_close__(tmp_path):
    # The same exposure belongs to another PyneCore run on the account.
    plugin = _manual_plugin(tmp_path, "reversal-peer")
    peer = DispatchEnvelope(
        intent=EntryIntent(pine_id='Peer', symbol='BTCUSDT', side='buy', qty=0.01,
                           order_type=OrderType.MARKET),
        run_tag='p33r', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )
    peer_open = peer.client_order_id(KIND_ENTRY)
    peer_close = peer.client_order_id(KIND_CLOSE)
    __test_helper_reverse_twice(plugin, foreign_open=__test_helper_foreign_open(
        'f-1', qty='0.01', seq=101) | {'orderLinkId': peer_open})
    _push(plugin, _exec_push(__test_helper_foreign_close(
        'f-2', qty='0.01', seq=104, orderLinkId=peer_close)))
    _push(plugin, __test_helper_view(size=0.02, seq=104))
    assert plugin.quarantines == []
    assert __test_helper_audits(plugin, 'external_close_unproven') == []
    assert plugin._run_owned_signed_size() == pytest.approx(0.02)


def __test_oneway_reversal_clip_still_reports_a_real_close_of_the_run_share__(tmp_path):
    plugin = _manual_plugin(tmp_path, "reversal-real-close")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    __test_helper_reversal_row(plugin, 'rev-1', side='sell', qty=0.04)
    _push(plugin, _exec_push(__test_helper_reversal_exec(
        'rev-1', exec_id='o-102', side='Sell', qty='0.04', closed='0.03', seq=102)))
    _push(plugin, __test_helper_view(size=0.01, side='Sell', seq=102))
    assert len(__test_helper_audits(plugin, 'own_fill_consumed_foreign_exposure')) == 1
    # Someone buys 0.005 against the run's short: the leg the run holds
    # shrinks below what it held.
    _push(plugin, _exec_push(__test_helper_foreign_close(
        'f-2', side='Buy', qty='0.005', seq=103)))
    _push(plugin, __test_helper_view(size=0.005, side='Sell', seq=103))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['exec_id'] == 'f-2'
    assert context['consumed'] == pytest.approx(0.005)


def __test_oneway_close_of_the_old_side_before_an_own_reversal_is_netted__(tmp_path):
    cases = (
        ('adl', 'CreateByAdl_PassThrough', 'one-view'),
        ('adl', 'CreateByAdl_PassThrough', 'late-view'),
        ('user', 'CreateByUser', 'one-view'),
        ('user', 'CreateByUser', 'late-view'),
    )
    for origin, create_type, delivery in cases:
        case = f"{origin}-{delivery}"
        # The run is long 0.02 (view seq 50).
        plugin = _manual_plugin(tmp_path, f"old-side-{case}")
        __test_helper_reversal_row(plugin, 'rev-1', side='sell', qty=0.04)
        # One push: someone else's close takes 0.01 of the run's long, then
        # the run's own folded reversal sells 0.04.
        _push(plugin, _exec_push(
            __test_helper_foreign_close('c-1', qty='0.01', seq=101, createType=create_type),
            __test_helper_reversal_exec('rev-1', exec_id='o-102', side='Sell', qty='0.04',
                                        closed='0.01', seq=102),
        ))
        if delivery == 'late-view':
            _push(plugin, __test_helper_view(size=0.01, seq=101))
        _push(plugin, __test_helper_view(size=0.03, side='Sell', seq=102))
        expected = [] if origin == 'adl' else [QuarantineKind.EXTERNAL_INTERVENTION]
        assert _kinds(plugin) == expected, case
        # The journal books the venue truth: the long row is netted although
        # the reversal already took the journal's signed total short.
        assert plugin._run_owned_signed_size() == pytest.approx(-0.03), case
        assert plugin.store_ctx.get_order('entry-1').filled_qty == pytest.approx(0.01), case
        assert plugin._leg_ledgers[0].share == pytest.approx(-0.03), case
        # Someone buys 0.01 against the run's own 0.03 short: the leg holds
        # no foreign exposure, so it is the run's share it took.
        _push(plugin, _exec_push(__test_helper_foreign_close(
            'u-2', side='Buy', qty='0.01', seq=103, createType='CreateByUser')))
        _push(plugin, __test_helper_view(size=0.02, side='Sell', seq=103))
        assert _kinds(plugin) == expected + [QuarantineKind.EXTERNAL_INTERVENTION], case
        _, context, _ = plugin.quarantines[-1]
        assert context['exec_id'] == 'u-2', case
        assert context['consumed'] == pytest.approx(0.01), case
        assert plugin._run_owned_signed_size() == pytest.approx(-0.02), case


def __test_oneway_closes_of_both_sides_around_an_own_reversal_net_each_side__(tmp_path):
    plugin = _manual_plugin(tmp_path, "both-sides")
    __test_helper_reversal_row(plugin, 'rev-1', side='sell', qty=0.04)
    # An ADL takes 0.01 of the run's long, the run reverses, and someone buys
    # 0.01 against the run's new short: one interval, a verdict on each side.
    _push(plugin, _exec_push(
        __test_helper_foreign_close('adl-1', qty='0.01', seq=101,
                                    createType='CreateByAdl_PassThrough'),
        __test_helper_reversal_exec('rev-1', exec_id='o-102', side='Sell', qty='0.04',
                                    closed='0.01', seq=102),
        __test_helper_foreign_close('u-1', side='Buy', qty='0.01', seq=103,
                                    createType='CreateByUser'),
    ))
    _push(plugin, __test_helper_view(size=0.02, side='Sell', seq=103))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['exec_id'] == 'u-1'
    assert context['consumed'] == pytest.approx(0.01)
    # Each side's rows lose what its verdict took.
    assert plugin.store_ctx.get_order('entry-1').filled_qty == pytest.approx(0.01)
    assert plugin.store_ctx.get_order('rev-1').filled_qty == pytest.approx(0.03)
    assert plugin._run_owned_signed_size() == pytest.approx(-0.02)


def __test_ledger_share_never_exceeds_the_journal__():
    """Random one-way runs: own adds, reduces and folded reversals among
    foreign adds and closes. The share stays within the journal, on its
    side, and a foreign execution is charged only for what it took below
    the journal share."""
    tolerance = 0.0005
    rng = random.Random(20261015)
    clips = charged = 0
    for case in range(400):
        ledger = LegLedger()
        journal = rng.choice((-0.02, 0.0, 0.02))
        leg = journal + rng.choice((0.0, 0.01)) * (1.0 if journal >= 0.0 else -1.0)
        seq = 100
        ledger.add_view(seq, leg)
        assert ledger.start(journal, tolerance) is None
        known: set[int] = set()
        for step in range(40):
            seq += 1
            qty = rng.randint(1, 4) * 0.01
            direction = 1.0 if journal > tolerance else -1.0 if journal < -tolerance else 0.0
            action = rng.choice(('own_add', 'own_reduce', 'own_reverse',
                                 'foreign_buy', 'foreign_sell'))
            if action.startswith('own'):
                if action == 'own_add' or not direction:
                    delta = qty * (direction or rng.choice((1.0, -1.0)))
                elif action == 'own_reduce':
                    delta = -direction * min(qty, abs(journal))
                else:
                    delta = -direction * (abs(journal) + qty)
                event = LegEvent(f"o{seq}", seq, delta, own=True, share=delta)
                journal += delta
            else:
                delta = qty if action == 'foreign_buy' else -qty
                event = LegEvent(f"f{seq}", seq, delta, own=False, origin=ORIGIN_USER)
            journal_before = journal
            leg += delta
            known.add(seq)
            ledger.add_event(event)
            ledger.add_view(seq, leg)
            ledger.sync_journal(journal, tolerance)
            for outcome in ledger.advance(0.0, 90.0, tolerance, known):
                assert outcome.proven
                assert outcome.event is not None
                if outcome.event.own:
                    clips += outcome.clipped > tolerance
                    continue
                if outcome.consumed > tolerance:
                    charged += 1
                    held = leg * (1.0 if journal_before > 0.0 else -1.0)
                    assert held < abs(journal_before) - tolerance, (case, step)
                    assert outcome.share_before <= abs(journal_before) + tolerance
            # The plugin nets each verdict off the side it took.
            taken = ledger.take_to_net()
            journal -= taken.get(1.0, 0.0) - taken.get(-1.0, 0.0)
            assert journal == pytest.approx(ledger.journal, abs=1e-9), (case, step)
            if abs(ledger.share) > tolerance:
                assert ledger.share * journal > 0.0, (case, step)
                assert abs(ledger.share) <= abs(journal) + tolerance, (case, step)
    assert clips > 0 and charged > 0, "both paths are exercised"


# --- executions outside ``Trade`` and executions without ``seq`` -------------

def __test_move_position_taking_the_run_share_reports__(tmp_path):
    for path in ('push', 'backfill'):
        plugin = _manual_plugin(tmp_path, f"move-position-{path}")
        moved = _close_exec(execId='mp-1', orderId='mp-order', execType='MovePosition',
                            createType='', execQty='0.01', closedSize='0.01')
        if path == 'push':
            _push(plugin, _exec_push(moved))
        else:
            assert plugin._backfill_one_execution(moved, plugin._market) == []
        _push(plugin, __test_helper_view(size=0.01, seq=100))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], path
        _, context, _ = plugin.quarantines[0]
        assert context['exec_id'] == 'mp-1', path
        assert context['consumed'] == pytest.approx(0.01), path
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), path


def __test_future_spread_add_never_reports__(tmp_path):
    for path in ('push', 'backfill'):
        plugin = _manual_plugin(tmp_path, f"spread-add-{path}")
        spread = _close_exec(execId='fs-1', orderId='fs-order', execType='FutureSpread',
                             createType='', side='Buy', execQty='0.01', closedSize='0',
                             seq='100')
        if path == 'push':
            _push(plugin, _exec_push(spread))
        else:
            plugin._backfill_one_execution(spread, plugin._market)
        _push(plugin, __test_helper_view(size=0.03, seq=100))
        # The spread trader closes exactly its own leg again.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.01', seq=101)))
        _push(plugin, __test_helper_view(size=0.02, seq=101))
        assert plugin.quarantines == [], path
        assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1, path
        assert plugin._run_owned_signed_size() == pytest.approx(0.02), path


def __test_cashflow_executions_only_record_their_seq__(tmp_path):
    for exec_type in ('Funding', 'Dividend'):
        plugin = _manual_plugin(tmp_path, f"cashflow-{exec_type}")
        _push(plugin, _exec_push(_close_exec(
            execId=f"cf-{exec_type}", execType=exec_type, createType='',
            execQty='0.02', closedSize='0.02', seq='100')))
        _push(plugin, __test_helper_view(size=0.02, seq=100))
        assert plugin.quarantines == [], exec_type
        assert not plugin._leg_ledgers[0].open, exec_type
        assert plugin._run_owned_signed_size() == pytest.approx(0.02), exec_type


def __test_foreign_execution_without_seq_is_admitted_when_the_backfill_brings_its_seq__(
        tmp_path):
    plugin = _manual_plugin(tmp_path, "late-seq")
    # The push copy carries no seq: it cannot be placed against the views.
    _push(plugin, _exec_push(_close_exec(seq='')))
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert plugin.quarantines == []
    # The REST copy of the same execution carries it.
    assert plugin._backfill_one_execution(_close_exec(), plugin._market) == []
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['exec_id'] == 'm-1'


# --- own fills racing a foreign close ---------------------------------------

def __test_helper_own_exit_row(plugin, coid: str, *, qty: float, side='sell') -> None:
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side=side, qty=qty, filled_qty=0.0,
        state='confirmed', intent_key=f"TP\x00Long-{coid}", from_entry='Long',
        exchange_order_id=f"venue-{coid}",
        extras={'kind': 'exit_leg', 'leg': 'tp', 'exit_id': 'TP'},
    )


def __test_helper_own_exec(coid: str, *, exec_id: str, qty: str, seq,
                           side='Sell') -> dict:
    return _close_exec(execId=exec_id, orderId=f"venue-{coid}", orderLinkId=coid,
                       side=side, execQty=qty, closedSize=qty,
                       createType='CreateByUser', seq=str(seq))


def __test_oneway_partial_tp_then_foreign_close_reports__(tmp_path):
    plugin = _manual_plugin(tmp_path, "partial-tp")
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.02)
    # The run's own TP limit fills half: the entry row is netted, the TP
    # row's own cursor must not be subtracted a second time.
    events = _push(plugin, _exec_push(
        __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60)))
    assert len(events) == 1
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)
    _push(plugin, __test_helper_view(size=0.01, seq=60))
    # Someone else closes half of what the run still holds.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.005', seq=70)))
    _push(plugin, __test_helper_view(size=0.005, seq=70))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['consumed'] == pytest.approx(0.005)


def __test_unbooked_own_reduce_never_causes_false_report__(tmp_path):
    # The leg holds the run's 0.02 and 0.01 of someone else's.
    plugin = _manual_plugin(tmp_path, "unbooked-own", physical=0.03)
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    own_tp = __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60)
    # The run's TP fills, but its execution is not delivered yet; the view
    # of the fill is.
    _push(plugin, __test_helper_view(size=0.02, seq=60))
    # Someone else closes exactly their own 0.01.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.01', seq=70)))
    _push(plugin, __test_helper_view(size=0.01, seq=70))
    assert plugin.quarantines == [], "an open interval is never judged"
    # The own fill arrives (a reconnect backfill): the interval is proven.
    events = plugin._backfill_one_execution(own_tp, plugin._market)
    assert len(events) == 1
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)


def __test_close_leg_in_flight_defers__(tmp_path):
    # Hedge: the run's own emulator close leg is in flight while someone
    # else closes their own 0.01 of the shared Buy leg.
    plugin = __test_helper_hedge_plugin(tmp_path, "close-leg", physical=0.03)
    link_id = wire_link_id('close-coid:1')
    plugin._record_identity(link_id, pine_id=None, from_entry='Long',
                            leg_type=LegType.CLOSE, qty=0.01)
    _push(plugin, __test_helper_view(idx=1, size=0.02, seq=60))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.01', seq=70)))
    _push(plugin, __test_helper_view(idx=1, size=0.01, seq=70))
    assert plugin.quarantines == []
    events = _push(plugin, _exec_push(_close_exec(
        execId='own-close', orderId='venue-close', orderLinkId=link_id,
        execQty='0.01', closedSize='0.01', createType='CreateByUser', seq='60')))
    assert len(events) == 1
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)


def __test_parked_close_gives_up_with_error_and_audit__(tmp_path, monkeypatch, caplog):
    clock = __test_helper_clock(monkeypatch)
    plugin = _manual_plugin(tmp_path, "give-up")
    # The leg loses the run's whole 0.02, but only 0.01 of it is explained.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.01', seq=100)))
    _push(plugin, __test_helper_view(size=0, seq=100))
    assert plugin.quarantines == []
    clock[0] += plugin.fill_report_grace_s + 1
    with caplog.at_level('ERROR', logger='pynecore_bybit.protection'):
        plugin._ledger_advance(plugin._market)
    assert plugin.quarantines == [], "unproven evidence never quarantines"
    assert len(__test_helper_audits(plugin, 'external_close_unproven')) == 1
    assert len(__test_helper_audits(plugin, 'own_share_clamped')) == 1
    assert any(record.levelname == 'ERROR' for record in caplog.records)
    assert plugin._run_owned_signed_size() == 0.0


def __test_hedge_inflated_share_foreign_only_close_is_not_reported__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_hedge_plugin(tmp_path, "hedge-inflated")
    # The leg goes flat with no execution the run can see (an own stop fire
    # the attribution refused, an ADL, a close the stream lost).
    _push(plugin, __test_helper_view(idx=1, size=0, seq=100))
    # Someone else then opens 0.01 on the leg and closes exactly that.
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=110)))
    _push(plugin, __test_helper_view(idx=1, size=0.01, seq=110))
    _push(plugin, _exec_push(__test_helper_foreign_close('f-2', qty='0.01', seq=111)))
    _push(plugin, __test_helper_view(idx=1, size=0, seq=111))
    assert plugin.quarantines == []
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin.quarantines == []
    # The unexplained flat books the venue truth: the run owns nothing.
    assert plugin._run_owned_signed_size() == 0.0
    assert len(__test_helper_audits(plugin, 'own_share_clamped')) == 1
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1


def __test_late_own_reduce_after_a_give_up_is_not_netted_twice__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = _manual_plugin(tmp_path, "late-own")
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    _push(plugin, __test_helper_view(size=0.01, seq=60))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)
    # The own TP fill that explains the gap arrives after all.
    events = plugin._backfill_one_execution(
        __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60),
        plugin._market)
    assert len(events) == 1
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)
    assert plugin.quarantines == []


def __test_give_up_never_charges_a_foreign_close_of_foreign_exposure__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode, idx, physical, foreign_qty, left in (
            (POSITION_MODE_HEDGE, 1, 0.03, '0.01', 0.0),
            (POSITION_MODE_ONE_WAY, 0, 0.03, '0.01', 0.0),
            # The leg holds enough foreign exposure that the give-up clamps
            # nothing; the foreign party closes part of its own 0.03.
            (POSITION_MODE_HEDGE, 1, 0.05, '0.02', 0.01),
    ):
        name = f"give-up-{mode}-{physical}"
        if mode == POSITION_MODE_HEDGE:
            plugin = __test_helper_hedge_plugin(tmp_path, name, physical=physical)
        else:
            plugin = _manual_plugin(tmp_path, name, physical=physical)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.02)
        # The run's own TP fills its whole 0.02; the execution stays missing
        # past the grace (a lost push and a lagging execution index), the
        # view of the fill does not.
        _push(plugin, __test_helper_view(idx=idx, size=physical - 0.02, seq=60))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin.quarantines == [], name
        # Someone else closes only exposure of their own.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty=foreign_qty, seq=70)))
        _push(plugin, __test_helper_view(idx=idx, size=left, seq=70))
        assert plugin.quarantines == [], name
        assert __test_helper_audits(plugin, 'external_close_unproven'), name
        # The own TP execution arrives at last: nothing is netted twice.
        events = plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.02', seq=60),
            plugin._market)
        assert len(events) == 1, name
        assert plugin._run_owned_signed_size() == 0.0, name
        assert plugin.quarantines == [], name


def __test_give_up_holds_the_whole_share_in_doubt__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    # The run holds 0.04 of a 0.05 leg; its own TP of 0.01 fills with the
    # execution missing past the grace.
    plugin = _InterventionFake(mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "doubt-whole")
    _seed_long(plugin, qty=0.04)
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_view(idx=1, size=0.05, seq=50))
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    _push(plugin, __test_helper_view(idx=1, size=0.04, seq=60))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    # The leg's fall does not bound what is missing (an own reduce a foreign
    # add offsets moves it by nothing): all of the share is in doubt.
    assert plugin._leg_ledgers[1].doubt == pytest.approx(0.04)
    assert __test_helper_audits(plugin, 'position_ledger_gap')[0]['doubt'] == \
        pytest.approx(0.04)
    # Someone else closes the whole leg before the TP execution arrives.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.04', seq=70)))
    _push(plugin, __test_helper_view(idx=1, size=0, seq=70))
    assert plugin.quarantines == [], "a close of the doubt is never reported"
    assert [item['exec_id'] for item in
            __test_helper_audits(plugin, 'external_close_unproven')] == ['f-1']
    assert plugin._run_owned_signed_size() == 0.0, "the venue truth is booked"
    # The TP execution arrives at last: nothing is netted twice.
    plugin._backfill_one_execution(
        __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60), plugin._market)
    assert plugin._run_owned_signed_size() == 0.0
    assert plugin.quarantines == []


def __test_completed_give_up_reports_a_close_of_the_run_share__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode, owned, physical, foreign_qty, consumed in (
            # The run alone holds the leg: the give-up clamps the share.
            (POSITION_MODE_ONE_WAY, 0.02, 0.02, '0.01', 0.01),
            (POSITION_MODE_HEDGE, 0.02, 0.02, '0.01', 0.01),
            # Someone else holds 0.01 of it: nothing is clamped.
            (POSITION_MODE_ONE_WAY, 0.04, 0.05, '0.04', 0.03),
            (POSITION_MODE_HEDGE, 0.04, 0.05, '0.04', 0.03),
    ):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        name = f"completed-{mode}-{physical}"
        plugin = __test_helper_ledger_plugin(tmp_path, name, mode, owned=owned,
                                             physical=physical)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        # The run's own TP of 0.01 fills, the execution stays missing past
        # the grace.
        _push(plugin, __test_helper_view(idx=idx, size=physical - 0.01, seq=60))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        # The share left after the clamp to the view is all in doubt.
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(
            min(owned, physical - 0.01)), name
        # The TP execution arrives: the interval is complete, the give-up
        # guessed nothing, and the doubt is released.
        events = plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60),
            plugin._market)
        assert len(events) == 1, name
        assert plugin._leg_ledgers[idx].doubt == 0.0, name
        assert plugin._leg_ledgers[idx].gaps == [], name
        assert plugin._run_owned_signed_size() == pytest.approx(owned - 0.01), name
        # Someone else closes what is left of the leg.
        _push(plugin, _exec_push(__test_helper_foreign_close(
            'f-1', qty=foreign_qty, seq=70)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=70))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name
        _, context, _ = plugin.quarantines[0]
        assert context['consumed'] == pytest.approx(consumed), name
        assert context['unproven'] == 0.0, name


def __test_start_clamp_holds_the_share_in_doubt__(tmp_path):
    # The ledger starts on a view newer than the journal: the run's own
    # 0.02 TP is not booked yet, someone else's 0.01 is all the leg holds.
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "start-clamp")
    _seed_long(plugin)
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.02)
    _push(plugin, __test_helper_view(size=0.01, seq=60))
    plugin._adoption_baselined = True
    plugin._ledger_advance(plugin._market)
    assert plugin._leg_ledgers[0].doubt == pytest.approx(0.01)
    # Someone else closes their own 0.01.
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.01', seq=70)))
    _push(plugin, __test_helper_view(size=0, seq=70))
    assert plugin.quarantines == []
    plugin._backfill_one_execution(
        __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.02', seq=60), plugin._market)
    assert plugin._run_owned_signed_size() == 0.0
    assert plugin.quarantines == []


def __test_give_up_credits_are_journalled__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_hedge_plugin(tmp_path, "credits-journal")
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    _push(plugin, __test_helper_view(idx=1, size=0.01, seq=60))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin._load_persisted_ledger_credits() == {1: [[60.0, pytest.approx(0.01), 1.0]]}
    plugin._backfill_one_execution(
        __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01', seq=60), plugin._market)
    assert plugin._load_persisted_ledger_credits() == {1: []}
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)


def __test_give_up_doubt_covers_an_own_reduce_a_foreign_add_offsets__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"offset-{mode}", mode, owned=0.02, physical=0.02)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.02)
        # The run's TP closes its 0.02 and someone else opens 0.01; both
        # executions stay missing past the grace, the view after them does
        # not: the leg fell by 0.01 only.
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=102))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(0.01), mode
        # The other party closes exactly its own 0.01.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-103', qty='0.01', seq=103)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=103))
        assert plugin.quarantines == [], mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'external_close_unproven')] == ['f-103'], mode
        # The lost executions arrive: nothing is netted twice.
        plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='o-101', qty='0.02', seq=101),
            plugin._market)
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-102', qty='0.01', seq=102), plugin._market)
        assert plugin.quarantines == [], mode
        assert plugin._run_owned_signed_size() == 0.0, mode


def __test_give_up_never_charges_a_known_foreign_close__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"known-foreign-{mode}", mode, owned=0.02, physical=0.02)
        # Someone else opens 0.02 (the execution is lost) and closes 0.01 of it.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-102', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.03, seq=102))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin.quarantines == [], mode
        # The leg still holds the run's 0.02: the journal keeps it.
        assert plugin._run_owned_signed_size() == pytest.approx(0.02), mode
        if mode == POSITION_MODE_HEDGE:
            scoped = plugin._scope_hedge_rows_to_run(
                [{'symbol': 'BTCUSDT', 'positionIdx': 1, 'side': 'Buy', 'size': '0.03'}])
            assert float(scoped[0]['size']) == pytest.approx(0.02)
        # The lost add arrives: the interval is complete, none of it was the run's.
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-101', qty='0.02', seq=101), plugin._market)
        assert plugin._leg_ledgers[idx].doubt == 0.0, mode
        assert 'f-102' in [item['exec_id'] for item in
                           __test_helper_audits(plugin, 'external_close_foreign_only')], mode
        # The other party now closes 0.02: the leg falls below the run's 0.02.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-110', qty='0.02', seq=110)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=110))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['exec_id'] == 'f-110', mode
        assert context['consumed'] == pytest.approx(0.01), mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_late_foreign_close_completing_a_give_up_releases_the_doubt__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        # The run holds 0.02, someone else 0.01.
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"late-foreign-{mode}", mode, owned=0.02, physical=0.03)
        # The other party closes its own 0.01; the execution stays missing
        # past the grace.
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=101))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(0.02), mode
        # It arrives: no own reduce is missing, the doubt is released.
        plugin._backfill_one_execution(
            __test_helper_foreign_close('f-101', qty='0.01', seq=101), plugin._market)
        assert plugin._leg_ledgers[idx].doubt == 0.0, mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'external_close_foreign_only')] == ['f-101'], mode
        assert __test_helper_audits(plugin, 'external_close_unproven') == [], mode
        # Someone now closes 0.01 of the leg the run alone holds.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-110', qty='0.01', seq=110)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=110))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['consumed'] == pytest.approx(0.01), mode


def __test_completed_give_up_keeps_the_doubt_when_the_share_was_taken__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"taken-{mode}", mode, owned=0.02, physical=0.02)
        # A liquidation takes the run's 0.02 (its execution is known) and
        # someone else opens 0.02 (the execution is lost): the leg looks
        # unchanged.
        _push(plugin, _exec_push(__test_helper_foreign_close(
            'l-101', qty='0.02', seq=101, createType='CreateByLiq')))
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=102))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(0.02), mode
        # The add arrives: complete now, the interval shows the liquidation
        # took the run's share. The doubt stays.
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-102', qty='0.02', seq=102), plugin._market)
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(0.02), mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'own_share_clamped')] == ['l-101'], mode
        # The other party closes its own 0.02: never reported, the venue
        # truth is booked.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-110', qty='0.02', seq=110)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=110))
        assert plugin.quarantines == [], mode
        assert plugin._run_owned_signed_size() == 0.0, mode


def __test_late_folded_reversal_after_a_give_up_is_not_netted_twice__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for name, leg, journal in (
            # The reversal closed the run's 0.02 and opened 0.02 short.
            ('reversal', 0.02, -0.02),
            # The run's 0.02 was gone before it: all of it opened.
            ('taken-first', 0.04, -0.04),
    ):
        plugin = _manual_plugin(tmp_path, f"late-reversal-{name}")
        __test_helper_reversal_row(plugin, 'rev-1', side='sell', qty=0.04)
        # The reversal's execution stays missing past the grace, the view
        # after it does not.
        _push(plugin, __test_helper_view(size=leg, side='Sell', seq=101))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin._run_owned_signed_size() == 0.0, name
        events = plugin._backfill_one_execution(__test_helper_reversal_exec(
            'rev-1', exec_id='o-101', side='Sell', qty='0.04', closed='0.02', seq=101),
            plugin._market)
        assert len(events) == 1, name
        assert plugin._run_owned_signed_size() == pytest.approx(journal), name
        assert plugin._leg_ledgers[0].share == pytest.approx(journal), name
        assert plugin.quarantines == [], name


def __test_view_stamped_by_a_missing_execution_keeps_the_interval_open__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"stamped-{mode}", mode, owned=0.02, physical=0.02)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        # The run's TP closes 0.01 and someone else opens 0.01; both
        # executions are lost, the view after them is not: the leg looks
        # unchanged, but its seq names an execution not known.
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=102))
        # The other party closes exactly its own 0.01.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-103', qty='0.01', seq=103)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=103))
        assert plugin.quarantines == [], mode
        plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='o-101', qty='0.01', seq=101),
            plugin._market)
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-102', qty='0.01', seq=102), plugin._market)
        assert plugin.quarantines == [], mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'external_close_foreign_only')] == ['f-103'], mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_interval_across_a_stream_outage_waits_for_the_backfill__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        outage_frames = (
            # Back on the stream, the other party closes exactly its own 0.01.
            _exec_push(__test_helper_foreign_close('f-104', qty='0.01', seq=104)),
            __test_helper_view(idx=idx, size=0.01, seq=104),
        )
        # The private stream reopens after an outage in which the run's TP
        # closed 0.01 and someone else opened 0.01: both executions and the
        # views of them are lost. The frames after it alone look like a
        # close of the run's share.
        control = __test_helper_ledger_plugin(
            tmp_path, f"outage-control-{mode}", mode, owned=0.02, physical=0.02)
        __test_helper_own_exit_row(control, 'tp-1', qty=0.01)
        for frame in outage_frames:
            _push(control, frame)
        assert _kinds(control) == [QuarantineKind.EXTERNAL_INTERVENTION], mode

        plugin = __test_helper_ledger_plugin(
            tmp_path, f"outage-{mode}", mode, owned=0.02, physical=0.02)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        now_ms = int(time.time() * 1000)
        plugin._ledger_stream_opened_ms = now_ms
        plugin._deriv_exec_watermark = now_ms - 90_000
        for frame in outage_frames:
            _push(plugin, frame)
        assert plugin.quarantines == [], mode
        # A backfill pass right after the reopen finds nothing yet (the
        # venue indexes executions late): nothing is proven.
        plugin.responses = [{'list': [], 'nextPageCursor': ''}]
        asyncio.run(plugin._run_deriv_fill_backfill(plugin._market))
        assert plugin._ledger_stream_opened_ms == now_ms, mode
        plugin._ledger_advance(plugin._market)
        assert plugin.quarantines == [], mode
        # The pass a read overlap after the reopen brings the lost executions.
        plugin._ledger_stream_opened_ms = now_ms - EXECUTION_CURSOR_OVERLAP_MS
        plugin._deriv_exec_watermark = now_ms - 90_000
        plugin.responses = [{'list': [
            __test_helper_own_exec('tp-1', exec_id='o-102', qty='0.01', seq=102),
            __test_helper_foreign_open('f-103', qty='0.01', seq=103),
        ], 'nextPageCursor': ''}]
        events = asyncio.run(plugin._run_deriv_fill_backfill(plugin._market))
        assert [event.fill_id for event in events] == ['o-102'], mode
        assert plugin._ledger_stream_opened_ms is None, mode
        plugin._ledger_advance(plugin._market)
        assert plugin.quarantines == [], mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'external_close_foreign_only')] == ['f-104'], mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_stream_open_arms_the_ledger_outage_hold__(tmp_path, monkeypatch):
    plugin = __test_helper_ledger_plugin(
        tmp_path, "arm-hold", POSITION_MODE_ONE_WAY, owned=0.02, physical=0.02)
    holds: list = []

    async def _drive() -> None:
        opened = asyncio.Event()
        queue: asyncio.Queue = asyncio.Queue()

        async def fake_open(_market) -> None:
            plugin._private_ws = type('_StubWS', (), {'is_open': True})()
            plugin._private_events = queue

        async def fake_backfill(_market) -> list:
            holds.append(plugin._ledger_stream_opened_ms)
            opened.set()
            return []

        monkeypatch.setattr(plugin, '_open_private_ws', fake_open)
        monkeypatch.setattr(plugin, '_run_deriv_fill_backfill', fake_backfill)
        stream = cast(AsyncGenerator, plugin.watch_orders())
        task = asyncio.ensure_future(stream.__anext__())
        await opened.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await stream.aclose()

    asyncio.run(_drive())
    assert len(holds) == 1 and holds[0] is not None


def __test_reconcile_keeps_its_cadence_while_the_private_stream_is_down__(
        tmp_path, monkeypatch):
    """A fill of a private-stream outage reaches the engine before the stream is back.

    The reconnect used to retry inside its own backoff loop, starving the
    reconcile pass and its execution backfill for the whole outage
    (measured live: bybit-inverse cycle 250, a reversal close filled while
    the stream was down for 134s; its fill arrived only at the reconnect,
    after two rejected close re-dispatches and a false "exchange shows
    flat ... no fill explains it" ERROR).
    """
    import pynecore_bybit.events as events_module
    plugin = __test_helper_ledger_plugin(
        tmp_path, "outage-cadence", POSITION_MODE_ONE_WAY, owned=0.02, physical=0.02)
    monkeypatch.setattr(events_module, 'RECONCILE_CADENCE_S', 0.01)
    monkeypatch.setattr(events_module, 'PRIVATE_WS_BACKOFF_S', (3600.0,))
    attempts: list[int] = []
    holds: list = []

    async def _drive() -> None:
        reconciled = asyncio.Event()

        async def fake_open(_market) -> BybitConnectionError:
            attempts.append(1)
            return BybitConnectionError("private stream down")

        async def fake_reconcile(_market) -> list:
            holds.append(plugin._ledger_stream_opened_ms)
            reconciled.set()
            return []

        monkeypatch.setattr(plugin, '_open_private_ws', fake_open)
        monkeypatch.setattr(plugin, '_run_deriv_reconcile', fake_reconcile)
        stream = cast(AsyncGenerator, plugin.watch_orders())
        task = asyncio.ensure_future(stream.__anext__())
        await reconciled.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await stream.aclose()

    asyncio.run(_drive())
    assert attempts == [1]
    assert holds and holds[0] is not None


# --- interval completeness: executions that cancel each other out -----------

def __test_helper_ledger_plugin(tmp_path, name: str, mode: str, *, owned: float,
                                physical: float) -> '_InterventionFake':
    """A run owning ``owned`` of a long leg holding ``physical``.

    The leg is the one-way position or the hedge Buy leg (``positionIdx`` 1).
    """
    plugin = _InterventionFake(mode=mode)
    _attach_store(plugin, tmp_path, name)
    _seed_long(plugin, qty=owned)
    plugin._adoption_baselined = True
    if mode == POSITION_MODE_HEDGE:
        _push(plugin, __test_helper_view(idx=1, size=physical, seq=50))
        _push(plugin, __test_helper_view(idx=2, size=0, seq=50))
    else:
        _push(plugin, __test_helper_view(size=physical, seq=50))
    return plugin


def __test_helper_own_add_row(plugin, coid: str, *, qty: float) -> None:
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side='buy', qty=qty, filled_qty=0.0,
        state='confirmed', intent_key=f"Long-{coid}", pine_entry_id='Long',
        exchange_order_id=f"venue-{coid}", extras={'kind': ENTRY_KIND_POSITION},
    )


def __test_helper_own_add_exec(coid: str, *, exec_id: str, qty: str, seq) -> dict:
    return _close_exec(execId=exec_id, orderId=f"venue-{coid}", orderLinkId=coid,
                       side='Buy', execQty=qty, closedSize='0',
                       createType='CreateByUser', seq=str(seq))


def __test_zero_sum_rest_of_a_batch_never_closes_the_interval_early__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        for path in ('reconcile', 'push'):
            idx = 1 if mode == POSITION_MODE_HEDGE else 0
            plugin = __test_helper_ledger_plugin(
                tmp_path, f"zero-sum-{mode}-{path}", mode, owned=0.01, physical=0.01)
            __test_helper_own_exit_row(plugin, 'tp-1', qty=0.005)
            __test_helper_own_add_row(plugin, 'entry-2', qty=0.01)
            # The run's TP fills half, the run adds 0.01, then someone else
            # sells 0.01: the leg ends at 0.005 while the run held 0.015.
            batch = (
                __test_helper_own_exec('tp-1', exec_id='o-101', qty='0.005', seq=101),
                __test_helper_own_add_exec('entry-2', exec_id='o-102', qty='0.01', seq=102),
                __test_helper_foreign_close('f-103', qty='0.01', seq=103),
            )
            view = __test_helper_view(idx=idx, size=0.005, seq=103)
            if path == 'reconcile':
                # The cadence reconcile reads the position first, then
                # books the backfilled executions one by one.
                plugin._ingest_position_sizes(view['data'])
                for entry in batch:
                    plugin._backfill_one_execution(entry, plugin._market)
                plugin._ledger_advance(plugin._market)
            else:
                # The position push overtakes the execution pushes.
                _push(plugin, view)
                for entry in batch:
                    _push(plugin, _exec_push(entry))
            name = f"{mode}-{path}"
            assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name
            _, context, _ = plugin.quarantines[0]
            assert context['exec_id'] == 'f-103', name
            assert context['owned'] == pytest.approx(0.015), name
            assert context['consumed'] == pytest.approx(0.01), name
            assert plugin._run_owned_signed_size() == pytest.approx(0.005), name


def __test_lost_foreign_add_never_charges_its_own_close_to_the_run__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"lost-add-{mode}", mode, owned=0.01, physical=0.01)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        # Someone else adds 0.01 (the push is lost in a reconnect gap) and
        # closes exactly that; the run's own TP then closes the run's 0.01.
        # The close and the view of the TP fill arrive before the TP's own
        # execution.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-102', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=103))
        assert plugin.quarantines == [], mode
        _push(plugin, _exec_push(
            __test_helper_own_exec('tp-1', exec_id='o-103', qty='0.01', seq=103)))
        assert plugin.quarantines == [], mode
        # The reconnect backfill delivers the lost add: the interval is proven.
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-101', qty='0.01', seq=101), plugin._market)
        assert plugin.quarantines == [], mode
        assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1, mode
        assert plugin._run_owned_signed_size() == 0.0, mode


def __test_every_view_of_the_interval_must_match_its_executions__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"prefix-{mode}", mode, owned=0.01, physical=0.01)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        __test_helper_own_add_row(plugin, 'entry-2', qty=0.01)
        # Someone else adds 0.01 and closes it, the run's TP closes the run's
        # 0.01, the run re-enters 0.01. The add and the TP execution are lost;
        # every view arrives. The known executions sum up to the last view,
        # the views in between show what is missing.
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=101))
        _push(plugin, _exec_push(__test_helper_foreign_close('f-102', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=102))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=103))
        _push(plugin, _exec_push(
            __test_helper_own_add_exec('entry-2', exec_id='o-104', qty='0.01', seq=104)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=104))
        assert plugin.quarantines == [], mode
        # The backfill delivers the lost executions: nothing was the run's.
        plugin._backfill_one_execution(
            __test_helper_foreign_open('f-101', qty='0.01', seq=101), plugin._market)
        plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='o-103', qty='0.01', seq=103),
            plugin._market)
        assert plugin.quarantines == [], mode
        assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1, mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_one_push_is_judged_whole__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        # The run owns 0.01 of a 0.02 leg.
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"whole-push-{mode}", mode, owned=0.01, physical=0.02)
        __test_helper_own_add_row(plugin, 'entry-2', qty=0.01)
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=103))
        # One match: someone else sells 0.02 in two fills, one of them
        # against the run's own buy of 0.01. Concluded together, the
        # executions share one seq; the first alone explains the view.
        _push(plugin, _exec_push(
            __test_helper_foreign_close('f-103a', qty='0.01', seq=103),
            __test_helper_own_add_exec('entry-2', exec_id='o-103', qty='0.01', seq=103),
            __test_helper_foreign_close('f-103b', qty='0.01', seq=103),
        ))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['exec_id'] == 'f-103b', mode
        assert context['consumed'] == pytest.approx(0.01), mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_late_own_add_then_reduce_keep_the_share__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_ledger_plugin(
            tmp_path, f"late-pair-{mode}", mode, owned=0.02, physical=0.02)
        __test_helper_own_add_row(plugin, 'entry-2', qty=0.01)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
        # The run adds 0.01 and its TP closes 0.01; both executions stay
        # missing past the grace, the view after them does not.
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=61))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        assert plugin.quarantines == [], mode
        # Both arrive late, in venue order: the run's share is what it was.
        plugin._backfill_one_execution(
            __test_helper_own_add_exec('entry-2', exec_id='o-60', qty='0.01', seq=60),
            plugin._market)
        plugin._backfill_one_execution(
            __test_helper_own_exec('tp-1', exec_id='o-61', qty='0.01', seq=61),
            plugin._market)
        assert plugin._run_owned_signed_size() == pytest.approx(0.02), mode
        # Someone else then closes 0.01 of the leg the run alone holds.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-70', qty='0.01', seq=70)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=70))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['consumed'] == pytest.approx(0.01), mode
        assert plugin._run_owned_signed_size() == pytest.approx(0.01), mode


def __test_helper_unowned_leg_plugin(tmp_path, name: str, mode: str, *,
                                     physical=0.0) -> '_InterventionFake':
    """A run owning nothing of a long leg holding ``physical`` (``seq`` 50).

    The run has an entry row of 0.01 that has not filled yet.
    """
    plugin = _InterventionFake(mode=mode)
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    idx = 1 if mode == POSITION_MODE_HEDGE else 0
    _push(plugin, __test_helper_view(idx=idx, size=physical, seq=50))
    if mode == POSITION_MODE_HEDGE:
        _push(plugin, __test_helper_view(idx=2, size=0, seq=50))
    __test_helper_own_add_row(plugin, 'entry-2', qty=0.01)
    return plugin


def __test_late_own_add_taken_after_it_is_never_charged_to_a_foreign_close__(
        tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_unowned_leg_plugin(tmp_path, f"late-add-adl-{mode}", mode)
        # The run's entry fills 0.01; its execution stays missing, the view
        # of it does not. An ADL then takes the whole leg.
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=101))
        _push(plugin, _exec_push(__test_helper_foreign_close(
            'a-102', qty='0.01', seq=102, execType='AdlTrade')))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=102))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        # Someone else opens 0.01 and closes half of it.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-103', qty='0.01', seq=103)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=103))
        _push(plugin, _exec_push(__test_helper_foreign_close('f-104', qty='0.005', seq=104)))
        _push(plugin, __test_helper_view(idx=idx, size=0.005, seq=104))
        # The run's own fill is recovered at last: the ADL took what it built.
        events = plugin._backfill_one_execution(
            __test_helper_own_add_exec('entry-2', exec_id='o-101', qty='0.01', seq=101),
            plugin._market)
        assert len(events) == 1, mode
        assert plugin._leg_ledgers[idx].share == 0.0, mode
        assert plugin._run_owned_signed_size() == 0.0, "the venue truth is booked"
        assert [item['cause'] for item in __test_helper_audits(plugin, 'own_share_clamped')
                if 'cause' in item] == ['late_own_fill'], mode
        # The other party closes the rest of its own exposure.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-105', qty='0.005', seq=105)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=105))
        assert plugin.quarantines == [], mode
        assert 'f-105' in [item['exec_id'] for item in
                           __test_helper_audits(plugin, 'external_close_foreign_only')], mode


def __test_late_own_add_on_its_own_view_keeps_the_share__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        plugin = __test_helper_unowned_leg_plugin(tmp_path, f"late-add-kept-{mode}", mode)
        # The run's entry fills 0.01 with the execution missing past the
        # grace; the view stamped by it is known.
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=101))
        clock[0] += plugin.fill_report_grace_s + 1
        plugin._ledger_advance(plugin._market)
        _push(plugin, _exec_push(__test_helper_foreign_open('f-102', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=102))
        plugin._backfill_one_execution(
            __test_helper_own_add_exec('entry-2', exec_id='o-101', qty='0.01', seq=101),
            plugin._market)
        # Nothing after it took any of the leg: the share is proven.
        assert plugin._leg_ledgers[idx].share == pytest.approx(0.01), mode
        assert plugin._leg_ledgers[idx].doubt == 0.0, mode
        # The other party closes its own 0.01: never reported.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-103', qty='0.01', seq=103)))
        _push(plugin, __test_helper_view(idx=idx, size=0.01, seq=103))
        assert plugin.quarantines == [], mode
        # A close below the run's 0.01 is an intervention.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-104', qty='0.005', seq=104)))
        _push(plugin, __test_helper_view(idx=idx, size=0.005, seq=104))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['exec_id'] == 'f-104', mode
        assert context['consumed'] == pytest.approx(0.005), mode


def __test_late_own_add_the_views_do_not_prove_is_held_in_doubt__(tmp_path):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        idx = 1 if mode == POSITION_MODE_HEDGE else 0
        # Someone else holds 0.01 of the leg.
        plugin = __test_helper_unowned_leg_plugin(
            tmp_path, f"late-add-masked-{mode}", mode, physical=0.01)
        # Lost: the other party closes its 0.01, the run's entry fills 0.01,
        # an ADL takes it, the other party opens 0.01 again. Known: one more
        # open of 0.01. The lost executions cancel out, so the interval looks
        # complete without them.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-102', qty='0.01', seq=102)))
        _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=102))
        assert plugin._leg_ledgers[idx].cursor_seq == 102, mode
        # The run's own fill is recovered: nothing proves the steps after it
        # left it on the leg.
        plugin._backfill_one_execution(
            __test_helper_own_add_exec('entry-2', exec_id='o-100', qty='0.01', seq=100),
            plugin._market)
        assert plugin._leg_ledgers[idx].share == pytest.approx(0.01), mode
        assert plugin._leg_ledgers[idx].doubt == pytest.approx(0.01), mode
        # The other party closes its own 0.02: the share it takes is in
        # doubt, so nothing is reported, and the venue truth is booked.
        _push(plugin, _exec_push(__test_helper_foreign_close('f-103', qty='0.02', seq=103)))
        _push(plugin, __test_helper_view(idx=idx, size=0, seq=103))
        assert plugin.quarantines == [], mode
        assert [item['exec_id'] for item in
                __test_helper_audits(plugin, 'external_close_unproven')] == ['f-103'], mode
        assert plugin._run_owned_signed_size() == 0.0, mode


# --- venue position stops over a merged leg --------------------------------

def __test_partial_stop_fire_over_the_run_share_reports__(tmp_path):
    for stop_type in ('PartialStopLoss', 'PartialTakeProfit'):
        plugin = __test_helper_hedge_plugin(tmp_path, f"partial-{stop_type}")
        create_bracket_ownership_row(
            plugin.store_ctx, coid="bo:L-X\x00Long:1", symbol='BTCUSDT.P', side='sell',
            qty=0.02, intent_key="L-X\x00Long", pine_entry_id='L-X', from_entry='Long',
            leg_id='1', attach_coid='attach-l', tp_price=None, sl_price=29000.0,
            trail_price=None, trail_offset=None,
        )
        # The bot writes every trading stop with ``tpslMode=Full``: a Partial
        # stop that fires is one someone else set on the run's leg.
        events = _push(plugin, _exec_push(_close_exec(
            stopOrderType=stop_type, createType=f"CreateBy{stop_type}")))
        assert events == [], "a Partial stop is not one the bot places"
        _push(plugin, __test_helper_view(idx=1, size=0, seq=100))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], stop_type
        _, context, _ = plugin.quarantines[0]
        assert context['stop_order_type'] == stop_type
        assert context['consumed'] == pytest.approx(0.02)
        assert plugin._run_owned_signed_size() == 0.0


def __test_partial_stop_fire_over_foreign_exposure_only_is_not_reported__(tmp_path):
    plugin = __test_helper_hedge_plugin(tmp_path, "partial-foreign", physical=0.03)
    # A Partial SL someone else set closes their own 0.01 of the shared leg.
    _push(plugin, _exec_push(__test_helper_foreign_close(
        'p-1', qty='0.01', seq=100, stopOrderType='PartialStopLoss',
        createType='CreateByPartialStopLoss')))
    _push(plugin, __test_helper_view(idx=1, size=0.02, seq=100))
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == pytest.approx(0.02)


def __test_helper_short_leg_plugin(tmp_path, name: str, entries) -> '_InterventionFake':
    """A hedge run short on the Sell leg, one ownership row per entry."""
    plugin = _InterventionFake(mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    total = 0.0
    for entry_id, qty in entries:
        _seed_long(plugin, qty=qty, side='sell', coid=f"entry-{entry_id}")
        create_bracket_ownership_row(
            plugin.store_ctx, coid=f"bo:{entry_id}-X\x00{entry_id}:2",
            symbol='BTCUSDT.P', side='buy', qty=qty,
            intent_key=f"{entry_id}-X\x00{entry_id}", pine_entry_id=f"{entry_id}-X",
            from_entry=entry_id, leg_id='2', attach_coid=f"attach-{entry_id}",
            tp_price=None, sl_price=31000.0, trail_price=None, trail_offset=None,
        )
        total += qty
    _push(plugin, __test_helper_view(idx=2, size=total, seq=50))
    return plugin


def __test_venue_stop_fire_over_merged_leg_claims_only_own_share__(tmp_path):
    for name, entries in (('single', (('S', 0.02),)),
                          ('pyramid', (('S1', 0.01), ('S2', 0.01)))):
        plugin = __test_helper_short_leg_plugin(tmp_path, f"merged-{name}", entries)
        # Someone else adds 0.01 to the run's short leg.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-1', side='Sell', qty='0.01', seq=101)))
        _push(plugin, __test_helper_view(idx=2, size=0.03, seq=101))
        # The run's Full stop fires and closes the whole merged leg.
        events = _push(plugin, _exec_push(_close_exec(
            execId='vs-1', orderId='venue-stop', side='Buy', execQty='0.03',
            closedSize='0.03', stopOrderType='StopLoss',
            createType='CreateByStopLoss', seq='102')))
        assert [event.leg_type for event in events] == \
            [LegType.STOP_LOSS] * len(entries), name
        assert sum(event.fill_qty for event in events) == pytest.approx(0.02), name
        assert len(__test_helper_audits(plugin, 'venue_bracket_fill_foreign_excess')) == 1, name
        _push(plugin, __test_helper_view(idx=2, size=0, seq=102))
        assert plugin.quarantines == [], name
        assert plugin._run_owned_signed_size() == 0.0, name


def __test_oneway_flat_view_before_own_close_never_reports_foreign_close__(tmp_path):
    for order in ('views-first', 'reconcile'):
        # One-way leg 0.03: the run's 0.02 and someone else's 0.01.
        plugin = _manual_plugin(tmp_path, f"sweep-{order}", physical=0.03)
        __test_helper_own_exit_row(plugin, 'tp-1', qty=0.02)
        own_tp = __test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.02', seq=60)
        foreign = __test_helper_foreign_close('f-1', qty='0.01', seq=70)
        if order == 'views-first':
            # The position pushes arrive before the execution pushes.
            _push(plugin, __test_helper_view(size=0.01, seq=60))
            _push(plugin, __test_helper_view(size=0, seq=70))
            assert plugin.store_ctx.get_order('entry-1').closed_ts_ms is None, order
            _push(plugin, _exec_push(own_tp))
            _push(plugin, _exec_push(foreign))
        else:
            # A stream outage: the reconcile reads the flat position, sweeps,
            # then backfills the executions.
            rows = __test_helper_view(size=0, seq=70)['data']
            plugin._ingest_position_sizes(rows)
            plugin._close_entry_rows_when_flat(plugin._market)
            assert plugin.store_ctx.get_order('entry-1').closed_ts_ms is None, order
            plugin._backfill_one_execution(own_tp, plugin._market)
            plugin._backfill_one_execution(foreign, plugin._market)
            plugin._ledger_advance(plugin._market)
            plugin._close_entry_rows_when_flat(plugin._market)
        assert plugin.quarantines == [], order
        assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1, order
        assert plugin._run_owned_signed_size() == 0.0, order


def __test_oneway_flat_view_before_own_stop_fire_claims_only_own_share__(tmp_path):
    for order in ('exec-first', 'view-first'):
        plugin = _manual_plugin(tmp_path, f"oneway-stop-{order}")
        create_bracket_ownership_row(
            plugin.store_ctx, coid="bo:L-X\x00Long:0", symbol='BTCUSDT.P', side='sell',
            qty=0.02, intent_key="L-X\x00Long", pine_entry_id='L-X', from_entry='Long',
            leg_id='0', attach_coid='attach-l', tp_price=None, sl_price=29000.0,
            trail_price=None, trail_offset=None,
        )
        # Someone else adds 0.01 to the run's one-way long.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
        _push(plugin, __test_helper_view(size=0.03, seq=101))
        # The run's Full stop closes the whole merged position.
        stop = _close_exec(execId='vs-1', orderId='venue-stop', side='Sell', execQty='0.03',
                           closedSize='0.03', stopOrderType='StopLoss',
                           createType='CreateByStopLoss', seq='102')
        if order == 'view-first':
            _push(plugin, __test_helper_view(size=0, seq=102))
            events = _push(plugin, _exec_push(stop))
        else:
            events = _push(plugin, _exec_push(stop))
            _push(plugin, __test_helper_view(size=0, seq=102))
        assert [event.fill_qty for event in events] == [pytest.approx(0.02)], order
        assert len(__test_helper_audits(plugin, 'venue_bracket_fill_foreign_excess')) == 1, order
        assert plugin.quarantines == [], order
        assert plugin._run_owned_signed_size() == 0.0, order


def __test_stop_fire_delayed_past_a_give_up_claims_only_own_share__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = _manual_plugin(tmp_path, "oneway-stop-gave-up")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:L-X\x00Long:0", symbol='BTCUSDT.P', side='sell',
        qty=0.02, intent_key="L-X\x00Long", pine_entry_id='L-X', from_entry='Long',
        leg_id='0', attach_coid='attach-l', tp_price=None, sl_price=29000.0,
        trail_price=None, trail_offset=None,
    )
    # Someone else adds 0.01 to the run's one-way long.
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    # The flat view of the run's stop fire arrives; its execution stays
    # missing past the grace, so the ledger gives up and clamps the share.
    _push(plugin, __test_helper_view(size=0, seq=102))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin._run_owned_signed_size() == 0.0
    # The stop's execution arrives after all: only the run's part is its own.
    events = _push(plugin, _exec_push(_close_exec(
        execId='vs-1', orderId='venue-stop', side='Sell', execQty='0.03',
        closedSize='0.03', stopOrderType='StopLoss', createType='CreateByStopLoss',
        seq='102')))
    assert [event.fill_qty for event in events] == [pytest.approx(0.02)]
    assert len(__test_helper_audits(plugin, 'venue_bracket_fill_foreign_excess')) == 1
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == 0.0


def __test_stop_fire_delayed_past_a_partial_give_up_claims_own_share__(
        tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = _manual_plugin(tmp_path, "oneway-stop-partial-give-up")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:L-X\x00Long:0", symbol='BTCUSDT.P', side='sell',
        qty=0.02, intent_key="L-X\x00Long", pine_entry_id='L-X', from_entry='Long',
        leg_id='0', attach_coid='attach-l', tp_price=None, sl_price=29000.0,
        trail_price=None, trail_offset=None,
    )
    # Someone else adds 0.01 to the run's one-way long.
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    # The view of the stop's first execution arrives; the execution stays
    # missing past the grace, so the ledger gives up and clamps the share
    # only in part.
    _push(plugin, __test_helper_view(size=0.005, seq=102))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin._run_owned_signed_size() == pytest.approx(0.005)
    # The stop's executions arrive after all: the run's whole share is its own.
    events = _push(plugin, _exec_push(
        __test_helper_stop_exec('vs-1', side='Sell', qty='0.025', seq=102)))
    events += _push(plugin, _exec_push(
        __test_helper_stop_exec('vs-2', side='Sell', qty='0.005', seq=103)))
    _push(plugin, __test_helper_view(size=0, seq=103))
    assert sum(event.fill_qty for event in events) == pytest.approx(0.02)
    assert all(event.leg_type is LegType.STOP_LOSS for event in events)
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == 0.0
    assert all(not ledger.credits for ledger in plugin._leg_ledgers.values())


def __test_helper_stop_exec(exec_id: str, *, side: str, qty: str, seq: int,
                            stop_type='StopLoss') -> dict:
    """One execution of a fired ``Full`` position stop (order ``venue-stop``)."""
    return _close_exec(execId=exec_id, orderId='venue-stop', side=side, execQty=qty,
                       closedSize=qty, stopOrderType=stop_type,
                       createType=f"CreateBy{stop_type}", seq=str(seq))


def __test_stop_fire_split_into_executions_claims_only_own_share__(tmp_path):
    cases = (
        ('single-0.02+0.01', (('S', 0.02),), '0.01', ('0.02', '0.01')),
        ('single-0.025+0.005', (('S', 0.02),), '0.01', ('0.025', '0.005')),
        ('single-0.01+0.02', (('S', 0.02),), '0.01', ('0.01', '0.02')),
        ('pyramid-0.02+0.03', (('S1', 0.01), ('S2', 0.01)), '0.03', ('0.02', '0.03')),
    )
    for name, entries, foreign, parts in cases:
        for delivery in ('one-push', 'restart'):
            case = f"{name}-{delivery}"
            plugin = __test_helper_short_leg_plugin(tmp_path, f"split-{case}", entries)
            # Someone else adds to the run's short leg.
            _push(plugin, _exec_push(
                __test_helper_foreign_open('f-1', side='Sell', qty=foreign, seq=101)))
            total = 0.02 + float(foreign)
            _push(plugin, __test_helper_view(idx=2, size=total, seq=101))
            # The run's Full stop closes the merged leg in several executions.
            stops = [__test_helper_stop_exec(f"vs-{index}", side='Buy', qty=qty, seq=102)
                     for index, qty in enumerate(parts, start=1)]
            if delivery == 'one-push':
                events = _push(plugin, _exec_push(*stops))
                current = plugin
            else:
                # The process restarts between two executions of the stop;
                # the backfill delivers the rest.
                events = _push(plugin, _exec_push(stops[0]))
                current = _restarted(plugin, mode=POSITION_MODE_HEDGE)
                current._adoption_baselined = True
                for stop in stops[1:]:
                    events.extend(current._backfill_one_execution(stop, current._market))
            assert sum(event.fill_qty for event in events) == pytest.approx(0.02), case
            assert all(event.leg_type is LegType.STOP_LOSS for event in events), case
            assert current._run_owned_signed_size() == 0.0, case
            _push(current, __test_helper_view(idx=2, size=0, seq=102))
            assert current.quarantines == [], case
            assert plugin.quarantines == [], case


def __test_oneway_trailing_stop_split_into_executions_claims_only_own_share__(tmp_path):
    plugin = _manual_plugin(tmp_path, "oneway-trail-split")
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:L-X\x00Long:0", symbol='BTCUSDT.P', side='sell',
        qty=0.02, intent_key="L-X\x00Long", pine_entry_id='L-X', from_entry='Long',
        leg_id='0', attach_coid='attach-l', tp_price=None, sl_price=None,
        trail_price=31000.0, trail_offset=50.0,
    )
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    events = _push(plugin, _exec_push(
        __test_helper_stop_exec('vs-1', side='Sell', qty='0.02', seq=102,
                                stop_type='TrailingStop'),
        __test_helper_stop_exec('vs-2', side='Sell', qty='0.01', seq=102,
                                stop_type='TrailingStop'),
    ))
    assert sum(event.fill_qty for event in events) == pytest.approx(0.02)
    assert len(__test_helper_audits(plugin, 'venue_bracket_fill_foreign_excess')) == 1
    _push(plugin, __test_helper_view(size=0, seq=102))
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == 0.0


def __test_pyramid_stop_fire_books_the_run_share_no_exit_row_covers__(tmp_path):
    cases = (
        # name, foreign add, stop executions, foreign-excess audits
        ('own-only', None, ('0.03',), 0),
        ('merged', '0.01', ('0.04',), 1),
        ('split', '0.02', ('0.025', '0.025'), 1),
    )
    for name, foreign, parts, excess_audits in cases:
        plugin = _InterventionFake(mode=POSITION_MODE_HEDGE)
        _attach_store(plugin, tmp_path, f"uncovered-{name}")
        plugin._adoption_baselined = True
        # Three pyramid entries; only the first two have an exit (and an
        # ownership row) on the shared Full stop.
        for entry_id in ('L1', 'L2', 'L3'):
            _seed_long(plugin, qty=0.01, coid=f"entry-{entry_id}")
        for entry_id in ('L1', 'L2'):
            create_bracket_ownership_row(
                plugin.store_ctx, coid=f"bo:{entry_id}-X\x00{entry_id}:1",
                symbol='BTCUSDT.P', side='sell', qty=0.01,
                intent_key=f"{entry_id}-X\x00{entry_id}", pine_entry_id=f"{entry_id}-X",
                from_entry=entry_id, leg_id='1', attach_coid=f"attach-{entry_id}",
                tp_price=None, sl_price=29000.0, trail_price=None, trail_offset=None,
            )
        _push(plugin, __test_helper_view(idx=1, size=0.03, seq=50))
        _push(plugin, __test_helper_view(idx=2, size=0, seq=50))
        if foreign is not None:
            _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty=foreign, seq=101)))
            _push(plugin, __test_helper_view(idx=1, size=0.03 + float(foreign), seq=101))
        # The Full stop closes the whole leg, the uncovered L3 included.
        events = _push(plugin, _exec_push(*(
            __test_helper_stop_exec(f"vs-{index}", side='Sell', qty=qty, seq=102)
            for index, qty in enumerate(parts, start=1))))
        _push(plugin, __test_helper_view(idx=1, size=0, seq=102))
        assert sum(event.fill_qty for event in events) == pytest.approx(0.03), name
        assert all(event.leg_type is LegType.STOP_LOSS for event in events), name
        assert len(__test_helper_audits(plugin, 'venue_bracket_fill_foreign_excess')) \
            == excess_audits, name
        assert __test_helper_audits(plugin, 'own_fill_consumed_foreign_exposure') == [], name
        assert plugin._run_owned_signed_size() == 0.0, name
        assert plugin.quarantines == [], name
        # Someone else opens and later closes 0.01 on the leg, across a
        # restart: none of it is the run's.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-2', qty='0.01', seq=103)))
        _push(plugin, __test_helper_view(idx=1, size=0.01, seq=103))
        assert plugin._scope_hedge_rows_to_run(
            __test_helper_view(idx=1, size=0.01, seq=103)['data']) == [], name
        fresh = _restarted(plugin, mode=POSITION_MODE_HEDGE)
        fresh._adoption_baselined = True
        _push(fresh, __test_helper_view(idx=1, size=0.01, seq=103))
        _push(fresh, __test_helper_view(idx=2, size=0, seq=103))
        _push(fresh, _exec_push(__test_helper_foreign_close(
            'f-3', qty='0.01', seq=104, createType='CreateByUser')))
        _push(fresh, __test_helper_view(idx=1, size=0, seq=104))
        assert fresh.quarantines == [], name
        assert plugin.quarantines == [], name


# --- same-direction adds leave the bot's things alone -----------------------

def __test_same_add_never_reports__(tmp_path):
    # Hedge trading stop: someone else adds to the protected short leg.
    plugin = _InterventionFake(responses=[{}], mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "same-add-hedge")
    plugin._adoption_baselined = True
    _seed_long(plugin, qty=0.02, side='sell', coid='entry-s')
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
    _push(plugin, _position_push(idx=2, side='Sell', size='0.02', tp='1879',
                                 sl='1887', trail='0', seq='50'))
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', side='Sell', qty='0.01', seq=101)))
    grown = _position_push(idx=2, side='Sell', size='0.03', tp='1879', sl='1887',
                           trail='0', seq='101')
    _push(plugin, grown)
    _push(plugin, grown)
    plugin._observe_position_rows(grown['data'], plugin._market,
                                  read_started_at=10 ** 12)
    assert plugin.quarantines == []

    # One-way order legs and a one-way trailing stop.
    for name, envelope in (('legs', _bracket_envelope(tp=110000.0)),
                           ('trail', _trail_envelope())):
        plugin = _InterventionFake(responses=[{'orderId': '610'}, {'orderId': '611'}])
        _attach_store(plugin, tmp_path, f"same-add-{name}")
        plugin._adoption_baselined = True
        _seed_long(plugin, qty=0.002)
        asyncio.run(plugin.execute_exit(envelope))
        # Order legs leave the position itself without a trading stop.
        stop = ({'tp': '0', 'sl': '0', 'trail': '0'} if name == 'legs' else {})
        _push(plugin, _position_push(size='0.002', seq='50', **stop))
        _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=101)))
        for _ in range(2):
            _push(plugin, _position_push(size='0.012', seq='101', **stop))
        if name == 'legs':
            _push(plugin, _order_push(envelope.client_order_id(KIND_EXIT_SL),
                                      trigger='90000'))
        assert plugin.quarantines == [], name
        assert __test_helper_audits(plugin, 'trading_stop_origin_unproven') == [], name


def __test_inverse_same_add_keeps_the_booked_base__():
    plugin = _InterventionFake()
    plugin._inverse_net_contracts = 165.0
    plugin._inverse_net_base = 0.002
    # Someone else adds 100 contracts at a higher price.
    size = plugin._inverse_core_size('buy', 265.0, 90000.0)
    assert size == pytest.approx(0.002 + 100 / 90000.0)
    assert size > plugin._inverse_net_base, "never a phantom reduction"
    # Fewer contracts than the run's is a real change.
    assert plugin._inverse_core_size('buy', 100.0, 82000.0) == pytest.approx(100 / 82000.0)


# --- inverse (contract-unit) one-way leg ------------------------------------

def __test_helper_inverse_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category='inverse', symbol='BTCUSD', base_coin='BTC', quote_coin='USD',
        settle_coin='BTC', status='Trading', tick_size_str='0.50', tick_size=0.5,
        qty_step_str='1', qty_step=1.0, min_order_qty=1.0, min_order_amt=0.0,
        min_notional=1.0, max_limit_order_qty=1_000_000.0,
        max_market_order_qty=1_000_000.0, contract_type='InversePerpetual',
        delivery_time=None,
    )


def __test_helper_inverse_exec(exec_id: str, **overrides) -> dict:
    """An inverse execution row (contracts); foreign by default."""
    return _close_exec(**{'symbol': 'BTCUSD', 'category': 'inverse', 'execId': exec_id,
                          'orderId': f"order-{exec_id}", 'execPrice': '82600',
                          **overrides})


def __test_helper_inverse_view(*, size, seq, side=None, avg='82600',
                               tp='0', sl='0', trail='0', updated=None) -> dict:
    if side is None:
        side = 'Buy' if float(size) > 0 else ''
    if updated is None:
        updated = 1752600000000 + int(seq)
    return {'topic': 'position', 'data': [{
        'symbol': 'BTCUSD', 'category': 'inverse', 'positionIdx': 0,
        'side': side, 'size': str(size), 'avgPrice': avg, 'takeProfit': tp,
        'stopLoss': sl, 'trailingStop': trail, 'updatedTime': str(updated),
        'seq': str(seq),
    }]}


def __test_helper_inverse_entry(plugin, coid: str, *, contracts: int, anchor: str,
                                price: str, seq: int, entry_id: str = 'Long') -> list:
    """Book one own inverse long entry through its execution push."""
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSD', side='buy', qty=float(contracts), filled_qty=0.0,
        state='confirmed', intent_key=f"{entry_id}-{coid}", pine_entry_id=entry_id,
        exchange_order_id=f"venue-{coid}",
        extras={'kind': ENTRY_KIND_POSITION, 'order_type': 'market', 'anchor': anchor},
    )
    return _push(plugin, _exec_push(__test_helper_inverse_exec(
        f"own-{coid}", orderId=f"venue-{coid}", orderLinkId=coid, side='Buy',
        execQty=str(contracts), execPrice=price, closedSize='0',
        createType='CreateByUser', execTime='1752599990000', seq=str(seq))))


def __test_helper_inverse_plugin(tmp_path, name: str) -> '_InterventionFake':
    """A one-way inverse run long 165 contracts, booked at the 82500 anchor."""
    plugin = _InterventionFake()
    plugin._market = __test_helper_inverse_instrument()
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    events = __test_helper_inverse_entry(plugin, 'entry-1', contracts=165,
                                         anchor='82500', price='82600', seq=40)
    assert len(events) == 1
    assert events[0].fill_qty == pytest.approx(165 / 82500.0, rel=1e-12)
    _push(plugin, __test_helper_inverse_view(size=165, seq=40))
    assert plugin._inverse_net_contracts == 165.0
    return plugin


def __test_helper_core_adoption(plugin, venue_avg: float) -> float:
    """What the core's startup adoption takes from the run's wire-unit journal."""
    engine_methods = vars(OrderSyncEngine)

    class _JournalReader:
        """The engine's own journal reads, over the plugin's journal."""
        _durable_owned_signed_size = engine_methods['_durable_owned_signed_size']
        _durable_owned_rows = engine_methods['_durable_owned_rows']
        _owned_total = engine_methods['_owned_total']
        _journal_in_book_units = engine_methods['_journal_in_book_units']
        _adoptable_owned_size = engine_methods['_adoptable_owned_size']

        def __init__(self, store_ctx) -> None:
            self._store_ctx = store_ctx

    engine = _JournalReader(plugin.store_ctx)
    assert not engine._journal_in_book_units(), "inverse rows stay in contracts"
    adopted = engine._adoptable_owned_size(engine._durable_owned_signed_size(), venue_avg)
    assert adopted is not None
    return adopted


def __test_helper_inverse_close_lands_flat(plugin) -> None:
    """A core full close of what the core holds lands on exactly flat."""
    book = plugin._inverse_core_size('buy', plugin._inverse_net_contracts, 82600.0)
    contracts, _anchor = asyncio.run(plugin._inverse_reduce_contracts(
        plugin._market, book, intent_key='Close', label='inverse close'))
    assert float(contracts) == plugin._inverse_net_contracts
    factor = plugin._inverse_fill_anchor(
        'close-x', from_entry=None, side='sell', contracts=float(contracts),
        exec_price=83000.0, reducing=True)
    assert float(contracts) / float(factor) == pytest.approx(book, rel=1e-12), \
        "no base residue in the core"


def __test_inverse_manual_close_reports_and_keeps_the_mirror_exact__(tmp_path):
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-manual")
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'm-1', side='Sell', execQty='165', closedSize='165', seq='100')))
    _push(plugin, __test_helper_inverse_view(size=0, seq=100))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['consumed'] == pytest.approx(165.0)
    assert context['owned'] == pytest.approx(165.0)
    assert plugin._run_owned_signed_size() == 0.0
    # The run's contracts left the leg: the mirror holds none of them.
    assert plugin._inverse_net_contracts == 0.0
    assert plugin._inverse_net_base == 0.0

    # The resumed bot re-enters at another anchor: the core-facing size is
    # exactly the base its fill booked, never a stale-mirror conversion at
    # the venue average.
    events = __test_helper_inverse_entry(plugin, 'entry-2', contracts=100,
                                         anchor='90000', price='90100', seq=120)
    _push(plugin, __test_helper_inverse_view(size=100, seq=120, avg='90100'))
    assert plugin._inverse_core_size('buy', 100.0, 90100.0) == events[0].fill_qty
    __test_helper_inverse_close_lands_flat(plugin)


def __test_inverse_part_close_reports_and_nets_the_contracts_taken__(tmp_path):
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-part")
    booked = plugin._inverse_net_base
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'm-1', side='Sell', execQty='65', closedSize='65', seq='100')))
    _push(plugin, __test_helper_inverse_view(size=100, seq=100))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['consumed'] == pytest.approx(65.0)
    assert context['physical_after'] == pytest.approx(100.0)
    # The journal stays in contracts, anchored: the core's wire-unit
    # adoption converts it at the venue average, and a restart adopts
    # exactly the run's share.
    assert plugin._run_owned_signed_size() == pytest.approx(100.0)
    row = plugin.store_ctx.get_order('entry-1')
    assert row is not None and (row.extras or {}).get('anchor') == '82500'
    assert __test_helper_core_adoption(plugin, 82600.0) == pytest.approx(100 / 82600.0)
    # The mirror keeps the run's contracts at its booked average, so the
    # core-facing size of the venue's 100 contracts is the booked base of
    # the 100 contracts left.
    assert plugin._inverse_net_contracts == 100.0
    assert plugin._inverse_net_base == pytest.approx(booked * 100 / 165, rel=1e-12)
    assert plugin._inverse_core_size('buy', 100.0, 82600.0) == plugin._inverse_net_base
    __test_helper_inverse_close_lands_flat(plugin)


def __test_inverse_add_overclose_reports_the_run_share_taken__(tmp_path):
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-overclose")
    booked = plugin._inverse_net_base
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'f-1', side='Buy', execQty='100', closedSize='0', createType='CreateByUser',
        seq='101')))
    _push(plugin, __test_helper_inverse_view(size=265, seq=101, avg='84000'))
    # The add alone changes none of the run's contracts.
    assert plugin.quarantines == []
    assert plugin._inverse_core_size('buy', 265.0, 84000.0) > booked
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'f-2', side='Sell', execQty='150', closedSize='150', seq='102')))
    _push(plugin, __test_helper_inverse_view(size=115, seq=102, avg='84000'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['consumed'] == pytest.approx(50.0)
    assert context['owned'] == pytest.approx(165.0)
    assert context['physical_after'] == pytest.approx(115.0)
    assert plugin._run_owned_signed_size() == pytest.approx(115.0)
    assert __test_helper_core_adoption(plugin, 84000.0) == pytest.approx(115 / 84000.0)
    assert plugin._inverse_net_contracts == 115.0
    assert plugin._inverse_core_size('buy', 115.0, 84000.0) == plugin._inverse_net_base
    __test_helper_inverse_close_lands_flat(plugin)


def __test_inverse_add_then_close_own_and_same_add_are_not_reported__(tmp_path):
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-close-own")
    booked = plugin._inverse_net_base
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'f-1', side='Buy', execQty='100', closedSize='0', createType='CreateByUser',
        seq='101')))
    _push(plugin, __test_helper_inverse_view(size=265, seq=101, avg='84000'))
    # same_add: the merged leg reports the booked base plus the extra
    # contracts, never a phantom reduction of the run's own.
    assert plugin._inverse_core_size('buy', 265.0, 84000.0) == pytest.approx(
        booked + 100 / 84000.0, rel=1e-12)
    # add_then_close_own: the other party takes exactly its own 100 back out.
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'f-2', side='Sell', execQty='100', closedSize='100', seq='102')))
    _push(plugin, __test_helper_inverse_view(size=165, seq=102, avg='84000'))
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'external_close_foreign_only')) == 1
    assert plugin._run_owned_signed_size() == pytest.approx(165.0)
    assert __test_helper_core_adoption(plugin, 82600.0) == pytest.approx(165 / 82600.0)
    assert plugin._inverse_net_contracts == 165.0
    assert plugin._inverse_core_size('buy', 165.0, 84000.0) == booked
    __test_helper_inverse_close_lands_flat(plugin)


def __test_inverse_late_own_reduce_after_a_give_up_moves_the_mirror_once__(
        tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-late-own")
    booked = plugin._inverse_net_base
    plugin.store_ctx.upsert_order(
        'tp-1', symbol='BTCUSD', side='sell', qty=65.0, filled_qty=0.0,
        state='confirmed', intent_key="TP\x00Long", from_entry='Long',
        exchange_order_id='venue-tp-1',
        extras={'kind': 'exit_leg', 'leg': 'tp', 'exit_id': 'TP', 'anchor': '82500'},
    )
    # The run's TP takes 65 contracts; its execution is missing past the
    # grace, so the ledger books the venue view as truth.
    _push(plugin, __test_helper_inverse_view(size=100, seq=60))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert plugin._run_owned_signed_size() == pytest.approx(100.0)
    assert plugin._inverse_net_contracts == 100.0
    # The TP execution arrives after all: neither the journal nor the
    # mirror takes its contracts a second time.
    events = plugin._backfill_one_execution(__test_helper_inverse_exec(
        'tp-a', orderId='venue-tp-1', orderLinkId='tp-1', side='Sell', execQty='65',
        closedSize='65', seq='60'), plugin._market)
    assert len(events) == 1
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == pytest.approx(100.0)
    assert plugin._inverse_net_contracts == 100.0
    assert plugin._inverse_net_base == pytest.approx(booked * 100 / 165, rel=1e-12)
    assert plugin._inverse_core_size('buy', 100.0, 82600.0) == plugin._inverse_net_base


def __test_inverse_fill_less_views_never_give_up_the_ledger__(tmp_path, monkeypatch):
    # Measured on the inverse stream (2026-10-18): the push of a fill carries
    # its seq, then more pushes follow with the same size and ``updatedTime``
    # but the symbol's later cross seq, which no execution of the account
    # carries (an order event before a close does the same).
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_inverse_plugin(tmp_path, "inverse-echo")
    plugin.store_ctx.upsert_order(
        'tp-1', symbol='BTCUSD', side='sell', qty=65.0, filled_qty=0.0,
        state='confirmed', intent_key="TP\x00Long", from_entry='Long',
        exchange_order_id='venue-tp-1',
        extras={'kind': 'exit_leg', 'leg': 'tp', 'exit_id': 'TP', 'anchor': '82500'},
    )
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'tp-a', orderId='venue-tp-1', orderLinkId='tp-1', side='Sell', execQty='65',
        closedSize='65', seq='60')))
    _push(plugin, __test_helper_inverse_view(size=100, seq=60))
    for seq in (75, 75, 90, 91):
        _push(plugin, __test_helper_inverse_view(size=100, seq=seq,
                                                 updated=1752600000060))
    clock[0] += plugin.fill_report_grace_s + 1
    plugin._ledger_advance(plugin._market)
    assert __test_helper_audits(plugin, 'position_ledger_gap') == []
    assert plugin._leg_ledgers[0].doubt == 0.0
    assert plugin._run_owned_signed_size() == pytest.approx(100.0)
    # The share stays proven: a manual close of it is reported in full, even
    # one concluded in the millisecond of the last fill (a new size is never
    # an echo).
    _push(plugin, _exec_push(__test_helper_inverse_exec(
        'm-1', side='Sell', execQty='100', closedSize='100', seq='120')))
    _push(plugin, __test_helper_inverse_view(size=0, seq=120, updated=1752600000060))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert plugin.quarantines[0][1]['consumed'] == pytest.approx(100.0)


def __test_helper_inverse_pyramid(tmp_path, name: str) -> '_InterventionFake':
    """An inverse run long 100 + 65 contracts from two entries (anchor 80000)."""
    plugin = _InterventionFake()
    plugin._market = __test_helper_inverse_instrument()
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    for coid, entry_id, contracts, seq in (('e-a', 'EA', 100, 40), ('e-b', 'EB', 65, 41)):
        __test_helper_inverse_entry(plugin, coid, contracts=contracts, anchor='80000',
                                    price='80000', seq=seq, entry_id=entry_id)
    _push(plugin, __test_helper_inverse_view(size=165, seq=41, avg='80000'))
    return plugin


def __test_helper_inverse_trailing_fire(plugin) -> list:
    return _push(plugin, _exec_push(__test_helper_inverse_exec(
        'ts-1', side='Sell', execQty='165', closedSize='165', execPrice='79000',
        stopOrderType='TrailingStop', createType='CreateByTrailingStop', seq='100')))


def __test_inverse_pyramid_trailing_stop_splits_in_contracts__(tmp_path):
    plugin = __test_helper_inverse_pyramid(tmp_path, "inverse-pyramid")
    plugin.responses = [{}, {}]
    for exit_id, entry_id, contracts in (('XA', 'EA', 100), ('XB', 'EB', 65)):
        asyncio.run(plugin.execute_exit(DispatchEnvelope(
            intent=ExitIntent(pine_id=exit_id, from_entry=entry_id, symbol='BTCUSD',
                              side='sell', qty=contracts / 80000.0, trail_offset=50.0),
            run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
        )))
    owners = sorted(plugin.store_ctx.iter_orders_in_state_for_run_id('bracket_own'),
                    key=lambda row: row.pine_entry_id or '')
    assert [(row.extras or {}).get('wire_qty') for row in owners] == [100.0, 65.0]
    events = __test_helper_inverse_trailing_fire(plugin)
    # Each exit closes its own entry's contracts, converted at its anchor.
    assert [(event.pine_id, event.fill_qty) for event in events] == [
        ('XA', pytest.approx(100 / 80000.0, rel=1e-12)),
        ('XB', pytest.approx(65 / 80000.0, rel=1e-12)),
    ]
    assert plugin.quarantines == []
    assert plugin._inverse_net_contracts == 0.0

    # Rows journalled without the wire quantity convert at their entry's anchor.
    plugin = __test_helper_inverse_pyramid(tmp_path, "inverse-pyramid-old-rows")
    for exit_id, entry_id, contracts in (('XA', 'EA', 100), ('XB', 'EB', 65)):
        create_bracket_ownership_row(
            plugin.store_ctx, coid=f"att-{exit_id}:0", symbol='BTCUSD', side='sell',
            qty=contracts / 80000.0, intent_key=f"{exit_id}\x00{entry_id}",
            pine_entry_id=exit_id, from_entry=entry_id, leg_id='0',
            attach_coid=f"att-{exit_id}", tp_price=None, sl_price=None,
            trail_price=None, trail_offset=50.0,
        )
    events = __test_helper_inverse_trailing_fire(plugin)
    assert [event.pine_id for event in events] == ['XA', 'XB']
    assert [payload['claim_qty'] for payload
            in __test_helper_audits(plugin, 'venue_bracket_fill_attributed')] == [100.0, 65.0]


def __test_inverse_start_clamp_keeps_the_venue_seeded_mirror__(tmp_path):
    # A restart: the journal still holds 165 contracts, the venue 100 (a
    # reduction while the bot was down). The mirror is seeded from the venue.
    plugin = _InterventionFake()
    plugin._market = __test_helper_inverse_instrument()
    _attach_store(plugin, tmp_path, "inverse-start")
    plugin.store_ctx.upsert_order(
        'entry-1', symbol='BTCUSD', side='buy', qty=165.0, filled_qty=165.0,
        state='confirmed', intent_key='Long', pine_entry_id='Long',
        extras={'kind': ENTRY_KIND_POSITION, 'anchor': '82500'},
    )
    view = __test_helper_inverse_view(size=100, seq=60)
    plugin._inverse_seed_net(view['data'])
    _push(plugin, view)
    plugin._adoption_baselined = True
    plugin._ledger_advance(plugin._market)
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == pytest.approx(100.0)
    # The clamp books the view the mirror already holds: not netted twice.
    assert plugin._inverse_net_contracts == 100.0
    assert plugin._inverse_net_base == pytest.approx(100 / 82600.0, rel=1e-12)
    assert plugin._inverse_core_size('buy', 100.0, 82600.0) == pytest.approx(
        __test_helper_core_adoption(plugin, 82600.0), rel=1e-12)


# --- order edits the push missed ---------------------------------------------

def __test_helper_open_order_row(coid: str, *, trigger='90000', price='0', qty='0.002',
                    status='Untriggered', order_id='611') -> dict:
    return _order_push(coid, status=status, trigger=trigger, price=price, qty=qty,
                       order_id=order_id)['data'][0]


def __test_lost_amend_push_is_detected_by_the_reconcile_snapshot__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "lost-push")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    moved = [__test_helper_open_order_row(coid, trigger='88000')]
    plugin._observe_open_order_levels(moved, plugin._market, read_started_at=10 ** 12)
    assert plugin.quarantines == [], "a polled mismatch needs a second snapshot"
    plugin._observe_open_order_levels(moved, plugin._market, read_started_at=10 ** 12)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_reconcile_open_orders_snapshot_judges_guarded_orders__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "reconcile-snapshot")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    moved = {'list': [__test_helper_open_order_row(coid, trigger='88000')],
             'nextPageCursor': ''}
    plugin.responses = [moved, moved]
    for _ in range(2):
        asyncio.run(plugin._disappearance_present(plugin._market, []))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_foreign_amend_during_own_amend_is_detected_by_the_snapshot__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}, {'orderId': '611'}])
    _attach_store(plugin, tmp_path, "amend-race")
    old = _bracket_envelope()
    asyncio.run(plugin.execute_exit(old))
    coid = old.client_order_id(KIND_EXIT_SL)
    # Someone moves the stop while the bot's own amend is in flight: the
    # push is skipped then.
    plugin.during['/v5/order/amend'] = lambda _body: _push(
        plugin, _order_push(coid, trigger='88000'))
    asyncio.run(plugin.modify_exit(old, _bracket_envelope(sl=91000.0)))
    assert plugin.quarantines == []
    moved = [__test_helper_open_order_row(coid, trigger='88000')]
    for _ in range(2):
        plugin._observe_open_order_levels(moved, plugin._market, read_started_at=10 ** 12)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_post_resume_baseline_is_taken_from_the_snapshot__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "resume")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    rebaseline_after_resume(plugin.store_ctx)

    fresh = _restarted(plugin)
    # The operator left the stop at 88000; the resumed bot reads it at once.
    fresh.responses = [{'list': [__test_helper_open_order_row(coid, trigger='88000')]},
                       {'list': []}]
    asyncio.run(fresh._observe_resume_baseline(fresh._market))
    assert fresh.quarantines == []
    # A later edit is judged against the operator's level.
    _push(fresh, _order_push(coid, trigger='87000'))
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


# --- resting entries and order quantities -----------------------------------

def __test_helper_entry_envelope(*, order_type=OrderType.LIMIT, limit=95000.0, stop=None,
                    qty=0.002) -> DispatchEnvelope:
    return DispatchEnvelope(
        intent=EntryIntent(pine_id='Long', symbol='BTCUSDT', side='buy', qty=qty,
                           order_type=order_type, limit=limit, stop=stop),
        run_tag='t3st', bar_ts_ms=1_752_600_000_000, coid_max_len=36,
    )


def __test_resting_entry_price_amend_reports__(tmp_path):
    cases = (
        ('limit', __test_helper_entry_envelope(), dict(status='New', price='94000', trigger='')),
        ('stop', __test_helper_entry_envelope(order_type=OrderType.STOP, limit=None, stop=105000.0),
         dict(status='Untriggered', trigger='106000')),
        ('qty', __test_helper_entry_envelope(), dict(status='New', price='95000', trigger='',
                                        qty='0.003')),
    )
    for name, envelope, push in cases:
        plugin = _InterventionFake(responses=[{'orderId': '700'}])
        _attach_store(plugin, tmp_path, f"entry-{name}")
        asyncio.run(plugin.execute_entry(envelope))
        coid = envelope.client_order_id(KIND_ENTRY)
        own = dict(push, price='95000' if name != 'stop' else '0',
                   trigger='' if name != 'stop' else '105000', qty='0.002')
        _push(plugin, _order_push(coid, order_id='700', **own))
        assert plugin.quarantines == [], name
        _push(plugin, _order_push(coid, order_id='700', **push))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name


def __test_own_modify_entry_is_not_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '700'}, {'orderId': '700'}])
    _attach_store(plugin, tmp_path, "entry-own-amend")
    old = __test_helper_entry_envelope()
    asyncio.run(plugin.execute_entry(old))
    coid = old.client_order_id(KIND_ENTRY)
    # The amended order is pushed before the amend ACK returns.
    plugin.during['/v5/order/amend'] = lambda body: _push(plugin, _order_push(
        coid, order_id='700', status='New', price=body['price'], trigger='',
        qty=body['qty']))
    asyncio.run(plugin.modify_entry(old, __test_helper_entry_envelope(limit=94000.0, qty=0.003)))
    assert plugin.quarantines == []
    # A late push of the previous own state and one of the new.
    _push(plugin, _order_push(coid, order_id='700', status='New', price='95000',
                              trigger='', qty='0.002'))
    _push(plugin, _order_push(coid, order_id='700', status='New', price='94000',
                              trigger='', qty='0.003'))
    assert plugin.quarantines == []


def __test_foreign_cancel_of_resting_entry_routes_to_the_policy__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '700'}])
    _attach_store(plugin, tmp_path, "entry-cancel")
    envelope = __test_helper_entry_envelope()
    asyncio.run(plugin.execute_entry(envelope))
    coid = envelope.client_order_id(KIND_ENTRY)
    cancelled = _order_push(coid, order_id='700', status='Cancelled', price='95000',
                            trigger='')
    cancelled['data'][0]['cancelType'] = 'CancelByUser'
    events = _push(plugin, cancelled)
    assert [event.event_type for event in events] == ['cancelled']
    # No venue reason: the engine applies the unexpected-cancel policy.
    assert events[0].cancel_reason is None
    assert not plugin._protection_guard().known(coid)


def __test_stop_limit_entry_is_not_guarded__(tmp_path):
    for name, last in (('resting', None), ('dormant', 94000.0)):
        plugin = _InterventionFake(responses=[{'orderId': '700'}, {'orderId': '700'}])
        _attach_store(plugin, tmp_path, f"stop-limit-{name}")
        plugin._last_price = last
        envelope = __test_helper_entry_envelope(limit=95000.0, stop=96000.0)
        asyncio.run(plugin.execute_entry(envelope))
        assert ('triggerPrice' in plugin.calls[-1][2]) == (name == 'dormant'), name
        coid = envelope.client_order_id(KIND_ENTRY)
        assert not plugin._protection_guard().known(coid), name
        # No venue view of it is judged, triggered or not.
        _push(plugin, _order_push(coid, order_id='700', status='Untriggered',
                                  price='94000', trigger='96500', qty='0.003'))
        _push(plugin, _order_push(coid, order_id='700', status='New', price='94500',
                                  trigger='', qty='0.003'))
        assert plugin.quarantines == [], name
    # A guarded limit entry amended into a stop-limit stops being guarded.
    plugin = _InterventionFake(responses=[{'orderId': '700'}, {'orderId': '700'}])
    _attach_store(plugin, tmp_path, "stop-limit-amended")
    old = __test_helper_entry_envelope()
    asyncio.run(plugin.execute_entry(old))
    coid = old.client_order_id(KIND_ENTRY)
    assert plugin._protection_guard().known(coid)
    asyncio.run(plugin.modify_entry(old, __test_helper_entry_envelope(limit=94000.0,
                                                                      stop=96000.0)))
    assert not plugin._protection_guard().known(coid)
    _push(plugin, _order_push(coid, order_id='700', status='New', price='94000',
                              trigger='', qty='0.002'))
    assert plugin.quarantines == []


def __test_exit_leg_qty_increase_reports__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '611'}])
    _attach_store(plugin, tmp_path, "leg-qty")
    asyncio.run(plugin.execute_exit(_bracket_envelope()))
    coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
    _push(plugin, _order_push(coid, trigger='90000', qty='0.002'))
    assert plugin.quarantines == []
    _push(plugin, _order_push(coid, trigger='90000', qty='0.003'))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['observed_qty'] == pytest.approx(0.003)


def __test_derivative_exit_leg_qty_cut_is_logged_never_reported__(tmp_path, caplog):
    for name, position, audits in (('short-of-position', 0.002, 1), ('trim', 0.001, 0)):
        plugin = _InterventionFake(responses=[{'orderId': '611'}])
        _attach_store(plugin, tmp_path, f"leg-cut-{name}")
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
        coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
        _push(plugin, __test_helper_view(size=position, seq=50))
        # The push alone never judges a cut: the position view explaining a
        # reduce-only trim may still be behind it.
        _push(plugin, _order_push(coid, trigger='90000', qty='0.001'))
        assert __test_helper_audits(plugin, 'order_qty_cut_unattributed') == [], name
        row = __test_helper_open_order_row(coid, qty='0.001')
        caplog.clear()
        with caplog.at_level('ERROR', logger='pynecore_bybit.protection'):
            for _ in range(3):
                plugin._observe_open_order_levels([row], plugin._market,
                                                  read_started_at=10 ** 12)
        assert plugin.quarantines == [], name
        logged = __test_helper_audits(plugin, 'order_qty_cut_unattributed')
        assert len(logged) == audits, name
        errors = [record for record in caplog.records if record.levelname == 'ERROR']
        assert len(errors) == audits, name
        if audits:
            assert logged[0]['expected_qty'] == pytest.approx(0.002)
            assert logged[0]['observed_qty'] == pytest.approx(0.001)
            assert logged[0]['position'] == pytest.approx(position)


def __test_derivative_entry_qty_cut_is_logged_never_reported__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '700'}])
    _attach_store(plugin, tmp_path, "entry-cut")
    envelope = __test_helper_entry_envelope()
    asyncio.run(plugin.execute_entry(envelope))
    coid = envelope.client_order_id(KIND_ENTRY)
    cut = dict(order_id='700', status='New', price='95000', trigger='', qty='0.001')
    _push(plugin, _order_push(coid, **cut))
    for _ in range(2):
        plugin._observe_open_order_levels([__test_helper_open_order_row(coid, **cut)],
                                          plugin._market, read_started_at=10 ** 12)
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'order_qty_cut_unattributed')) == 1


def __test_resume_accepts_the_venue_order_qty__(tmp_path):
    for name in ('entry', 'exit-leg'):
        plugin = _InterventionFake(responses=[{'orderId': '700'}])
        _attach_store(plugin, tmp_path, f"resume-qty-{name}")
        if name == 'entry':
            envelope = __test_helper_entry_envelope()
            asyncio.run(plugin.execute_entry(envelope))
            coid = envelope.client_order_id(KIND_ENTRY)
            edited = dict(status='New', price='95000', trigger='')
        else:
            envelope = _bracket_envelope()
            asyncio.run(plugin.execute_exit(envelope))
            coid = envelope.client_order_id(KIND_EXIT_SL)
            edited = dict(status='Untriggered', trigger='90000')
        _push(plugin, _order_push(coid, order_id='700', qty='0.003', **edited))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name

        # The operator accepts the venue as it stands; the bot restarts.
        rebaseline_after_resume(plugin.store_ctx)
        fresh = _restarted(plugin)
        row = __test_helper_open_order_row(coid, order_id='700', qty='0.003', **edited)
        for _ in range(3):
            fresh._observe_open_order_levels([row], fresh._market, read_started_at=10 ** 12)
        _push(fresh, _order_push(coid, order_id='700', qty='0.003', **edited))
        assert fresh.quarantines == [], name

        # The accepted quantity survives another restart; a new edit does not.
        again = _restarted(fresh)
        _push(again, _order_push(coid, order_id='700', qty='0.003', **edited))
        assert again.quarantines == [], name
        _push(again, _order_push(coid, order_id='700', qty='0.004', **edited))
        assert _kinds(again) == [QuarantineKind.EXTERNAL_INTERVENTION], name


def __test_own_amend_with_unknown_outcome_stays_own_after_restart__(tmp_path):
    plugin = _InterventionFake(responses=[{'orderId': '700'}, BybitConnectionError('timeout')])
    _attach_store(plugin, tmp_path, "unknown-amend-qty")
    old = __test_helper_entry_envelope()
    asyncio.run(plugin.execute_entry(old))
    coid = old.client_order_id(KIND_ENTRY)
    with pytest.raises(OrderDispositionUnknownError):
        asyncio.run(plugin.modify_entry(old, __test_helper_entry_envelope(qty=0.003)))
    assert plugin.store_ctx.get_order(coid).qty == pytest.approx(0.002)

    # The amend did land; the process restarts before anything resolves it.
    fresh = _restarted(plugin)
    _push(fresh, _order_push(coid, order_id='700', status='New', price='95000',
                             trigger='', qty='0.003'))
    assert fresh.quarantines == []
    _push(fresh, _order_push(coid, order_id='700', status='New', price='95000',
                             trigger='', qty='0.005'))
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_refused_amend_quantity_stops_being_own__(tmp_path):
    # An answered reject proves the order never carried the requested qty:
    # the same qty showing up later at the unchanged price is someone
    # else's edit, in this process and after a restart.
    for restart in (False, True):
        name = f"refused-amend-qty-{restart}"
        plugin = _InterventionFake(responses=[{'orderId': '700'}, BybitAPIError(
            "Bybit API error on /v5/order/amend: retCode=110007 "
            "ab not enough for new order", ret_code=110007,
        )])
        _attach_store(plugin, tmp_path, name)
        old = __test_helper_entry_envelope()
        asyncio.run(plugin.execute_entry(old))
        coid = old.client_order_id(KIND_ENTRY)
        with pytest.raises(Exception):  # noqa: B017 - the mapped reject is not under test
            asyncio.run(plugin.modify_entry(old, __test_helper_entry_envelope(qty=0.003)))
        judged = _restarted(plugin) if restart else plugin
        _push(judged, _order_push(coid, order_id='700', status='New', price='95000',
                                  trigger='', qty='0.003'))
        assert _kinds(judged) == [QuarantineKind.EXTERNAL_INTERVENTION], name


def __test_venue_initiated_cancel_types_are_venue_driven__(tmp_path):
    venue = ('CancelByReduceOnly', 'CancelByPrepareLiq', 'CancelAllBeforeLiq',
             'CancelByPrepareAdl', 'CancelAllBeforeAdl', 'CancelByAdmin',
             'CancelBySettle', 'CancelByDelivery', 'CancelByTpSlTsClear',
             'CancelByDCP', 'CancelByCannotAffordOrderCost',
             'CancelByPmTrialMmOverEquity', 'CancelByAccountBlocking',
             'CancelByCrossSelfMuch', 'CancelByCrossReachMaxTradeNum',
             'CancelByRebalance', 'CancelByOCOTpCanceledBySlTriggered',
             'CancelByOCOSlCanceledByTpTriggered')
    for index, cancel_type in enumerate((*venue, 'CancelByUser', 'CancelBySmp')):
        plugin = _InterventionFake(responses=[{'orderId': '611'}])
        _attach_store(plugin, tmp_path, f"cancel-{index}")
        asyncio.run(plugin.execute_exit(_bracket_envelope()))
        coid = _bracket_envelope().client_order_id(KIND_EXIT_SL)
        cancelled = _order_push(coid, status='Deactivated')
        cancelled['data'][0]['cancelType'] = cancel_type
        events = _push(plugin, cancelled)
        assert len(events) == 1, cancel_type
        if cancel_type in venue:
            assert events[0].cancel_reason in VENUE_DRIVEN_CANCEL_REASONS, cancel_type
        else:
            assert events[0].cancel_reason is None, cancel_type


def __test_spot_foreign_sl_amend_reports__(tmp_path):
    plugin = _InterventionFake()
    plugin._market = __test_helper_spot_instrument()
    _attach_store(plugin, tmp_path, "spot-sl")
    coid = 'spot-sl-leg'
    plugin.store_ctx.upsert_order(
        coid, symbol='ETHUSDC', side='sell', qty=0.01, state='confirmed',
        intent_key='TP/SL\x00Long', from_entry='Long',
        extras={'kind': 'exit_leg', 'leg': 'sl', 'exit_id': 'TP/SL'},
    )
    write = plugin._expect_protection(coid, ProtectionLevels(stop=2400.0), row_coid=coid)
    plugin._settle_protection(coid, ProtectionLevels(stop=2400.0), write=write)
    push = {'topic': 'order', 'data': [{
        'symbol': 'ETHUSDC', 'orderId': '901', 'orderLinkId': coid,
        'orderStatus': 'Untriggered', 'side': 'Sell', 'orderType': 'Market',
        'orderFilter': 'StopOrder', 'qty': '0.01', 'cumExecQty': '0',
        'price': '0', 'triggerPrice': '2300', 'createdTime': '1752600000000',
    }]}
    _push(plugin, push)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_helper_spot_leg(tmp_path, name: str, leg: str) -> tuple:
    """A spot run with one guarded exit leg (``tp``: limit, ``sl``: conditional)."""
    plugin = _InterventionFake()
    plugin._market = __test_helper_spot_instrument()
    _attach_store(plugin, tmp_path, name)
    coid = f"spot-{leg}-leg"
    plugin.store_ctx.upsert_order(
        coid, symbol='ETHUSDC', side='sell', qty=0.02, state='confirmed',
        intent_key='TP/SL\x00Long', from_entry='Long', exchange_order_id='901',
        extras={'kind': 'exit_leg', 'leg': leg, 'exit_id': 'TP/SL'},
    )
    levels = (ProtectionLevels(limit=2600.0) if leg == 'tp'
              else ProtectionLevels(stop=2400.0))
    write = plugin._expect_protection(coid, levels, row_coid=coid)
    plugin._settle_protection(coid, levels, write=write)
    return plugin, coid


def __test_helper_spot_leg_push(coid: str, leg: str, *, qty: str, status=None) -> dict:
    row = {
        'symbol': 'ETHUSDC', 'orderId': '901', 'orderLinkId': coid, 'side': 'Sell',
        'qty': qty, 'cumExecQty': '0', 'createdTime': '1752600000000',
    }
    if leg == 'tp':
        row.update(orderStatus=status or 'New', orderType='Limit', price='2600',
                   triggerPrice='0')
    else:
        row.update(orderStatus=status or 'Untriggered', orderType='Market',
                   orderFilter='StopOrder', price='0', triggerPrice='2400')
    return {'topic': 'order', 'data': [row]}


def __test_spot_foreign_exit_leg_qty_cut_reports__(tmp_path):
    for leg in ('tp', 'sl'):
        plugin, coid = __test_helper_spot_leg(tmp_path, f"spot-cut-{leg}", leg)
        _push(plugin, __test_helper_spot_leg_push(coid, leg, qty='0.02'))
        assert plugin.quarantines == [], leg
        # Someone amends the leg's quantity and keeps its level: the leg now
        # protects a fraction of the run's inventory.
        _push(plugin, __test_helper_spot_leg_push(coid, leg, qty='0.001'))
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], leg
        _, context, _ = plugin.quarantines[0]
        assert context['observed_qty'] == pytest.approx(0.001), leg


def __test_spot_own_exit_leg_qty_cut_is_not_reported__(tmp_path):
    for leg in ('tp', 'sl'):
        plugin, coid = __test_helper_spot_leg(tmp_path, f"spot-own-cut-{leg}", leg)
        # The bot itself resizes the leg; a partially filled leg shows its
        # order quantity, never judged.
        plugin._note_own_order_qty(coid, 0.01)
        _push(plugin, __test_helper_spot_leg_push(coid, leg, qty='0.01'))
        _push(plugin, __test_helper_spot_leg_push(coid, leg, qty='0.005',
                                                  status='PartiallyFilled'))
        assert plugin.quarantines == [], leg


def __test_spot_foreign_execution_is_left_to_the_inventory__(tmp_path):
    plugin = _InterventionFake()
    plugin._market = __test_helper_spot_instrument()
    _attach_store(plugin, tmp_path, "spot-foreign")
    plugin._adoption_baselined = True
    plugin._note_external_execution(_close_exec(symbol='ETHUSDC', category='spot'),
                                    plugin._market, ambiguous=False)
    assert plugin._leg_ledgers == {}
    assert plugin.quarantines == []


def __test_helper_spot_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category='spot', symbol='ETHUSDC', base_coin='ETH', quote_coin='USDC',
        settle_coin='USDC', status='Trading', tick_size_str='0.01', tick_size=0.01,
        qty_step_str='0.00001', qty_step=0.00001, min_order_qty=0.00001,
        min_order_amt=1.0, min_notional=1.0, max_limit_order_qty=1000.0,
        max_market_order_qty=1000.0, contract_type='', delivery_time=None,
    )


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


# === Bare position: a protection the bot never set added to its position =====

def __test_helper_own_entry_fill(plugin, coid: str, *, qty: str, seq: int,
                                 exec_ms: int = 1_752_599_990_000) -> list:
    """Book one own linear long entry through its execution push."""
    plugin.store_ctx.upsert_order(
        coid, symbol='BTCUSDT', side='buy', qty=float(qty), filled_qty=0.0,
        state='confirmed', intent_key=f"Long-{coid}", pine_entry_id='Long',
        exchange_order_id=f"venue-{coid}", extras={'kind': ENTRY_KIND_POSITION},
    )
    return _push(plugin, _exec_push(_close_exec(
        execId=f"own-{coid}", orderId=f"venue-{coid}", orderLinkId=coid, side='Buy',
        execQty=qty, closedSize='0', createType='CreateByUser', seq=str(seq),
        execTime=str(exec_ms))))


def __test_helper_stop_view(*, idx=0, side='Buy', size='0.02', tp='0', sl='0', trail='0',
                            seq='60') -> dict:
    return _position_push(idx=idx, side=side, size=size, tp=tp, sl=sl, trail=trail,
                          seq=seq)


def __test_foreign_stop_added_to_a_bare_position_reports__(tmp_path):
    for name, levels in (('sl', {'sl': '29000'}), ('tp', {'tp': '31000'}),
                         ('trail', {'sl': '29500', 'trail': '50'})):
        plugin = _manual_plugin(tmp_path, f"bare-{name}")
        # The view of the run's position set the bare baseline.
        assert plugin._protection_guard().known('trading-stop:0'), name
        added = __test_helper_stop_view(**levels)
        _push(plugin, added)
        assert plugin.quarantines == [], "a position push needs a confirmation"
        _push(plugin, added)
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name
        _, context, _ = plugin.quarantines[0]
        assert context['protection_key'] == 'trading-stop:0'
        assert context['expected'] == [None, None, False, None]

    # Hedge: the run's Buy leg is bare; the Sell leg holds nothing of the run.
    plugin = __test_helper_hedge_plugin(tmp_path, "bare-hedge")
    other = __test_helper_stop_view(idx=2, side='Sell', size='0.01', sl='31000', seq='70')
    _push(plugin, other)
    _push(plugin, other)
    assert plugin.quarantines == [], "someone else's own position"
    added = __test_helper_stop_view(idx=1, sl='29000', seq='70')
    _push(plugin, added)
    _push(plugin, added)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['protection_key'] == 'trading-stop:1'


def __test_bare_baseline_survives_a_restart__(tmp_path):
    plugin = _manual_plugin(tmp_path, "bare-restart")
    row = plugin.store_ctx.get_order('entry-1')
    assert (row.extras or {})[PROTECTION_LEVELS_EXTRA]['key'] == 'trading-stop:0'
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    added = __test_helper_stop_view(sl='29000')
    _push(fresh, added)
    _push(fresh, added)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_helper_store_clock(monkeypatch) -> None:
    """Give every journal row its own creation time, in creation order."""
    tick = [1_752_600_000_000]

    def now_ms() -> int:
        tick[0] += 1
        return tick[0]

    monkeypatch.setattr('pynecore.core.broker.storage._now_ms', now_ms)


def __test_helper_trading_stop_home(plugin) -> list:
    """The live rows holding the persisted levels of ``trading-stop:0``."""
    return [row.client_order_id for row in plugin.store_ctx.iter_live_orders()
            if ((row.extras or {}).get(PROTECTION_LEVELS_EXTRA) or {}).get('key')
            == 'trading-stop:0']


def __test_bare_baseline_never_takes_over_a_resting_entry_record__(tmp_path):
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "bare-resting")
    plugin._adoption_baselined = True
    # The run's share sits on a resting limit entry, half filled: its row
    # holds the entry's own limit.
    plugin.store_ctx.upsert_order(
        'entry-w', symbol='BTCUSDT', side='buy', qty=0.04, filled_qty=0.02,
        state='confirmed', intent_key='Long-w', pine_entry_id='Long',
        exchange_order_id='venue-w',
        extras={'kind': ENTRY_KIND_WORKING, 'order_type': 'limit'},
    )
    plugin._note_own_order_qty('entry-w', 0.04)
    write = plugin._expect_protection('entry-w', ProtectionLevels(limit=29000.0),
                                      row_coid='entry-w')
    plugin._settle_protection('entry-w', ProtectionLevels(limit=29000.0), write=write)
    _push(plugin, __test_helper_view(size=0.02, seq=50))
    assert plugin._protection_guard().known('trading-stop:0')
    record = plugin.store_ctx.get_order('entry-w').extras[PROTECTION_LEVELS_EXTRA]
    assert record['key'] == 'entry-w'
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    moved = _order_push('entry-w', status='New', trigger='0', price='28000', qty='0.04',
                        order_id='venue-w')
    _push(fresh, moved)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]

    # The entry fills completely: its row rests no more, and the next view
    # of the position persists the bare baseline there.
    _push(plugin, _exec_push(_close_exec(
        execId='own-w', orderId='venue-w', orderLinkId='entry-w', side='Buy',
        execQty='0.02', closedSize='0', createType='CreateByUser', seq='60')))
    assert plugin._run_owned_signed_size() == pytest.approx(0.04)
    assert __test_helper_trading_stop_home(plugin) == []
    _push(plugin, __test_helper_view(size=0.04, seq=60))
    assert __test_helper_trading_stop_home(plugin) == ['entry-w']
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    added = __test_helper_stop_view(size='0.04', sl='29000', seq='70')
    _push(fresh, added)
    _push(fresh, added)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_bare_baseline_moves_off_an_entry_row_the_netting_closes__(tmp_path, monkeypatch):
    __test_helper_store_clock(monkeypatch)
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "bare-netted")
    _seed_long(plugin, qty=0.01)
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_view(size=0.01, seq=50))
    assert __test_helper_trading_stop_home(plugin) == ['entry-1']
    # The run adds; its own TP then takes 0.01, netting the oldest row away.
    __test_helper_own_entry_fill(plugin, 'entry-2', qty='0.01', seq=55)
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    _push(plugin, _exec_push(__test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01',
                                                    seq=60)))
    assert plugin.store_ctx.get_order('entry-1').closed_ts_ms is not None
    assert plugin._run_owned_signed_size() == pytest.approx(0.01)
    assert __test_helper_trading_stop_home(plugin) == ['entry-2']
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    added = __test_helper_stop_view(size='0.01', sl='29000', seq='70')
    _push(fresh, added)
    _push(fresh, added)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_bare_baseline_moves_off_the_row_of_a_refused_attach__(tmp_path):
    plugin = _manual_plugin(tmp_path, "bare-refused-attach")
    plugin.responses = [BybitAPIError(
        "Bybit API error on /v5/position/trading-stop: retCode=10001 params error",
        ret_code=10001,
    )]
    with pytest.raises(BracketAttachAfterFillRejectedError):
        asyncio.run(plugin.execute_exit(_trail_envelope()))
    # The refused attach left the bare levels on its released ownership row.
    assert __test_helper_trading_stop_home(plugin) == []
    _push(plugin, __test_helper_view(size=0.02, seq=60))
    assert __test_helper_trading_stop_home(plugin) == ['entry-1']
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    added = __test_helper_stop_view(sl='29000', seq='70')
    _push(fresh, added)
    _push(fresh, added)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_view_older_than_the_run_fill_never_sets_the_bare_baseline__(tmp_path):
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "bare-merged")
    plugin._adoption_baselined = True
    # Someone else's position already carries a stop.
    _push(plugin, __test_helper_stop_view(size='0.01', sl='29000', seq='30'))
    assert not plugin._protection_guard().known('trading-stop:0')
    # The run's own entry merges into it at seq 40.
    assert len(__test_helper_own_entry_fill(plugin, 'entry-1', qty='0.02', seq=40)) == 1
    # A delayed view from before the fill shows no stop: it proves nothing
    # about the position the run holds a share of.
    _push(plugin, __test_helper_stop_view(size='0.01', seq='39'))
    assert not plugin._protection_guard().known('trading-stop:0')
    # The first view of the merged position shows the stop: it was there
    # before the fill or added since — not known.
    merged = __test_helper_stop_view(size='0.03', sl='29000', seq='40')
    _push(plugin, merged)
    _push(plugin, merged)
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'trading_stop_origin_unproven')) == 1
    # From then on it covers the run's position: moving it is an edit.
    moved = __test_helper_stop_view(size='0.03', sl='28000', seq='40')
    _push(plugin, moved)
    _push(plugin, moved)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_view_before_its_own_execution_push_sets_the_bare_baseline__(tmp_path):
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "bare-view-first")
    plugin._adoption_baselined = True
    # The view of the position the run's entry built arrives before the
    # entry's execution push.
    _push(plugin, __test_helper_stop_view(size='0.02', seq='40'))
    assert not plugin._protection_guard().known('trading-stop:0')
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.02', seq=40)
    assert plugin._protection_guard().known('trading-stop:0')
    # A stop someone else adds before any further view is judged against it.
    added = __test_helper_stop_view(size='0.02', sl='29000', seq='40')
    _push(plugin, added)
    _push(plugin, added)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert __test_helper_audits(plugin, 'trading_stop_origin_unproven') == []

    # A view older than the execution still proves nothing.
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, "bare-view-older")
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_stop_view(size='0.01', seq='39'))
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.02', seq=40)
    assert not plugin._protection_guard().known('trading-stop:0')


def __test_helper_attach_after_exec_first(tmp_path, name: str, *,
                                          during=None) -> '_InterventionFake':
    """The run's entry execution push arrives before any view of its position.

    The flat leg was seen at seq 30; the entry fills at seq 40 and the bot
    attaches its trailing stop before the position push of the fill arrives.
    ``during`` is pushed while the attach is in flight.
    """
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_view(size=0, seq=30))
    assert len(__test_helper_own_entry_fill(plugin, 'entry-1', qty='0.002', seq=40)) == 1
    if during is not None:
        plugin.during['/v5/position/trading-stop'] = lambda _body: _push(plugin, during)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    plugin.during.clear()
    return plugin


def __test_view_of_the_fill_after_the_own_attach_is_an_older_write__(tmp_path):
    fill_view = __test_helper_stop_view(size='0.002', seq='40')
    own = __test_helper_stop_view(size='0.002', tp='110000', sl='90000', trail='5',
                                  seq='40')
    for name, during in (('settled', None), ('in-flight', fill_view)):
        plugin = __test_helper_attach_after_exec_first(tmp_path, f"exec-first-{name}",
                                                       during=during)
        # The venue pushed the position the fill built before the attach
        # landed: bare, the bot's own older state, never someone else's edit.
        _push(plugin, fill_view)
        _push(plugin, fill_view)
        assert plugin.quarantines == [], name
        assert __test_helper_audits(plugin, 'trading_stop_origin_unproven') == [], name
        # Once the venue is seen carrying the attach, a bare view is an edit.
        _push(plugin, own)
        _push(plugin, fill_view)
        _push(plugin, fill_view)
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], name


def __test_view_older_than_the_run_fill_never_judges_or_forgets_the_own_stop__(tmp_path):
    own = __test_helper_stop_view(size='0.002', tp='110000', sl='90000', trail='5',
                                  seq='40')
    bare = __test_helper_stop_view(size='0.002', seq='40')
    # A re-push of the flat leg from before the run's fill must not retire
    # the stop the bot just attached to the position the fill opened.
    plugin = __test_helper_attach_after_exec_first(tmp_path, "older-flat")
    _push(plugin, __test_helper_view(size=0, seq=30))
    assert plugin._protection_guard().known('trading-stop:0')
    _push(plugin, own)
    assert plugin.quarantines == []
    assert __test_helper_audits(plugin, 'trading_stop_origin_unproven') == []
    _push(plugin, bare)
    _push(plugin, bare)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]

    # Someone else's position carried its own stop before the run's add
    # merged into it; the bot's Full attach replaced it. Late views of that
    # older position are not judged against the bot's levels.
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "older-merged")
    plugin._adoption_baselined = True
    foreign = __test_helper_stop_view(size='0.01', sl='29000', seq='30')
    _push(plugin, foreign)
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.002', seq=40)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    _push(plugin, foreign)
    _push(plugin, foreign)
    assert plugin.quarantines == []
    assert plugin._protection_guard().known('trading-stop:0')
    # The view of the merged position from before the attach still shows the
    # other party's stop: not the bot's, and not provably an edit either.
    merged = __test_helper_stop_view(size='0.012', sl='29000', seq='40')
    _push(plugin, merged)
    _push(plugin, merged)
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'trading_stop_origin_unproven')) == 1


def __test_helper_merge_into_a_protected_position(tmp_path, name: str) -> '_InterventionFake':
    """The run's 0.02 joins someone else's 0.01 that already carries a stop."""
    plugin = _InterventionFake()
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    _push(plugin, __test_helper_stop_view(size='0.01', sl='29000', seq='30'))
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.02', seq=40)
    merged = __test_helper_stop_view(size='0.03', sl='29000', seq='40')
    _push(plugin, merged)
    _push(plugin, merged)
    assert plugin.quarantines == []
    assert len(__test_helper_audits(plugin, 'trading_stop_origin_unproven')) == 1
    return plugin


def __test_fire_of_a_stop_of_unproven_origin_is_not_reported__(tmp_path):
    for stop_type in ('StopLoss', 'TakeProfit', 'TrailingStop'):
        plugin = __test_helper_merge_into_a_protected_position(
            tmp_path, f"merged-fire-{stop_type}")
        # The venue executes the stop the position carried before the run
        # joined it: nobody changed anything of the bot's after it entered.
        _push(plugin, _exec_push(__test_helper_stop_fire(
            f"s-{stop_type}", stop_type, qty='0.03', seq='50')))
        _push(plugin, __test_helper_view(size=0, seq=50))
        assert plugin.quarantines == [], stop_type
        assert len(__test_helper_audits(plugin, 'external_close_unproven')) == 1, stop_type
        assert plugin._run_owned_signed_size() == 0.0, stop_type

    # A restart forgets where the share started: the stop stays unproven.
    plugin = __test_helper_merge_into_a_protected_position(tmp_path, "merged-fire-restart")
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    _push(fresh, __test_helper_stop_view(size='0.03', sl='29000', seq='45'))
    _push(fresh, _exec_push(__test_helper_stop_fire('s-r', 'StopLoss', qty='0.03', seq='50')))
    _push(fresh, __test_helper_view(size=0, seq=50))
    assert fresh.quarantines == []
    assert len(__test_helper_audits(fresh, 'external_close_unproven')) == 1


def __test_unproven_stop_of_an_earlier_share_proves_nothing_later__(tmp_path):
    plugin = __test_helper_merge_into_a_protected_position(tmp_path, "merged-next-share")
    _push(plugin, _exec_push(__test_helper_stop_fire('s-1', 'StopLoss', qty='0.03', seq='50')))
    _push(plugin, __test_helper_view(size=0, seq=50))
    assert plugin.quarantines == []
    # A new share on a bare position: a stop the bot never set fires on it.
    __test_helper_own_entry_fill(plugin, 'entry-2', qty='0.02', seq=60,
                                 exec_ms=1_752_600_010_000)
    _push(plugin, __test_helper_view(size=0.02, seq=60))
    _push(plugin, _exec_push(__test_helper_stop_fire('s-2', 'StopLoss', qty='0.02', seq='70')))
    _push(plugin, __test_helper_view(size=0, seq=70))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert context['stop_order_type'] == 'StopLoss'


def __test_own_trading_stop_on_a_bare_position_is_not_reported__(tmp_path):
    plugin = _manual_plugin(tmp_path, "bare-own-write")
    plugin.responses = [{}]
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    own = __test_helper_stop_view(tp='110000', sl='90000', trail='5', seq='60')
    _push(plugin, own)
    _push(plugin, own)
    assert plugin.quarantines == []


def __test_own_cleared_trading_stop_stays_guarded__(tmp_path):
    # One-way: the bot clears its own trailing stop and keeps the position.
    plugin = _InterventionFake(responses=[{}, {}])
    _attach_store(plugin, tmp_path, "cleared-oneway")
    plugin._adoption_baselined = True
    _seed_long(plugin, qty=0.002)
    asyncio.run(plugin.execute_exit(_trail_envelope()))
    asyncio.run(plugin._clear_position_trading_stop(
        plugin._market, context_coid='cancel-x', row_coid=None))
    assert plugin._protection_guard().known('trading-stop:0')
    row = plugin.store_ctx.get_order('entry-1')
    assert (row.extras or {})[PROTECTION_LEVELS_EXTRA]['key'] == 'trading-stop:0'
    bare = __test_helper_stop_view(size='0.002', seq='60')
    _push(plugin, bare)
    _push(plugin, bare)
    assert plugin.quarantines == []
    added = __test_helper_stop_view(size='0.002', sl='29000', seq='60')
    _push(plugin, added)
    _push(plugin, added)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]

    # Hedge: the emulator clears the bracket of a leg the run still holds.
    plugin = _InterventionFake(responses=[{}, {}], mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "cleared-hedge")
    plugin._adoption_baselined = True
    _seed_long(plugin, qty=0.02, side='sell', coid='entry-s')
    create_bracket_ownership_row(
        plugin.store_ctx, coid="bo:S-X\x00S:2", symbol='BTCUSDT.P', side='buy',
        qty=0.02, intent_key="S-X\x00S", pine_entry_id='S-X', from_entry='S',
        leg_id='2', attach_coid='attach-s', tp_price=1879.0, sl_price=1887.0,
        trail_price=None, trail_offset=None,
    )
    for tp, sl in ((1879.0, 1887.0), (None, None)):
        asyncio.run(plugin.amend_bracket(
            'BTCUSDT', '2', side='buy', tp_price=tp, sl_price=sl,
            trail_offset=None, coid='attach-s',
        ))
    assert plugin._protection_guard().known('trading-stop:2')
    added = __test_helper_stop_view(idx=2, side='Sell', tp='1850', seq='60')
    _push(plugin, added)
    _push(plugin, added)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]

    # Without a share of the run on the leg, a cleared stop leaves nothing.
    plugin = _InterventionFake(responses=[{}, {}], mode=POSITION_MODE_HEDGE)
    _attach_store(plugin, tmp_path, "cleared-hedge-flat")
    for tp, sl in ((1879.0, 1887.0), (None, None)):
        asyncio.run(plugin.amend_bracket(
            'BTCUSDT', '2', side='buy', tp_price=tp, sl_price=sl,
            trail_offset=None, coid='attach-s',
        ))
    assert not plugin._protection_guard().known('trading-stop:2')


def __test_foreign_close_of_the_whole_run_share_forgets_its_protection__(tmp_path):
    # The run's 0.02 is closed by someone else; foreign exposure keeps the
    # leg open. Whatever happens to the protection there afterwards is not
    # the bot's.
    plugin = _manual_plugin(tmp_path, "share-gone", physical=0.05)
    assert plugin._protection_guard().known('trading-stop:0')
    _push(plugin, _exec_push(__test_helper_foreign_close('f-1', qty='0.05', seq=100)))
    _push(plugin, _exec_push(__test_helper_foreign_open('f-2', qty='0.03', seq=101)))
    _push(plugin, __test_helper_view(size=0.03, seq=101))
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert plugin._run_owned_signed_size() == 0.0
    assert not plugin._protection_guard().known('trading-stop:0')
    added = __test_helper_stop_view(size='0.03', sl='29000', seq='101')
    _push(plugin, added)
    _push(plugin, added)
    assert len(plugin.quarantines) == 1


# --- foreign partial TP/SL orders --------------------------------------------

def __test_helper_partial_tpsl_order(order_id: str, *, qty='0.02', created_ms=1_752_600_000_100,
                                     stop_type='PartialStopLoss', side='Sell',
                                     status='Untriggered', idx=0, trigger='29000',
                                     updated_ms=None) -> dict:
    return {'symbol': 'BTCUSDT', 'orderId': order_id, 'orderLinkId': '',
            'orderStatus': status, 'side': side, 'orderType': 'Market',
            'stopOrderType': stop_type, 'qty': qty, 'cumExecQty': '0', 'price': '0',
            'triggerPrice': trigger, 'positionIdx': idx, 'createdTime': str(created_ms),
            'updatedTime': str(created_ms if updated_ms is None else updated_ms)}


def __test_helper_partial_plugin(tmp_path, name: str, *, mode=POSITION_MODE_ONE_WAY):
    """A run whose 0.02 share started with its own fill at seq 40."""
    plugin = _InterventionFake(mode=mode)
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.02', seq=40)
    idx = 1 if mode == POSITION_MODE_HEDGE else 0
    _push(plugin, __test_helper_view(idx=idx, size=0.02, seq=40))
    return plugin


def __test_helper_partial_settle(plugin, clock, *, size, seq, idx=0) -> None:
    """Let the fill-report grace pass, then deliver a position view taken after it."""
    clock[0] += plugin.fill_report_grace_s
    _push(plugin, __test_helper_view(idx=idx, size=size, seq=seq))


def __test_helper_partial_verdicts(plugin) -> list:
    return [payload['verdict'] for payload
            in __test_helper_audits(plugin, 'foreign_tpsl_order_judged')]


def __test_foreign_partial_tpsl_order_on_the_run_position_reports__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    for mode, idx in ((POSITION_MODE_ONE_WAY, 0), (POSITION_MODE_HEDGE, 1)):
        plugin = __test_helper_partial_plugin(tmp_path, f"partial-{mode}", mode=mode)
        order = __test_helper_partial_tpsl_order('p-1', idx=idx)
        _push(plugin, {'topic': 'order', 'data': [order]})
        assert plugin.quarantines == [], "judged once the evidence covers its creation"
        __test_helper_partial_settle(plugin, clock, idx=idx, size=0.02, seq=40)
        assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION], mode
        _, context, _ = plugin.quarantines[0]
        assert context['order_id'] == 'p-1'
        assert context['position_idx'] == idx
        assert context['stop_order_type'] == 'PartialStopLoss'
        assert context['foreign_exposure'] == 0.0
        # The snapshot and a restart see the same order: judged once.
        plugin._observe_open_order_levels([order], plugin._market, read_started_at=clock[0])
        fresh = _restarted(plugin, mode=mode)
        fresh._adoption_baselined = True
        fresh._observe_open_order_levels([order], fresh._market, read_started_at=clock[0])
        assert len(plugin.quarantines) == 1 and fresh.quarantines == [], mode
        assert __test_helper_partial_verdicts(plugin) == ['reported'], mode

    # Found by the open-orders snapshot alone, judged on a later reconcile pass.
    plugin = __test_helper_partial_plugin(tmp_path, "partial-snapshot")
    snapshot = [__test_helper_partial_tpsl_order('p-2', stop_type='PartialTakeProfit')]
    plugin._observe_open_order_levels(snapshot, plugin._market, read_started_at=clock[0])
    clock[0] += plugin.fill_report_grace_s
    # The grace alone proves nothing: no view of the leg was taken since.
    plugin._ledger_advance(plugin._market)
    assert plugin.quarantines == []
    rows = __test_helper_view(size=0.02, seq=40)['data']
    plugin._ingest_position_sizes(rows)
    plugin._observe_position_rows(rows, plugin._market, read_started_at=clock[0])
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]


def __test_foreign_partial_tpsl_order_beside_the_run_share_is_not_reported__(tmp_path,
                                                                            monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    cases = (
        # It covers only the 0.03 someone else holds beside the run's 0.02.
        ('foreign_only', {}, 0.05),
        # It was there before the run's fill merged into the position.
        ('preexisting', {'created_ms': 1_752_599_980_000}, 0.02),
        # A Full-mode stop order is the trading stop's: judged from the
        # position rows, never here.
        (None, {'stop_type': 'StopLoss'}, 0.02),
        # It would close a short; the run is long.
        (None, {'side': 'Buy'}, 0.02),
        # Already gone.
        (None, {'status': 'Cancelled'}, 0.02),
    )
    for index, (verdict, overrides, physical) in enumerate(cases):
        plugin = __test_helper_partial_plugin(tmp_path, f"partial-beside-{index}")
        seq = 40
        if physical > 0.02:
            seq = 41
            _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=seq)))
            _push(plugin, __test_helper_view(size=physical, seq=seq))
        _push(plugin, {'topic': 'order', 'data': [
            __test_helper_partial_tpsl_order(f"p-{index}", **overrides)]})
        __test_helper_partial_settle(plugin, clock, size=physical, seq=seq)
        assert plugin.quarantines == [], (verdict, overrides)
        assert __test_helper_partial_verdicts(plugin) == ([verdict] if verdict else []), \
            (verdict, overrides)


def __test_amended_foreign_partial_tpsl_order_is_judged_again__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    amend_ms = 1_752_600_005_000
    # Someone else holds 0.03 beside the run's 0.02; its Partial SL covers
    # its own 0.03 only, then grows to cover the run's share.
    plugin = __test_helper_partial_plugin(tmp_path, "amend-qty")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=41)))
    _push(plugin, __test_helper_view(size=0.05, seq=41))
    order = __test_helper_partial_tpsl_order('p-1', qty='0.03')
    _push(plugin, {'topic': 'order', 'data': [order]})
    __test_helper_partial_settle(plugin, clock, size=0.05, seq=41)
    assert __test_helper_partial_verdicts(plugin) == ['foreign_only']
    # The same order seen again, or shrunk, is not judged again.
    _push(plugin, {'topic': 'order', 'data': [order]})
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-1', qty='0.02', updated_ms=amend_ms)]})
    __test_helper_partial_settle(plugin, clock, size=0.05, seq=41)
    assert __test_helper_partial_verdicts(plugin) == ['foreign_only']
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-1', qty='0.04', updated_ms=amend_ms)]})
    assert plugin.quarantines == [], "judged once the evidence covers the amend"
    __test_helper_partial_settle(plugin, clock, size=0.05, seq=41)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = plugin.quarantines[0]
    assert (context['order_id'], context['amended']) == ('p-1', True)
    assert context['foreign_exposure'] == pytest.approx(0.03)
    assert __test_helper_partial_verdicts(plugin) == ['foreign_only', 'reported']

    # A stop that covered the run's position before its fill merged in is
    # moved after the run entered: a change of the stop covering the run.
    plugin = __test_helper_partial_plugin(tmp_path, "amend-trigger")
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-2', created_ms=1_752_599_980_000)]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert __test_helper_partial_verdicts(plugin) == ['preexisting']
    moved = __test_helper_partial_tpsl_order('p-2', created_ms=1_752_599_980_000,
                                             trigger='28000', updated_ms=amend_ms)
    plugin._observe_open_order_levels([moved], plugin._market, read_started_at=clock[0])
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert _kinds(plugin) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert __test_helper_partial_verdicts(plugin) == ['preexisting', 'reported']
    # Reported once: never judged again, not even after a restart.
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    fresh._observe_open_order_levels([__test_helper_partial_tpsl_order(
        'p-2', created_ms=1_752_599_980_000, trigger='27000', updated_ms=amend_ms + 1)],
        fresh._market, read_started_at=clock[0])
    assert fresh._pending_tpsl_orders == {}

    # An amend made before the run's fill merged in is no change of the run's.
    plugin = __test_helper_partial_plugin(tmp_path, "amend-before-fill")
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-3', created_ms=1_752_599_970_000)]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-3', created_ms=1_752_599_970_000, trigger='28500', updated_ms=1_752_599_980_000)]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['preexisting', 'preexisting']


def __test_judged_partial_tpsl_order_terms_survive_a_restart__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_partial_plugin(tmp_path, "amend-restart")
    order = __test_helper_partial_tpsl_order('p-1', created_ms=1_752_599_980_000)
    _push(plugin, {'topic': 'order', 'data': [order]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert __test_helper_partial_verdicts(plugin) == ['preexisting']
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    fresh._note_share_held_since(1_752_600_000_000, fresh._market)
    # The order the earlier process judged, unchanged: not judged again.
    fresh._observe_open_order_levels([order], fresh._market, read_started_at=clock[0])
    assert fresh._pending_tpsl_orders == {}
    # Amended after the share was proven held: judged again.
    _push(fresh, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-1', created_ms=1_752_599_980_000, trigger='28000', updated_ms=1_752_600_005_000)]})
    __test_helper_partial_settle(fresh, clock, size=0.02, seq=40)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]
    assert __test_helper_partial_verdicts(fresh) == ['preexisting', 'reported']


def __test_partial_tpsl_order_ahead_of_the_view_of_its_fill_is_not_reported__(tmp_path,
                                                                             monkeypatch):
    # Someone else buys 0.03 beside the run's 0.02 with a Partial SL attached
    # to its own fill. The SL's order push comes ahead of the position push
    # that shows the fill — once even ahead of the execution, behind a
    # delayed view taken before the fill: the leg first looks like the run's
    # 0.02 alone.
    clock = __test_helper_clock(monkeypatch)
    for name, order_first in (('after-exec', False), ('before-exec', True)):
        plugin = __test_helper_partial_plugin(tmp_path, f"race-{name}")
        order = {'topic': 'order', 'data': [
            __test_helper_partial_tpsl_order('p-1', qty='0.03')]}
        add = _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=41))
        if order_first:
            _push(plugin, order)
            clock[0] += 1.0
            _push(plugin, __test_helper_view(size=0.02, seq=40))
            _push(plugin, add)
        else:
            _push(plugin, add)
            _push(plugin, order)
        assert plugin.quarantines == [], name
        _push(plugin, __test_helper_view(size=0.05, seq=41))
        __test_helper_partial_settle(plugin, clock, size=0.05, seq=41)
        assert plugin.quarantines == [], name
        assert __test_helper_partial_verdicts(plugin) == ['foreign_only'], name


def __test_partial_tpsl_order_ahead_of_the_view_of_an_own_fill_is_not_reported__(tmp_path,
                                                                                monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    # The run holds 0.02 beside someone else's 0.01. The run adds 0.01; the
    # other party protects its own 0.01 before the view of the add arrives.
    plugin = __test_helper_partial_plugin(tmp_path, "race-own-add")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=41)))
    _push(plugin, __test_helper_view(size=0.03, seq=41))
    __test_helper_own_entry_fill(plugin, 'entry-2', qty='0.01', seq=50,
                                 exec_ms=1_752_600_000_000)
    _push(plugin, {'topic': 'order', 'data': [
        __test_helper_partial_tpsl_order('p-1', qty='0.01')]})
    assert plugin.quarantines == []
    __test_helper_partial_settle(plugin, clock, size=0.04, seq=50)
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['foreign_only']

    # The run's own TP takes 0.01 of its 0.02; the view of the reduce comes
    # ahead of its execution, and the other party protects its own 0.01.
    plugin = __test_helper_partial_plugin(tmp_path, "race-own-reduce")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=41)))
    _push(plugin, __test_helper_view(size=0.03, seq=41))
    __test_helper_own_exit_row(plugin, 'tp-1', qty=0.01)
    _push(plugin, __test_helper_view(size=0.02, seq=50))
    _push(plugin, {'topic': 'order', 'data': [
        __test_helper_partial_tpsl_order('p-1', qty='0.01')]})
    clock[0] += plugin.fill_report_grace_s / 2
    _push(plugin, _exec_push(__test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.01',
                                                    seq=50)))
    assert plugin.quarantines == []
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=50)
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['foreign_only']


def __test_partial_tpsl_order_withdrawn_before_its_verdict_is_logged__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_partial_plugin(tmp_path, "partial-withdrawn")
    order = __test_helper_partial_tpsl_order('p-1')
    _push(plugin, {'topic': 'order', 'data': [order]})
    _push(plugin, {'topic': 'order', 'data': [{**order, 'orderStatus': 'Deactivated'}]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert plugin.quarantines == []
    [payload] = __test_helper_audits(plugin, 'foreign_tpsl_order_judged')
    assert (payload['verdict'], payload['withdrawn']) == ('unproven', 'Deactivated')

    # One that fired is the position ledger's to judge.
    fired = __test_helper_partial_tpsl_order('p-2')
    _push(plugin, {'topic': 'order', 'data': [fired]})
    _push(plugin, {'topic': 'order', 'data': [{**fired, 'orderStatus': 'Triggered'}]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert len(__test_helper_audits(plugin, 'foreign_tpsl_order_judged')) == 1

    # A complete snapshot read after the order was seen, without it: gone.
    gone = __test_helper_partial_tpsl_order('p-3')
    _push(plugin, {'topic': 'order', 'data': [gone]})
    plugin._observe_open_order_levels([], plugin._market, read_started_at=clock[0] - 1.0)
    assert 'p-3' in plugin._pending_tpsl_orders, "a read started before may predate it"
    plugin._observe_open_order_levels([], plugin._market, read_started_at=clock[0])
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['unproven', 'unproven']


def __test_partial_tpsl_order_judged_unproven_without_evidence__(tmp_path, monkeypatch):
    clock = __test_helper_clock(monkeypatch)
    # The private stream's outage is not re-read yet: nothing proves the leg.
    plugin = __test_helper_partial_plugin(tmp_path, "partial-outage")
    plugin._ledger_stream_opened_ms = 1
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order('p-1')]})
    __test_helper_partial_settle(plugin, clock, size=0.02, seq=40)
    assert __test_helper_partial_verdicts(plugin) == []
    clock[0] += 300.0
    _push(plugin, __test_helper_view(size=0.02, seq=40))
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['unproven']

    # The foreign exposure beside the run's share is in doubt: an order
    # covering more than the proven part of it may still cover it only.
    plugin = __test_helper_partial_plugin(tmp_path, "partial-doubt")
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=41)))
    _push(plugin, __test_helper_view(size=0.05, seq=41))
    plugin._leg_ledgers[0].doubt = 0.02
    _push(plugin, {'topic': 'order', 'data': [
        __test_helper_partial_tpsl_order('p-1', qty='0.04')]})
    __test_helper_partial_settle(plugin, clock, size=0.05, seq=41)
    assert plugin.quarantines == []
    assert __test_helper_partial_verdicts(plugin) == ['unproven']


def __test_foreign_partial_tpsl_order_without_evidence_is_logged__(tmp_path):
    # A restart: the run's share was not started by a fill of this process.
    plugin = _manual_plugin(tmp_path, "partial-unproven")
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order('p-1')]})
    assert plugin.quarantines == []
    verdicts = [payload['verdict'] for payload
                in __test_helper_audits(plugin, 'foreign_tpsl_order_judged')]
    assert verdicts == ['unproven']


def __test_foreign_partial_tpsl_order_added_after_a_restart_reports__(tmp_path, monkeypatch):
    # The run's 0.02 share started before a restart; the adoption baseline of
    # the new process proves the run held it from the venue time it read on.
    clock = __test_helper_clock(monkeypatch)
    plugin = __test_helper_partial_plugin(tmp_path, "partial-restart")
    fresh = _restarted(plugin)
    floor_ms = 1_752_600_000_000
    rows = __test_helper_view(size=0.02, seq=60)['data']

    async def venue_time() -> int:
        return floor_ms

    async def lookup(_market, coid):
        return {'orderId': f"venue-{coid}", 'cumExecQty': '0.02'}, True

    async def fills(_market, _order_id, _from_ms, until_ms=None):
        assert until_ms == floor_ms
        return {'own-entry-1'}, 0.02, True, []

    async def position_rows(_market):
        return [dict(row) for row in rows]

    monkeypatch.setattr(fresh, '_venue_time_ms', venue_time)
    monkeypatch.setattr(fresh, '_confirm_lookup', lookup)
    monkeypatch.setattr(fresh, '_recovery_fill_ids', fills)
    monkeypatch.setattr(fresh, '_fetch_position_rows', position_rows)
    asyncio.run(fresh._apply_adoption_baseline(fresh._market, rows))
    assert fresh._adoption_baselined and fresh._share_start == {}

    # Created while the bot was down: it may have been there before the
    # run's fill or added since; not reported.
    _push(fresh, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-8', created_ms=floor_ms - 1_000)]})
    assert fresh.quarantines == []
    assert __test_helper_partial_verdicts(fresh) == ['unproven']

    # Created after the baseline: added to the position the run held.
    _push(fresh, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-9', created_ms=floor_ms + 1_000)]})
    assert fresh.quarantines == [], "judged once the evidence covers its creation"
    __test_helper_partial_settle(fresh, clock, size=0.02, seq=60)
    assert _kinds(fresh) == [QuarantineKind.EXTERNAL_INTERVENTION]
    _, context, _ = fresh.quarantines[0]
    assert (context['order_id'], context['foreign_exposure']) == ('p-9', 0.0)
    assert __test_helper_partial_verdicts(fresh) == ['unproven', 'reported']


def __test_share_held_since_ends_with_the_share__(tmp_path):
    plugin = __test_helper_partial_plugin(tmp_path, "held-since")
    fresh = _restarted(plugin)
    fresh._adoption_baselined = True
    fresh._note_share_held_since(1_752_600_000_000, fresh._market)
    assert fresh._share_held_since == {0: 1_752_600_000_000}
    # The run's own TP closes its share: a later share is a new one.
    __test_helper_own_exit_row(fresh, 'tp-1', qty=0.02)
    _push(fresh, _exec_push(__test_helper_own_exec('tp-1', exec_id='tp-a', qty='0.02',
                                                   seq=70)))
    assert fresh._run_owned_signed_size() == 0.0
    assert fresh._share_held_since == {}
    fresh._note_share_held_since(1_752_600_100_000, fresh._market)
    assert fresh._share_held_since == {}, "nothing held, nothing proven"


def __test_share_start_survives_a_same_side_add_without_seq__(tmp_path):
    plugin = __test_helper_partial_plugin(tmp_path, "start-kept")
    assert plugin._share_start == {0: (40, 1_752_599_990_000)}
    # A later own add on the same side carries no seq: the run's share
    # started with the first fill all the same.
    plugin.store_ctx.upsert_order(
        'entry-2', symbol='BTCUSDT', side='buy', qty=0.01, filled_qty=0.0,
        state='confirmed', intent_key='Long-entry-2', pine_entry_id='Long',
        exchange_order_id='venue-entry-2', extras={'kind': ENTRY_KIND_POSITION},
    )
    _push(plugin, _exec_push(_close_exec(
        execId='own-entry-2', orderId='venue-entry-2', orderLinkId='entry-2', side='Buy',
        execQty='0.01', closedSize='0', createType='CreateByUser', seq='')))
    assert plugin._run_owned_signed_size() == pytest.approx(0.03)
    assert plugin._share_start == {0: (40, 1_752_599_990_000)}
    _push(plugin, {'topic': 'order', 'data': [__test_helper_partial_tpsl_order(
        'p-1', created_ms=1_752_599_980_000)]})
    verdicts = [payload['verdict'] for payload
                in __test_helper_audits(plugin, 'foreign_tpsl_order_judged')]
    assert verdicts == ['preexisting']


# === Restart: an own trading stop that fired while the bot was down =========

def __test_helper_stop_fired_while_down(tmp_path, name: str, monkeypatch, *,
                                        mode: str = POSITION_MODE_ONE_WAY,
                                        foreign_qty: str = '0.002',
                                        fired: dict | None = None):
    """The run's 0.002 long carries its own trailing stop; the bot goes down.

    While it is down the stop fires (a coid-less venue execution no journal
    row names) and someone else opens ``foreign_qty`` on the same leg. The
    restarted process runs its adoption baseline over that venue. ``fired``
    replaces the stop's execution with another one closing the run's long.

    :return: The restarted plugin and the execution rows the adoption read
        down to its floor.
    """
    idx = 1 if mode == POSITION_MODE_HEDGE else 0
    plugin = _InterventionFake(responses=[{}], mode=mode)
    _attach_store(plugin, tmp_path, name)
    plugin._adoption_baselined = True
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.002', seq=40)
    _push(plugin, __test_helper_view(idx=idx, size=0.002, seq=40))
    envelope = _trail_envelope()
    envelope = DispatchEnvelope(
        intent=ExitIntent(**{**envelope.intent.__dict__, 'symbol': 'BTCUSDT.P'}),
        run_tag=envelope.run_tag, bar_ts_ms=envelope.bar_ts_ms,
        coid_max_len=envelope.coid_max_len,
    )
    if mode == POSITION_MODE_ONE_WAY:
        asyncio.run(plugin.execute_exit(envelope))
        assert plugin._protection_guard().known('trading-stop:0')
    else:
        create_bracket_ownership_row(
            plugin.store_ctx, coid='own-hedge-1', symbol='BTCUSDT.P', side='sell',
            qty=0.002, intent_key='Trail-X', pine_entry_id='Trail-X', from_entry='Long',
            leg_id=str(idx), attach_coid='attach-hedge-1', tp_price=110000.0,
            sl_price=90000.0, trail_price=105000.0, trail_offset=50.0,
            oca_name=None, oca_type=None,
        )
    attached_ms = int(plugin.store_ctx.get_order(
        next(row.client_order_id for row in plugin.store_ctx.iter_live_orders()
             if row.state == 'bracket_own')).created_ts_ms)

    if fired is None:
        fired = __test_helper_stop_fire('ts-fire', 'TrailingStop', qty='0.002', seq='50')
    fired['execTime'] = str(attached_ms + 1_000)
    foreign = __test_helper_foreign_open('f-open', qty=foreign_qty, seq=60)
    foreign['execTime'] = str(attached_ms + 2_000)
    floor_ms = attached_ms + 10_000
    rows = __test_helper_view(idx=idx, size=float(foreign_qty), seq=60)['data']
    if mode == POSITION_MODE_HEDGE:
        rows = rows + __test_helper_view(idx=2, size=0, seq=60)['data']
    executions = [fired, foreign]
    reads = []

    fresh = _restarted(plugin, mode=mode)

    async def venue_time() -> int:
        return floor_ms

    async def lookup(_market, coid):
        return {'orderId': f"venue-{coid}", 'cumExecQty': '0.002'}, True

    async def fills(_market, order_id, _from_ms, until_ms=None):
        assert until_ms == floor_ms
        assert order_id == 'venue-entry-1'
        return {'own-entry-1'}, 0.002, True, []

    async def position_rows(_market):
        return [dict(row) for row in rows]

    async def window(_market, start_ms, end_ms):
        reads.append((start_ms, end_ms))
        return [dict(entry) for entry in executions
                if start_ms <= int(entry['execTime']) <= end_ms], True

    monkeypatch.setattr(fresh, '_venue_time_ms', venue_time)
    monkeypatch.setattr(fresh, '_confirm_lookup', lookup)
    monkeypatch.setattr(fresh, '_recovery_fill_ids', fills)
    monkeypatch.setattr(fresh, '_fetch_position_rows', position_rows)
    monkeypatch.setattr(fresh, '_drain_deriv_exec_window', window)
    asyncio.run(fresh._apply_adoption_baseline(fresh._market, rows))
    assert fresh._adoption_baselined
    assert reads and reads[-1][1] == floor_ms and reads[0][0] <= int(fired['execTime'])
    return fresh, idx


def __test_own_stop_fired_while_down_is_booked_at_adoption__(tmp_path, monkeypatch):
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        fresh, idx = __test_helper_stop_fired_while_down(
            tmp_path, f"down-fire-{mode}", monkeypatch, mode=mode)
        # The stop closed the run's share; the leg now holds only the other
        # party's position.
        assert fresh._journal_leg_shares().get(idx, 0.0) == pytest.approx(0.0), mode
        assert not fresh._protection_guard().known(f"trading-stop:{idx}"), mode
        audits = __test_helper_audits(fresh, 'venue_bracket_fill_attributed')
        assert [(payload['exec_id'], payload['claim_qty']) for payload in audits] \
            == [('ts-fire', pytest.approx(0.002))], mode
        # The other party's bare position and its own close are not the bot's.
        bare = __test_helper_view(idx=idx, size=0.002, seq=60)
        _push(fresh, bare)
        _push(fresh, bare)
        _push(fresh, _exec_push(__test_helper_foreign_close('f-close', qty='0.002', seq=70)))
        _push(fresh, __test_helper_view(idx=idx, size=0, seq=70))
        assert fresh.quarantines == [], mode


def __test_own_stop_fired_while_down_is_booked_once__(tmp_path, monkeypatch):
    fresh, _idx = __test_helper_stop_fired_while_down(tmp_path, "down-fire-once", monkeypatch)
    # A replayed push of the same execution changes nothing.
    events = _push(fresh, _exec_push(__test_helper_stop_fire(
        'ts-fire', 'TrailingStop', qty='0.002', seq='50')))
    assert events == []
    assert len(__test_helper_audits(fresh, 'venue_bracket_fill_attributed')) == 1



def __test_helper_closeout(exec_type: str, create_type: str) -> dict:
    """The venue closing the run's 0.002 long on its own: no link id, no stop type."""
    return _close_exec(execId='closeout-1', orderId='order-closeout-1', execType=exec_type,
                       execQty='0.002', closedSize='0.002', createType=create_type, seq='50')


@pytest.mark.parametrize('exec_type, create_type', [
    ('Trade', 'CreateByLiq'), ('Trade', 'CreateByAdl_PassThrough'),
    ('Trade', 'CreateByTakeOver_PassThrough'), ('Trade', 'CreateByAdminClosing'),
    ('AdlTrade', ''), ('BustTrade', ''), ('Delivery', ''),
])
def __test_venue_closeout_while_down_retires_the_run_share__(
        tmp_path, monkeypatch, caplog, exec_type, create_type):
    """A liquidation (or ADL, admin close, delivery) while the bot is down
    names no leg size: the run's share on that side is taken as closed,
    logged ERROR and audited. The other party who reopened the leg keeps
    theirs, and its later close is no intervention."""
    for mode in (POSITION_MODE_ONE_WAY, POSITION_MODE_HEDGE):
        with caplog.at_level(logging.ERROR, logger='pynecore_bybit.positions'):
            fresh, idx = __test_helper_stop_fired_while_down(
                tmp_path, f"closeout-{exec_type}-{create_type}-{mode}", monkeypatch, mode=mode,
                fired=__test_helper_closeout(exec_type, create_type))
        assert fresh._journal_leg_shares().get(idx, 0.0) == pytest.approx(0.0), mode
        audits = __test_helper_audits(fresh, 'venue_closeout_while_down')
        assert [(payload['exec_id'], payload['retired_qty']) for payload in audits] \
            == [('closeout-1', pytest.approx(0.002))], mode
        assert any('closed the bot' in record.getMessage() for record in caplog.records)
        assert not fresh._protection_guard().known(f"trading-stop:{idx}"), mode
        bare = __test_helper_view(idx=idx, size=0.002, seq=60)
        _push(fresh, bare)
        _push(fresh, bare)
        _push(fresh, _exec_push(__test_helper_foreign_close('f-close', qty='0.002', seq=70)))
        _push(fresh, __test_helper_view(idx=idx, size=0, seq=70))
        assert fresh.quarantines == [], mode


def __test_venue_closeout_while_down_is_retired_once__(tmp_path, monkeypatch):
    """The audit names the execution: an older entry's remainder that filled
    after the restart is not retired again by the same closeout at the next
    start."""
    closeout = __test_helper_closeout('Trade', 'CreateByLiq')
    fresh, _idx = __test_helper_stop_fired_while_down(
        tmp_path, "closeout-once", monkeypatch, fired=closeout)
    at = int(closeout['execTime'])
    __test_helper_own_entry_fill(fresh, 'entry-2', qty='0.002', seq=80, exec_ms=at + 20_000)
    with sqlite3.connect(tmp_path / "closeout-once.sqlite") as db:
        db.execute("UPDATE orders SET created_ts_ms = ? WHERE client_order_id = 'entry-2'",
                   (at - 5_000,))
    assert asyncio.run(fresh._unbooked_venue_closeouts(fresh._market, at + 10_000)) == []


def __test_user_close_while_down_is_no_venue_closeout__(tmp_path, monkeypatch):
    """A user's close is not the venue's mechanics: never taken as a closeout."""
    entry = __test_helper_closeout('Trade', 'CreateByUser')
    plugin = _InterventionFake(responses=[{}])
    _attach_store(plugin, tmp_path, "closeout-user")
    __test_helper_own_entry_fill(plugin, 'entry-1', qty='0.002', seq=40)
    rows = list(plugin.store_ctx.iter_live_orders())
    entry['execTime'] = str(max(row.created_ts_ms for row in rows) + 1_000)
    assert not plugin._venue_closeout_execution(entry, plugin._market, rows)
    assert plugin._venue_closeout_execution(
        dict(entry, createType='CreateByLiq'), plugin._market, rows)
    # A closeout older than every entry row closed someone else's exposure.
    older = str(min(row.created_ts_ms for row in rows) - 120_000)
    assert not plugin._venue_closeout_execution(
        dict(entry, createType='CreateByLiq', execTime=older), plugin._market, rows)

def __test_stop_fire_after_the_run_share_is_gone_books_no_phantom_fill__(tmp_path):
    for name, legs in (('single', ('L-X',)), ('pyramid', ('L-X', 'L-Y'))):
        plugin = _manual_plugin(tmp_path, f"phantom-{name}")
        for leg in legs:
            create_bracket_ownership_row(
                plugin.store_ctx, coid=f"bo:{leg}\x00Long:0", symbol='BTCUSDT.P',
                side='sell', qty=0.01, intent_key=f"{leg}\x00Long", pine_entry_id=leg,
                from_entry='Long', leg_id='0', attach_coid=f"attach-{leg}",
                tp_price=None, sl_price=29000.0, trail_price=None, trail_offset=None,
            )
        # The run's own close takes its whole share; its stop is still recorded.
        __test_helper_own_exit_row(plugin, 'close-1', qty=0.02)
        _push(plugin, _exec_push(__test_helper_own_exec('close-1', exec_id='c-1',
                                                        qty='0.02', seq=60)))
        _push(plugin, __test_helper_view(size=0, seq=60))
        assert plugin._run_owned_signed_size() == 0.0, name
        # Someone else opens on the leg, and a Full stop closes that position.
        _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.01', seq=70)))
        _push(plugin, __test_helper_view(size=0.01, seq=70))
        events = _push(plugin, _exec_push(_close_exec(
            execId='vs-1', orderId='venue-stop', side='Sell', execQty='0.01',
            closedSize='0.01', stopOrderType='StopLoss', createType='CreateByStopLoss',
            seq='80')))
        assert events == [], name
        assert __test_helper_audits(plugin, 'venue_bracket_fill_attributed') == [], name
        _push(plugin, __test_helper_view(size=0, seq=80))
        assert plugin.quarantines == [], name
        assert plugin._run_owned_signed_size() == 0.0, name


def __test_later_stop_execution_after_the_run_share_is_claimed_books_no_phantom_fill__(
        tmp_path):
    plugin = _manual_plugin(tmp_path, "phantom-split")
    for leg in ('L-X', 'L-Y'):
        create_bracket_ownership_row(
            plugin.store_ctx, coid=f"bo:{leg}\x00Long:0", symbol='BTCUSDT.P', side='sell',
            qty=0.01, intent_key=f"{leg}\x00Long", pine_entry_id=leg, from_entry='Long',
            leg_id='0', attach_coid=f"attach-{leg}", tp_price=None, sl_price=29000.0,
            trail_price=None, trail_offset=None,
        )
    # An own close leaves the run 0.01 of its pyramid; someone else adds 0.03.
    __test_helper_own_exit_row(plugin, 'close-1', qty=0.01)
    _push(plugin, _exec_push(__test_helper_own_exec('close-1', exec_id='c-1',
                                                    qty='0.01', seq=60)))
    _push(plugin, __test_helper_view(size=0.01, seq=60))
    _push(plugin, _exec_push(__test_helper_foreign_open('f-1', qty='0.03', seq=70)))
    _push(plugin, __test_helper_view(size=0.04, seq=70))
    # The Full stop closes the merged 0.04 in two executions: the first takes
    # the run's 0.01, the second only the other party's exposure.
    booked = []
    for exec_id, seq in (('vs-1', '80'), ('vs-2', '81')):
        booked.extend(_push(plugin, _exec_push(_close_exec(
            execId=exec_id, orderId='venue-stop', side='Sell', execQty='0.02',
            closedSize='0.02', stopOrderType='StopLoss', createType='CreateByStopLoss',
            seq=seq))))
    assert sum(event.fill_qty for event in booked) == pytest.approx(0.01)
    _push(plugin, __test_helper_view(size=0, seq=81))
    assert plugin.quarantines == []
    assert plugin._run_owned_signed_size() == 0.0
