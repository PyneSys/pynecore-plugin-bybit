"""Opt-in offline conformance scenarios for the Bybit broker plugin."""

import asyncio
from dataclasses import replace
from decimal import Decimal, ROUND_DOWN
import logging
from time import time as epoch_time
from typing import Any

from pynecore.core.broker.exceptions import OrderSkippedByPlugin
from pynecore.core.broker.models import LegType, OrderStatus
from pynecore.testing.broker_lab import Scenario, Step, pairwise_cases
from pynecore.testing.broker_lab.reference import (
    ReferenceVenueProfile,
    VenueOrder,
)
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, BybitConnectionError
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import (
    HEDGE_IDX_BUY,
    HEDGE_IDX_SELL,
    POSITION_MODE_HEDGE,
    POSITION_MODE_ONE_WAY,
)


class _WarningCollector(logging.Handler):
    """Collect broker warnings without changing their normal output path."""

    def __init__(self) -> None:
        super().__init__(logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            self.messages.append(record.getMessage())


def _linear_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category="linear",
        symbol="BTCUSDT",
        base_coin="BTC",
        quote_coin="USDT",
        settle_coin="USDT",
        status="Trading",
        tick_size_str="0.10",
        tick_size=0.1,
        qty_step_str="0.001",
        qty_step=0.001,
        min_order_qty=0.001,
        min_order_amt=0.0,
        min_notional=5.0,
        max_limit_order_qty=1500.0,
        max_market_order_qty=150.0,
        contract_type="LinearPerpetual",
        delivery_time=None,
    )


def _usdc_linear_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category="linear",
        symbol="ETHPERP",
        base_coin="ETH",
        quote_coin="USDC",
        settle_coin="USDC",
        status="Trading",
        tick_size_str="0.01",
        tick_size=0.01,
        qty_step_str="0.01",
        qty_step=0.01,
        min_order_qty=0.01,
        min_order_amt=0.0,
        min_notional=5.0,
        max_limit_order_qty=2500.0,
        max_market_order_qty=500.0,
        contract_type="LinearPerpetual",
        delivery_time=None,
    )


def _spot_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category="spot",
        symbol="BTCUSDT",
        base_coin="BTC",
        quote_coin="USDT",
        settle_coin="",
        status="Trading",
        tick_size_str="0.01",
        tick_size=0.01,
        qty_step_str="0.000001",
        qty_step=0.000001,
        min_order_qty=0.0,
        min_order_amt=5.0,
        min_notional=0.0,
        max_limit_order_qty=100.0,
        max_market_order_qty=50.0,
        contract_type="",
        delivery_time=None,
    )


def _usdc_spot_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category="spot",
        symbol="ETHUSDC",
        base_coin="ETH",
        quote_coin="USDC",
        settle_coin="",
        status="Trading",
        tick_size_str="0.01",
        tick_size=0.01,
        qty_step_str="0.00001",
        qty_step=0.00001,
        min_order_qty=0.00001,
        min_order_amt=5.0,
        min_notional=0.0,
        max_limit_order_qty=1500.0,
        max_market_order_qty=730.0,
        contract_type="",
        delivery_time=None,
    )


def _inverse_instrument() -> InstrumentInfo:
    return InstrumentInfo(
        category="inverse",
        symbol="BTCUSD",
        base_coin="BTC",
        quote_coin="USD",
        settle_coin="BTC",
        status="Trading",
        tick_size_str="0.10",
        tick_size=0.1,
        qty_step_str="1",
        qty_step=1.0,
        min_order_qty=1.0,
        min_order_amt=0.0,
        min_notional=5.0,
        max_limit_order_qty=1_000_000.0,
        max_market_order_qty=1_000_000.0,
        contract_type="InversePerpetual",
        delivery_time=None,
    )


class OfflineBybit(Bybit):
    """Real Bybit execution code with an in-memory REST transport."""

    def __init__(self, profile: "BybitProfile", run_name: str, store_ctx: Any) -> None:
        super().__init__(
            symbol=profile.symbol,
            timeframe=profile.timeframe,
            config=BybitConfig(),
        )
        self.profile = profile
        self.run_name = run_name
        self.store_ctx = store_ctx
        self._market = profile.market
        self._broker_started = True
        self._position_mode = POSITION_MODE_ONE_WAY
        self.rest_calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "get",
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        del method, auth
        payload = dict(body or {})
        self.rest_calls.append((endpoint, payload))
        self.profile.transport_calls.append((endpoint, payload))
        if endpoint == "/v5/order/create":
            order_id = self.profile.state.new_id()
            coid = str(payload.get("orderLinkId") or "")
            self.profile.raw_orders[order_id] = {
                "orderId": order_id,
                "symbol": payload["symbol"],
                "side": payload["side"],
                "orderType": payload["orderType"],
                "qty": payload["qty"],
                "price": payload.get("price", ""),
                "triggerPrice": payload.get("triggerPrice", ""),
                "cumExecQty": "0",
                "avgPrice": "",
                "orderStatus": "New",
                "reduceOnly": payload.get("reduceOnly", False),
                "orderLinkId": payload.get("orderLinkId", ""),
                "createdTime": "1700000000000",
            }
            self.profile.transport_barriers.append(("write_emitted", coid))
            if self.profile.drop_create_ack:
                self.profile.drop_create_ack = False
                self.profile.transport_barriers.append(("ack_suppressed", coid))
                raise self.profile.post_write_error(
                    "injected Bybit post-write/pre-ACK transport loss"
                )
            self.profile.transport_barriers.append(("ack_received", coid))
            return {"orderId": order_id, "orderLinkId": payload.get("orderLinkId")}
        if endpoint == "/v5/order/amend":
            coid = payload["orderLinkId"]
            raw = next(
                order
                for order in self.profile.raw_orders.values()
                if order["orderLinkId"] == coid
            )
            raw["qty"] = payload.get("qty", raw["qty"])
            raw["price"] = payload.get("price", raw["price"])
            raw["triggerPrice"] = payload.get("triggerPrice", raw["triggerPrice"])
            return {"orderId": raw["orderId"], "orderLinkId": coid}
        if endpoint == "/v5/order/cancel":
            coid = payload.get("orderLinkId")
            order_id = payload.get("orderId")
            raw = next(
                order
                for order in self.profile.raw_orders.values()
                if (coid is not None and order["orderLinkId"] == coid)
                or (order_id is not None and order["orderId"] == order_id)
            )
            if raw["orderStatus"] != "Filled":
                raw["orderStatus"] = "Cancelled"
            return {"orderId": raw["orderId"], "orderLinkId": raw["orderLinkId"]}
        if endpoint == "/v5/order/realtime":
            return {
                "list": [
                    order
                    for order in self.profile.raw_orders.values()
                    if order["orderStatus"] not in ("Cancelled", "Filled")
                ],
                "nextPageCursor": "",
            }
        if endpoint == "/v5/order/history":
            return {
                "list": list(self.profile.raw_orders.values()),
                "nextPageCursor": "",
            }
        if endpoint == "/v5/position/list":
            size = self.profile.state.position
            return {
                "list": [
                    {
                        "symbol": self.profile.symbol,
                        "positionIdx": 0,
                        "size": str(abs(size)),
                        "side": "Buy" if size >= 0 else "Sell",
                        "avgPrice": "100",
                        "unrealisedPnl": "0",
                        "liqPrice": "",
                        "leverage": "1",
                        "tradeMode": 0,
                        "updatedTime": "1700000000000",
                    }
                ]
            }
        if endpoint == "/v5/market/time":
            return {
                "timeSecond": "1700000000",
                "timeNano": "1700000000000000000",
            }
        if endpoint == "/v5/execution/list":
            order_id = str((params or {}).get("orderId") or "")
            return {
                "list": [
                    execution
                    for execution in self.profile.raw_executions
                    if not order_id or execution["orderId"] == order_id
                ],
                "nextPageCursor": "",
            }
        raise AssertionError(f"unexpected offline Bybit REST call: {endpoint} {params}")

    async def execute_entry(self, envelope):
        self.profile.entry_attempts.append(envelope.intent.intent_key)
        try:
            orders = await super().execute_entry(envelope)
        except OrderSkippedByPlugin as exc:
            self.profile.structured_skips.append(
                {
                    "intent_key": exc.intent_key,
                    "reason": exc.reason,
                    "context": dict(exc.context),
                }
            )
            raise
        for order in orders:
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=LegType.ENTRY,
                intent_key=envelope.intent.intent_key,
                from_entry=None,
            )
        self.profile.state.calls.append(
            (self.run_name, "entry", envelope.intent.intent_key)
        )
        return orders

    async def execute_exit(self, envelope):
        orders = await super().execute_exit(envelope)
        self._mirror_exit_orders(envelope, orders)
        self.profile.state.calls.append(
            (self.run_name, "exit", envelope.intent.intent_key)
        )
        return orders

    def _mirror_exit_orders(self, envelope, orders):
        for order in orders:
            leg_type = (
                LegType.STOP_LOSS
                if order.stop_price is not None
                else LegType.TAKE_PROFIT
            )
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=leg_type,
                intent_key=envelope.intent.intent_key,
                from_entry=envelope.intent.from_entry,
            )

    async def modify_exit(self, old, new):
        orders = await super().modify_exit(old, new)
        self._mirror_exit_orders(new, orders)
        return orders

    async def execute_close(self, envelope):
        order = await super().execute_close(envelope)
        self.profile.state.orders[order.id] = VenueOrder(
            order=order,
            run_name=self.run_name,
            pine_id=envelope.intent.pine_id,
            leg_type=LegType.CLOSE,
            intent_key=envelope.intent.intent_key,
            from_entry=envelope.intent.pine_id,
        )
        self.profile.state.calls.append(
            (self.run_name, "close", envelope.intent.intent_key)
        )
        return order

    async def execute_cancel(self, envelope):
        result = await super().execute_cancel(envelope)
        intent = envelope.intent
        for record in self.profile.state.orders.values():
            if (
                record.run_name == self.run_name
                and record.pine_id == intent.pine_id
                and (
                    intent.from_entry is None or record.from_entry == intent.from_entry
                )
                and record.order.status
                in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
            ):
                record.order = replace(
                    record.order,
                    status=OrderStatus.CANCELLED,
                    remaining_qty=0.0,
                )
        return result


class AliasingOfflineBybit(OfflineBybit):
    """Broken transport control that aliases a second bracket onto the first."""

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "get",
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        payload = dict(body or {})
        if (
            endpoint == "/v5/order/create"
            and payload.get("reduceOnly") is True
            and (
                payload.get("orderType") != "Market"
                or bool(payload.get("triggerPrice"))
            )
        ):
            aliased = next(
                (
                    order
                    for order in self.profile.raw_orders.values()
                    if order.get("reduceOnly") is True
                    and order["side"] == payload["side"]
                    and order["orderType"] == payload["orderType"]
                    and order.get("price", "") == payload.get("price", "")
                    and order.get("triggerPrice", "")
                    == payload.get("triggerPrice", "")
                ),
                None,
            )
            if aliased is not None:
                self.rest_calls.append((endpoint, payload))
                self.profile.transport_calls.append((endpoint, payload))
                return {
                    "orderId": aliased["orderId"],
                    "orderLinkId": payload.get("orderLinkId"),
                }
        return super().__call__(
            endpoint,
            params,
            method=method,
            body=body,
            auth=auth,
        )


class BybitProfile(ReferenceVenueProfile):
    """One-way linear venue semantics around the real Bybit plugin."""

    plugin_name = "bybit-offline-lab"
    symbol = "BTCUSDT"
    quantity_step = 0.001

    def __init__(self) -> None:
        super().__init__()
        self.market = _linear_instrument()
        self.transport_calls: list[tuple[str, dict[str, Any]]] = []
        self.raw_orders: dict[str, dict[str, Any]] = {}
        self.raw_executions: list[dict[str, Any]] = []
        self.entry_attempts: list[str] = []
        self.structured_skips: list[dict[str, Any]] = []
        self.drop_create_ack = False
        self.transport_barriers: list[tuple[str, str]] = []
        self.warning_collector = _WarningCollector()
        logging.getLogger("pyne_core_logger").addHandler(self.warning_collector)

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineBybit:
        return OfflineBybit(self, run_name, store_ctx)

    @staticmethod
    def post_write_error(message: str) -> Exception:
        return BybitConnectionError(message)

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "set_bybit_last_price":
            runner.runs[step.run].broker._last_price = float(step.values["price"])
            return True
        if step.kind == "drop_next_bybit_create_ack":
            self.drop_create_ack = True
            return True
        if step.kind == "expect_bybit_post_write_ack_loss":
            suppressed = [
                coid for event, coid in self.transport_barriers
                if event == "ack_suppressed"
            ]
            if len(suppressed) != 1:
                raise AssertionError(
                    f"expected one suppressed Bybit create ACK, got {suppressed}"
                )
            coid = suppressed[0]
            events = [
                event for event, event_coid in self.transport_barriers
                if event_coid == coid
            ]
            if events != ["write_emitted", "ack_suppressed"]:
                raise AssertionError(
                    "post-write/pre-ACK seam not proven: "
                    f"expected write then suppressed ACK, got {events}"
                )
            if not any(
                raw.get("orderLinkId") == coid for raw in self.raw_orders.values()
            ):
                raise AssertionError(
                    "post-write/pre-ACK seam not proven: venue order is absent"
                )
            pending = runner.runs[step.run].engine.pending_verification
            if coid not in pending:
                raise AssertionError(
                    f"lost-ACK order {coid!r} was not parked for verification"
                )
            return True
        if step.kind == "expect_bybit_no_pending_verification":
            pending = runner.runs[step.run].engine.pending_verification
            if pending:
                raise AssertionError(
                    f"expected no pending Bybit dispatch verification, got {pending}"
                )
            return True
        if step.kind == "expect_bybit_request":
            requests = [
                body
                for endpoint, body in self.transport_calls
                if endpoint == "/v5/order/create"
            ]
            if not requests:
                raise AssertionError("Bybit did not issue an order request")
            body = requests[-1]
            for key, value in step.values.items():
                if body.get(key) != value:
                    raise AssertionError(
                        f"expected Bybit request {key}={value!r}, got {body.get(key)!r}"
                    )
            return True
        if step.kind == "expect_bybit_request_absent":
            requests = [
                body
                for endpoint, body in self.transport_calls
                if endpoint == "/v5/order/create"
            ]
            if not requests:
                raise AssertionError("Bybit did not issue an order request")
            body = requests[-1]
            unexpected = [key for key in step.values["keys"] if key in body]
            if unexpected:
                raise AssertionError(
                    f"expected Bybit request to omit {unexpected}, got {body}"
                )
            return True
        if step.kind == "expect_bybit_create_count":
            actual = sum(
                endpoint == "/v5/order/create" for endpoint, _ in self.transport_calls
            )
            expected = int(step.values["count"])
            if actual != expected:
                raise AssertionError(f"expected {expected} Bybit creates, got {actual}")
            return True
        if step.kind == "expect_bybit_raw_open_count":
            actual = sum(
                order["orderStatus"] not in ("Cancelled", "Filled")
                for order in self.raw_orders.values()
            )
            expected = int(step.values["count"])
            if actual != expected:
                live = [
                    order for order in self.raw_orders.values()
                    if order["orderStatus"] not in ("Cancelled", "Filled")
                ]
                raise AssertionError(
                    f"expected {expected} raw Bybit open orders, got {live}"
                )
            return True
        if step.kind == "expect_bybit_below_grid_skip":
            if len(self.entry_attempts) != 1:
                raise AssertionError(
                    f"expected one Bybit entry attempt, got {self.entry_attempts}"
                )
            if len(self.structured_skips) != 1:
                raise AssertionError(
                    f"expected one structured Bybit skip, got {self.structured_skips}"
                )
            skip = self.structured_skips[0]
            expected = {
                "intent_key": "tiny",
                "reason": "below_min_size",
                "context": {
                    "symbol": "BTCUSDT",
                    "qty": 0.0005,
                    "qty_step": "0.001",
                },
            }
            for key, value in expected.items():
                if skip.get(key) != value:
                    raise AssertionError(
                        f"expected structured skip {key}={value!r}, got {skip!r}"
                    )
            skip_warnings = [
                message
                for message in self.warning_collector.messages
                if "size 0.0005 quantizes to zero" in message
            ]
            if len(skip_warnings) != 1 or not skip_warnings[0].startswith("[BROKER]"):
                raise AssertionError(
                    "expected one operator-visible [BROKER] below-grid warning, "
                    f"got {skip_warnings}"
                )
            return True
        if step.kind == "expect_bybit_endpoint_count":
            endpoint = str(step.values["endpoint"])
            actual = sum(
                call_endpoint == endpoint for call_endpoint, _ in self.transport_calls
            )
            expected = int(step.values["count"])
            if actual != expected:
                raise AssertionError(
                    f"expected {expected} Bybit {endpoint} calls, got {actual}"
                )
            return True
        if step.kind == "expect_bybit_order_state":
            pine_id = str(step.values["id"])
            matching = [
                raw
                for raw in self.raw_orders.values()
                if any(
                    record.order.id == raw["orderId"] and record.pine_id == pine_id
                    for record in self.state.orders.values()
                )
            ]
            if len(matching) != 1:
                raise AssertionError(
                    f"expected one Bybit order for {pine_id!r}, got {matching}"
                )
            raw = matching[0]
            for key, value in step.values.items():
                if key == "id":
                    continue
                if raw.get(key) != value:
                    raise AssertionError(
                        f"expected Bybit {pine_id!r} {key}={value!r}, got {raw.get(key)!r}"
                    )
            return True
        if step.kind == "expect_bybit_bracket_outcome_swept":
            bracket = [
                order
                for order in self.raw_orders.values()
                if order.get("reduceOnly") is True
            ]
            statuses = sorted(order["orderStatus"] for order in bracket)
            if statuses != ["Cancelled", "Filled"]:
                raise AssertionError(
                    f"expected one filled Bybit leg and one cancelled sibling, got {statuses}"
                )
            return True
        if step.kind == "expect_bybit_active_bracket":
            expected_tp = float(step.values["tp"])
            expected_sl = float(step.values["sl"])
            records = [
                record
                for record in self.state.orders.values()
                if record.from_entry == step.values["from_entry"]
                and record.order.status is OrderStatus.OPEN
                and record.leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS)
            ]
            tp = [
                record for record in records if record.leg_type is LegType.TAKE_PROFIT
            ]
            sl = [record for record in records if record.leg_type is LegType.STOP_LOSS]
            if len(tp) != 1 or len(sl) != 1:
                raise AssertionError(f"expected one active TP and SL, got {records}")
            if (
                tp[0].order.price != expected_tp
                or sl[0].order.stop_price != expected_sl
            ):
                raise AssertionError(
                    f"expected Bybit bracket TP={expected_tp} SL={expected_sl}, "
                    f"got TP={tp[0].order.price} SL={sl[0].order.stop_price}"
                )
            return True
        if step.kind == "expect_bybit_bracket_coverage":
            expected_qty = float(step.values["qty"])
            records = [
                record
                for record in self.state.orders.values()
                if record.leg_type in (LegType.TAKE_PROFIT, LegType.STOP_LOSS)
            ]
            if len(records) != 4:
                raise AssertionError(
                    f"expected four physical Bybit bracket legs, got {len(records)}"
                )
            if len({record.order.id for record in records}) != 4:
                raise AssertionError(
                    "Bybit bracket legs do not have distinct physical identities"
                )
            if len({record.order.client_order_id for record in records}) != 4:
                raise AssertionError(
                    "Bybit bracket legs do not have distinct client order ids"
                )
            for from_entry in ("A", "B"):
                parent = [
                    record for record in records if record.from_entry == from_entry
                ]
                if {record.leg_type for record in parent} != {
                    LegType.TAKE_PROFIT,
                    LegType.STOP_LOSS,
                }:
                    raise AssertionError(
                        f"Bybit parent {from_entry!r} does not have distinct TP and SL legs"
                    )
                for outcome in (LegType.TAKE_PROFIT, LegType.STOP_LOSS):
                    coverage = sum(
                        record.order.remaining_qty
                        for record in parent
                        if record.leg_type is outcome
                    )
                    if abs(coverage - expected_qty) > 1e-9:
                        raise AssertionError(
                            f"Bybit {outcome.value} coverage for {from_entry!r} "
                            f"is {coverage}, expected {expected_qty}"
                        )
            return True
        if step.kind == "fill_bybit_bracket_leg":
            leg_type = (
                LegType.TAKE_PROFIT if step.values["leg"] == "tp" else LegType.STOP_LOSS
            )
            candidates = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run
                and record.leg_type is leg_type
                and record.order.status is OrderStatus.OPEN
            ]
            if len(candidates) != 1:
                raise AssertionError(
                    f"expected one open Bybit {step.values['leg']} leg, got {candidates}"
                )
            record = candidates[0]
            raw = self.raw_orders[record.order.id]
            raw["orderStatus"] = "Filled"
            raw["cumExecQty"] = raw["qty"]
            raw["avgPrice"] = str(step.values.get("price", 110.0))
            runtime = runner.runs[step.run]
            runtime.broker._translate_private_frame(
                {
                    "topic": "position",
                    "data": [
                        {
                            "category": "linear",
                            "symbol": self.symbol,
                            "positionIdx": 0,
                            "size": "0",
                            "side": "",
                            "updatedTime": "1700000002000",
                        }
                    ],
                },
                runtime.broker._market,
            )
            execution = {
                "category": "linear",
                "symbol": self.symbol,
                "execType": "Trade",
                "execId": f"exec-{record.order.id}",
                "orderLinkId": raw["orderLinkId"],
                "orderId": record.order.id,
                "side": raw["side"],
                "execQty": raw["qty"],
                "execPrice": raw["avgPrice"],
                "execFee": "0",
                "execTime": "1700000001000",
            }
            self.raw_executions.append(execution)
            events = runtime.broker._translate_private_frame(
                {
                    "topic": "execution",
                    "data": [execution],
                },
                runtime.broker._market,
            )
            if len(events) != 1:
                raise AssertionError(
                    f"Bybit private WS did not emit one fill: {events}"
                )
            record.order = events[0].order
            self.state.position_owners[step.run] = 0.0
            self.state.position = sum(self.state.position_owners.values())
            self.state.pending_events.append((step.run, events[0]))
            return True
        if step.kind in ("fill_bybit_entry", "fill_bybit_close"):
            pine_id = str(step.values["id"])
            leg_type = (
                LegType.ENTRY
                if step.kind == "fill_bybit_entry"
                else LegType.CLOSE
            )
            candidates = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run
                and record.pine_id == pine_id
                and record.leg_type is leg_type
                and record.order.status
                in (OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
            ]
            if len(candidates) != 1:
                raise AssertionError(
                    f"expected one open Bybit {leg_type.value} {pine_id!r}, "
                    f"got {candidates}"
                )
            record = candidates[0]
            raw = self.raw_orders[record.order.id]
            fill_qty = float(step.values.get("qty", record.order.remaining_qty))
            cumulative = float(raw["cumExecQty"]) + fill_qty
            raw["cumExecQty"] = str(cumulative)
            raw["avgPrice"] = str(step.values.get("price", 100.0))
            raw["orderStatus"] = (
                "Filled"
                if cumulative >= float(raw["qty"]) - 1e-12
                else "PartiallyFilled"
            )
            runtime = runner.runs[step.run]
            execution = {
                "category": runtime.broker._market.category,
                "symbol": self.symbol,
                "execType": "Trade",
                "execId": str(
                    step.values.get(
                        "fill_id", f"exec-{record.order.id}-{cumulative}"
                    )
                ),
                "orderLinkId": raw["orderLinkId"],
                "orderId": record.order.id,
                "side": raw["side"],
                "execQty": str(fill_qty),
                "execPrice": raw["avgPrice"],
                "execFee": "0",
                "execTime": "1700000001000",
            }
            self.raw_executions.append(execution)
            events = runtime.broker._translate_private_frame(
                {
                    "topic": "execution",
                    "data": [execution],
                },
                runtime.broker._market,
            )
            if len(events) != 1:
                raise AssertionError(
                    f"Bybit private WS did not emit one {leg_type.value} fill: {events}"
                )
            record.order = events[0].order
            signed = (
                events[0].fill_qty
                if record.order.side == "buy"
                else -events[0].fill_qty
            )
            self.state.position_owners[step.run] = (
                self.state.position_owners.get(step.run, 0.0) + signed
            )
            self.state.position = sum(self.state.position_owners.values())
            self.state.pending_events.append((step.run, events[0]))
            return True
        return super().handle_step(runner, step)

    def close(self) -> None:
        logging.getLogger("pyne_core_logger").removeHandler(self.warning_collector)
        super().close()

    def check_invariants(self, runner: Any):
        violations = list(super().check_invariants(runner))
        live_by_coid: dict[str, int] = {}
        for raw in self.raw_orders.values():
            if raw["orderStatus"] in ("Cancelled", "Filled"):
                continue
            coid = str(raw.get("orderLinkId") or "")
            live_by_coid[coid] = live_by_coid.get(coid, 0) + 1
        for coid, count in live_by_coid.items():
            if coid and count > 1:
                violations.append(
                    f"Bybit client-order-id idempotence violated for {coid}: "
                    f"{count} live physical orders"
                )
        live_by_intent_leg: dict[tuple[str, str, str, str], int] = {}
        for run_name, runtime in runner.runs.items():
            for raw in self.raw_orders.values():
                if raw["orderStatus"] in ("Cancelled", "Filled"):
                    continue
                row = runtime.store_ctx.get_order(str(raw.get("orderLinkId") or ""))
                if row is None or not row.intent_key:
                    continue
                extras = row.extras or {}
                key = (
                    run_name,
                    row.intent_key,
                    str(extras.get("kind") or ""),
                    str(extras.get("leg") or ""),
                )
                live_by_intent_leg[key] = live_by_intent_leg.get(key, 0) + 1
        for (run_name, intent_key, kind, leg), count in live_by_intent_leg.items():
            if count > 1:
                violations.append(
                    "Bybit exactly-once physical-order invariant violated for "
                    f"{run_name}/{intent_key}/{kind}/{leg}: {count} live orders"
                )
        for event, coid in self.transport_barriers:
            if event != "ack_suppressed":
                continue
            prior = []
            for barrier_event, barrier_coid in self.transport_barriers:
                if barrier_event == "ack_suppressed" and barrier_coid == coid:
                    break
                if barrier_coid == coid:
                    prior.append(barrier_event)
            if "write_emitted" not in prior:
                violations.append(
                    f"post-write/pre-ACK seam not proven for {coid}: write absent"
                )
        return violations


class MisclassifiedAckLossBybitProfile(BybitProfile):
    """Broken control that misclassifies a booked order as rejected."""

    @staticmethod
    def post_write_error(message: str) -> Exception:
        return BybitAPIError(message, ret_code=10001)


class AliasingBybitProfile(BybitProfile):
    """Deliberately broken physical-order identity model for oracle testing."""

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineBybit:
        return AliasingOfflineBybit(self, run_name, store_ctx)


class FragmentedFillBybitProfile(BybitProfile):
    """Fragmented derivative fill with a private-stream gap and REST recovery."""

    inject_corrupt_duplicate = False

    def __init__(self) -> None:
        super().__init__()
        self.recovered_counts: list[int] = []
        self.fragment_coid: str | None = None

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "fragment_bybit_entry_with_ws_gap":
            runtime = runner.runs[step.run]
            if not runtime.broker._adoption_baselined:
                raise AssertionError("Bybit adoption baseline was not committed")
            pine_id = str(step.values["id"])
            candidates = [
                record
                for record in self.state.orders.values()
                if record.run_name == step.run
                and record.pine_id == pine_id
                and record.leg_type is LegType.ENTRY
                and record.order.status is OrderStatus.OPEN
            ]
            if len(candidates) != 1:
                raise AssertionError(
                    f"expected one open fragmented Bybit entry {pine_id!r}, "
                    f"got {candidates}"
                )
            record = candidates[0]
            raw = self.raw_orders[record.order.id]
            coid = str(raw["orderLinkId"])
            self.fragment_coid = coid
            now_ms = int(epoch_time() * 1000)
            slices = (
                ("frag-1", "0.02", now_ms - 3_000),
                ("frag-3", "0.025", now_ms - 1_000),
                ("frag-2", "0.015", now_ms - 2_000),
            )
            executions: list[dict[str, Any]] = []
            for exec_id, qty, ts_ms in slices:
                executions.append(
                    {
                        "category": "linear",
                        "symbol": self.symbol,
                        "execType": "Trade",
                        "execId": exec_id,
                        "orderLinkId": coid,
                        "orderId": raw["orderId"],
                        "side": raw["side"],
                        "execQty": qty,
                        "execPrice": "90",
                        "execFee": "0",
                        "execTime": str(ts_ms),
                    }
                )
            # The REST endpoint is newest-first rather than chronological;
            # repeat one execId as an overlap/page duplicate as well.
            self.raw_executions.extend(
                (executions[0], executions[1], executions[2], dict(executions[2]))
            )
            if self.inject_corrupt_duplicate:
                corrupt = dict(executions[2])
                corrupt["execId"] = "frag-2-corrupt-copy"
                self.raw_executions.append(corrupt)
            raw["cumExecQty"] = "0.06"
            raw["avgPrice"] = "90"
            raw["orderStatus"] = "PartiallyFilled"

            pushed = runtime.broker._translate_private_frame(
                {"topic": "execution", "data": [executions[0]]},
                runtime.broker._market,
            )
            if len(pushed) != 1 or pushed[0].fill_id != "frag-1":
                raise AssertionError(
                    f"expected only frag-1 on the private stream, got {pushed}"
                )
            record.order = pushed[0].order
            self.state.position_owners[step.run] = 0.06
            self.state.position = sum(self.state.position_owners.values())
            self.state.pending_events.append((step.run, pushed[0]))

            runtime.broker._deriv_exec_floor_ms = now_ms - 10_000
            runtime.broker._deriv_exec_watermark = now_ms - 10_000
            return True
        if step.kind == "run_bybit_deriv_reconcile":
            runtime = runner.runs[step.run]
            events = asyncio.run(
                runtime.broker._run_deriv_reconcile(runtime.broker._market)
            )
            self.recovered_counts.append(len(events))
            for event in events:
                matching = [
                    record
                    for record in self.state.orders.values()
                    if record.run_name == step.run
                    and record.order.id == event.order.id
                ]
                if len(matching) == 1:
                    matching[0].order = event.order
                self.state.pending_events.append((step.run, event))
            return True
        if step.kind == "expect_bybit_fragment_recovery":
            runtime = runner.runs[step.run]
            if self.fragment_coid is None:
                raise AssertionError("fragmented Bybit entry was not recorded")
            row = runtime.store_ctx.get_order(self.fragment_coid)
            if row is None:
                raise AssertionError("fragmented Bybit store row is absent")
            expected_position = float(step.values["position"])
            expected_filled = float(step.values["filled"])
            if abs(runtime.position.size - expected_position) > 1e-12:
                raise AssertionError(
                    f"expected recovered position {expected_position}, "
                    f"got {runtime.position.size}"
                )
            if abs(row.filled_qty - expected_filled) > 1e-12:
                raise AssertionError(
                    f"expected durable filled cursor {expected_filled}, "
                    f"got {row.filled_qty}"
                )
            expected_counts = list(step.values["recovered_counts"])
            if self.recovered_counts != expected_counts:
                raise AssertionError(
                    f"expected reconcile recovery counts {expected_counts}, "
                    f"got {self.recovered_counts}"
                )
            if [row["execId"] for row in self.raw_executions[:3]] != [
                "frag-1", "frag-3", "frag-2"
            ]:
                raise AssertionError("fragmented REST rows were not out of order")
            if sum(row["execId"] == "frag-2" for row in self.raw_executions) != 2:
                raise AssertionError("fragmented REST overlap duplicate is absent")
            if not {"frag-1", "frag-2", "frag-3"}.issubset(
                runtime.broker._seen_exec_ids
            ):
                raise AssertionError(
                    "fragmented execution IDs did not enter the dedup frontier"
                )
            return True
        return super().handle_step(runner, step)


class CorruptDuplicateFragmentBybitProfile(FragmentedFillBybitProfile):
    """Broken control: one economic slice is exposed under a second execId."""

    inject_corrupt_duplicate = True


class SpotBybitProfile(BybitProfile):
    """Spot request-denomination profile using the real execution mapper."""

    quantity_step = 0.000001

    def __init__(self) -> None:
        super().__init__()
        self.market = _spot_instrument()


class OfflineSpotInventoryBybit(OfflineBybit):
    """Real spot inventory path over deterministic wallet/execution reads."""

    def __init__(
        self, profile: "UsdcSpotInventoryBybitProfile", run_name: str, store_ctx: Any
    ) -> None:
        super().__init__(profile, run_name, store_ctx)
        self._broker_started = False
        self._account_id = profile.account_id

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "get",
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        del method, auth
        if endpoint == "/v5/account/wallet-balance":
            payload = dict(params or {})
            self.rest_calls.append((endpoint, payload))
            self.profile.transport_calls.append((endpoint, payload))
            requested = str(payload.get("coin") or "")
            coins = [
                {"coin": coin, "walletBalance": str(balance)}
                for coin, balance in self.profile.wallet.items()
                if not requested or coin == requested
            ]
            return {"list": [{"accountType": "UNIFIED", "coin": coins}]}
        if endpoint == "/v5/execution/list":
            payload = dict(params or {})
            self.rest_calls.append((endpoint, payload))
            self.profile.transport_calls.append((endpoint, payload))
            start = int(payload.get("startTime") or 0)
            end = int(payload.get("endTime") or 2**63 - 1)
            rows = [
                execution
                for execution in self.profile.raw_executions
                if start <= int(execution["execTime"]) <= end
            ]
            return {"list": rows, "nextPageCursor": ""}
        return super().__call__(endpoint, params, body=body)


class UsdcSpotInventoryBybitProfile(BybitProfile):
    """ETHUSDC wallet, fee and sellable-residual lifecycle model."""

    symbol = "ETHUSDC"
    quantity_step = 0.00001
    foreign_base = Decimal("1")
    initial_quote = Decimal("1000")
    fee_rate = Decimal("0.0005")

    def __init__(self) -> None:
        super().__init__()
        self.market = _usdc_spot_instrument()
        self.wallet = {
            self.market.base_coin: self.foreign_base,
            self.market.quote_coin: self.initial_quote,
        }
        self.fill_sequence: list[dict[str, str]] = []

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineSpotInventoryBybit:
        return OfflineSpotInventoryBybit(self, run_name, store_ctx)

    def _reported_fee_currency(self, side: str) -> str:
        return self.market.base_coin if side == "buy" else self.market.quote_coin

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "fill_bybit_usdc_spot":
            runtime = runner.runs[step.run]
            side = str(step.values["side"])
            pending = [
                raw
                for raw in self.raw_orders.values()
                if raw["orderStatus"] in ("New", "PartiallyFilled")
                and raw["side"].lower() == side
            ]
            if len(pending) != 1:
                raise AssertionError(f"expected one pending ETHUSDC {side}, got {pending}")
            raw = pending[0]
            qty = Decimal(str(raw["qty"]))
            price = Decimal(str(step.values.get("price", "2000")))
            value = qty * price
            if side == "buy":
                fee = qty * self.fee_rate
                self.wallet[self.market.base_coin] += qty - fee
                self.wallet[self.market.quote_coin] -= value
            else:
                fee = value * self.fee_rate
                self.wallet[self.market.base_coin] -= qty
                self.wallet[self.market.quote_coin] += value - fee
            fee_currency = self._reported_fee_currency(side)
            raw["cumExecQty"] = raw["qty"]
            raw["avgPrice"] = str(price)
            raw["orderStatus"] = "Filled"
            execution = {
                "category": "spot",
                "symbol": self.symbol,
                "execType": "Trade",
                "execId": f"spot-{side}-{len(self.raw_executions) + 1}",
                "orderLinkId": raw["orderLinkId"],
                "orderId": raw["orderId"],
                "side": raw["side"],
                "execQty": str(qty),
                "execPrice": str(price),
                "execValue": str(value),
                "execFee": str(fee),
                "feeCurrency": fee_currency,
                "execTime": str(runner.now_ms),
                "seq": str(len(self.raw_executions) + 1),
            }
            self.raw_executions.append(execution)
            events = runtime.broker._translate_private_frame(
                {"topic": "execution", "data": [execution]},
                runtime.broker._market,
            )
            if len(events) != 1:
                raise AssertionError(f"ETHUSDC execution did not emit one fill: {events}")
            event = events[0]
            signed = event.fill_qty if side == "buy" else -event.fill_qty
            self.state.position_owners[step.run] = (
                self.state.position_owners.get(step.run, 0.0) + signed
            )
            self.state.position = sum(self.state.position_owners.values())
            self.state.pending_events.append((step.run, event))
            self.fill_sequence.append(
                {
                    "side": side,
                    "qty": str(qty),
                    "fee": str(fee),
                    "fee_currency": fee_currency,
                }
            )
            return True
        if step.kind == "expect_bybit_usdc_spot_inventory":
            runtime = runner.runs[step.run]
            manager = runtime.broker._spot_manager
            if manager is None:
                raise AssertionError("ETHUSDC spot inventory manager was not started")
            rows = runtime.store_ctx.iter_spot_executions(
                runtime.broker.account_id, self.market.symbol
            )
            ledger_base = sum((Decimal(row.base_delta) for row in rows), Decimal(0))
            ledger_quote = sum((Decimal(row.quote_delta) for row in rows), Decimal(0))
            expected_base = Decimal(str(step.values["base_delta"]))
            expected_quote = Decimal(str(step.values["quote_delta"]))
            if ledger_base != expected_base or ledger_quote != expected_quote:
                raise AssertionError(
                    "expected ETHUSDC ledger deltas "
                    f"base={expected_base} quote={expected_quote}, got "
                    f"base={ledger_base} quote={ledger_quote}"
                )
            expected_fee_currencies = list(step.values["fee_currencies"])
            actual_fee_currencies = [row.fee_currency for row in rows]
            if actual_fee_currencies != expected_fee_currencies:
                raise AssertionError(
                    f"expected ETHUSDC fee currencies {expected_fee_currencies}, "
                    f"got {actual_fee_currencies}"
                )
            sellable = ledger_base.quantize(
                Decimal(self.market.qty_step_str), rounding=ROUND_DOWN
            )
            expected_sellable = Decimal(str(step.values["sellable"]))
            if sellable != expected_sellable:
                raise AssertionError(
                    f"expected ETHUSDC sellable inventory {expected_sellable}, got {sellable}"
                )
            engine = Decimal(str(runtime.position.size))
            expected_engine = Decimal(str(step.values["engine_position"]))
            if abs(engine - expected_engine) > Decimal("1e-12"):
                raise AssertionError(
                    f"expected ETHUSDC engine position {expected_engine}, got {engine}"
                )
            if expected_engine == 0 and 0 < manager.fold.net_base < Decimal(
                self.market.qty_step_str
            ):
                # VenueState models the engine-visible tradable position. The
                # exact sub-grid residue remains independently asserted in the
                # durable inventory ledger and wallet invariant above.
                self.state.position_owners[step.run] = 0.0
                self.state.position = sum(self.state.position_owners.values())
            return True
        if step.kind == "expect_no_bybit_spot_external_close_warning":
            false_external_close = [
                message
                for message in self.warning_collector.messages
                if "external close detected" in message
            ]
            if false_external_close:
                raise AssertionError(
                    "own ETHUSDC sub-grid dust was misclassified as external close: "
                    f"{false_external_close}"
                )
            return True
        return super().handle_step(runner, step)

    def check_invariants(self, runner: Any):
        violations = list(super().check_invariants(runner))
        for runtime in runner.runs.values():
            manager = runtime.broker._spot_manager
            if manager is None:
                continue
            expected_total = self.foreign_base + manager.fold.net_base
            actual_total = self.wallet[self.market.base_coin]
            if expected_total != actual_total:
                violations.append(
                    "Bybit spot inventory balance mismatch: "
                    f"expected={expected_total} actual={actual_total}"
                )
        return violations


class WrongBuyFeeCurrencyBybitProfile(UsdcSpotInventoryBybitProfile):
    """Negative control: the execution lies about a base-charged buy fee."""

    def _reported_fee_currency(self, side: str) -> str:
        if side == "buy":
            return self.market.quote_coin
        return super()._reported_fee_currency(side)


class UsdcLinearBybitProfile(BybitProfile):
    """USDC-settled linear profile using discovered ETHPERP venue rules."""

    symbol = "ETHPERP"
    quantity_step = 0.01

    def __init__(self) -> None:
        super().__init__()
        self.market = _usdc_linear_instrument()


class InverseBybitProfile(BybitProfile):
    """Inverse-contract profile using a deterministic conversion anchor."""

    symbol = "BTCUSD"
    quantity_step = 0.00001

    def __init__(self) -> None:
        super().__init__()
        self.market = _inverse_instrument()


class OfflineHedgeBybit(OfflineBybit):
    """Real hedge-mode position port over an in-memory Bybit transport."""

    def __init__(self, profile: "HedgeBybitProfile", run_name: str, store_ctx: Any) -> None:
        super().__init__(profile, run_name, store_ctx)
        self._position_mode = POSITION_MODE_HEDGE
        self.position_port = self

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "get",
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        if endpoint == "/v5/position/list":
            self.rest_calls.append((endpoint, {}))
            self.profile.transport_calls.append((endpoint, {}))
            return {"list": self.profile.position_rows()}
        if endpoint == "/v5/order/create":
            payload = dict(body or {})
            coid = str(payload.get("orderLinkId") or "")
            duplicate = next(
                (
                    raw
                    for raw in self.profile.raw_orders.values()
                    if raw.get("orderLinkId") == coid
                ),
                None,
            )
            if duplicate is not None:
                self.rest_calls.append((endpoint, payload))
                self.profile.transport_calls.append((endpoint, payload))
                return {
                    "orderId": duplicate["orderId"],
                    "orderLinkId": duplicate["orderLinkId"],
                }
        result = super().__call__(
            endpoint,
            params,
            method=method,
            body=body,
            auth=auth,
        )
        if endpoint == "/v5/order/create":
            order_id = str(result["orderId"])
            payload = dict(body or {})
            self.profile.raw_orders[order_id]["positionIdx"] = int(
                payload.get("positionIdx") or 0
            )
        return result


class HedgeBybitProfile(BybitProfile):
    """USDT perpetual hedge account with run-owned and foreign leg attribution."""

    venue_mode = "hedged"
    foreign_sell_qty = 0.001

    def __init__(self) -> None:
        super().__init__()
        self.owned_legs: dict[str, dict[int, float]] = {}
        self.foreign_legs = {
            HEDGE_IDX_BUY: 0.0,
            HEDGE_IDX_SELL: self.foreign_sell_qty,
        }
        self.initial_foreign_legs = dict(self.foreign_legs)
        self.hedge_fill_sequence: list[dict[str, Any]] = []

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineHedgeBybit:
        self.owned_legs.setdefault(
            run_name,
            {HEDGE_IDX_BUY: 0.0, HEDGE_IDX_SELL: 0.0},
        )
        return OfflineHedgeBybit(self, run_name, store_ctx)

    def position_rows(self) -> list[dict[str, Any]]:
        totals = {
            idx: self.foreign_legs[idx]
            + sum(legs[idx] for legs in self.owned_legs.values())
            for idx in (HEDGE_IDX_BUY, HEDGE_IDX_SELL)
        }
        return [
            {
                "symbol": self.symbol,
                "positionIdx": idx,
                "size": str(totals[idx]),
                "side": "Buy" if idx == HEDGE_IDX_BUY else "Sell",
                "avgPrice": "100",
                "unrealisedPnl": "0",
                "liqPrice": "",
                "leverage": "1",
                "tradeMode": 0,
                "createdTime": "1700000000000",
                "updatedTime": "1700000002000",
            }
            for idx in (HEDGE_IDX_BUY, HEDGE_IDX_SELL)
        ]

    def _apply_hedge_fill(self, run_name: str, raw: dict[str, Any]) -> None:
        idx = int(raw["positionIdx"])
        qty = float(raw["qty"])
        owned = self.owned_legs[run_name]
        if raw.get("reduceOnly") is True:
            reduced = min(owned[idx], qty)
            owned[idx] -= reduced
            remainder = qty - reduced
            if remainder > 1e-12:
                self.foreign_legs[idx] = max(
                    0.0, self.foreign_legs[idx] - remainder
                )
        else:
            owned[idx] += qty

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "fill_bybit_hedge_orders":
            runtime = runner.runs[step.run]
            pending = [
                raw
                for raw in self.raw_orders.values()
                if raw["orderStatus"] in ("New", "PartiallyFilled")
            ]
            if not pending:
                raise AssertionError("expected at least one open hedge order")
            for raw in pending:
                self._apply_hedge_fill(step.run, raw)
                raw["cumExecQty"] = raw["qty"]
                raw["avgPrice"] = str(step.values.get("price", 100.0))
                raw["orderStatus"] = "Filled"
                execution = {
                    "category": "linear",
                    "symbol": self.symbol,
                    "execType": "Trade",
                    "execId": f"exec-{raw['orderId']}",
                    "orderLinkId": raw["orderLinkId"],
                    "orderId": raw["orderId"],
                    "side": raw["side"],
                    "execQty": raw["qty"],
                    "execPrice": raw["avgPrice"],
                    "execFee": "0",
                    "execTime": "1700000003000",
                }
                self.raw_executions.append(execution)
                events = runtime.broker._translate_private_frame(
                    {"topic": "execution", "data": [execution]},
                    runtime.broker._market,
                )
                if len(events) != 1:
                    raise AssertionError(
                        f"Bybit hedge execution did not emit one fill: {events}"
                    )
                signed = events[0].fill_qty if raw["side"] == "Buy" else -events[0].fill_qty
                self.state.position_owners[step.run] = (
                    self.state.position_owners.get(step.run, 0.0) + signed
                )
                self.state.pending_events.append((step.run, events[0]))
                self.hedge_fill_sequence.append(
                    {
                        "reduceOnly": bool(raw.get("reduceOnly")),
                        "positionIdx": int(raw["positionIdx"]),
                        "side": raw["side"],
                        "qty": float(raw["qty"]),
                    }
                )
            self.state.position = sum(self.state.position_owners.values())
            return True
        if step.kind == "expect_bybit_hedge_sequence":
            expected = list(step.values["sequence"])
            actual = self.hedge_fill_sequence[-len(expected):]
            if actual != expected:
                raise AssertionError(
                    f"expected Bybit hedge order sequence {expected}, got {actual}"
                )
            return True
        if step.kind == "expect_bybit_hedge_ownership":
            expected = float(step.values["position"])
            actual = self.state.position_owners.get(step.run, 0.0)
            if abs(actual - expected) > 1e-9:
                raise AssertionError(
                    f"expected run-owned hedge position {expected}, got {actual}"
                )
            engine = runner.runs[step.run].position.size
            if abs(engine - expected) > 1e-9:
                raise AssertionError(
                    f"expected engine hedge position {expected}, got {engine}"
                )
            return True
        return super().handle_step(runner, step)

    def check_invariants(self, runner: Any):
        violations: list[str] = []
        if self.foreign_legs != self.initial_foreign_legs:
            violations.append(
                "Bybit external hedge leg changed: "
                f"expected={self.initial_foreign_legs} actual={self.foreign_legs}"
            )
        if not self.state.pending_events:
            for run_name, runtime in runner.runs.items():
                owned = self.owned_legs[run_name]
                nonzero = [idx for idx, qty in owned.items() if qty > 1e-9]
                if len(nonzero) > 1:
                    violations.append(
                        f"Bybit run {run_name} owns opposing hedge legs: {owned}"
                    )
                signed = owned[HEDGE_IDX_BUY] - owned[HEDGE_IDX_SELL]
                journal = self.state.position_owners.get(run_name, 0.0)
                if abs(signed - journal) > 1e-9:
                    violations.append(
                        f"Bybit hedge ownership mismatch for {run_name}: "
                        f"legs={signed} journal={journal}"
                    )
                if abs(runtime.position.size - journal) > 1e-9:
                    violations.append(
                        f"Bybit hedge engine mismatch for {run_name}: "
                        f"engine={runtime.position.size} journal={journal}"
                    )
        return violations


class ForeignMutationHedgeBybitProfile(HedgeBybitProfile):
    """Negative control: lowest transport applies an entry to the foreign leg."""

    def _apply_hedge_fill(self, run_name: str, raw: dict[str, Any]) -> None:
        if raw.get("reduceOnly") is not True and raw["side"] == "Buy":
            leaked = min(self.foreign_legs[HEDGE_IDX_SELL], float(raw["qty"]))
            self.foreign_legs[HEDGE_IDX_SELL] -= leaked
        super()._apply_hedge_fill(run_name, raw)


class CoidAliasOfflineHedgeBybit(OfflineHedgeBybit):
    """Negative control that aliases the reversal residual to the spent entry."""

    def __call__(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "get",
        body: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> dict[str, Any]:
        payload = dict(body or {})
        if (
            endpoint == "/v5/order/create"
            and payload.get("side") == "Sell"
            and payload.get("reduceOnly") is not True
        ):
            prior = next(
                (
                    raw
                    for raw in self.profile.raw_orders.values()
                    if raw.get("side") == "Buy" and raw.get("reduceOnly") is not True
                ),
                None,
            )
            if prior is not None:
                payload["orderLinkId"] = prior["orderLinkId"]
                body = payload
        return super().__call__(
            endpoint,
            params,
            method=method,
            body=body,
            auth=auth,
        )


class CoidAliasHedgeBybitProfile(HedgeBybitProfile):
    """Hedge profile whose transport deliberately reuses a spent entry ID."""

    def create_broker(self, run_name: str, store_ctx: Any) -> CoidAliasOfflineHedgeBybit:
        self.owned_legs.setdefault(
            run_name,
            {HEDGE_IDX_BUY: 0.0, HEDGE_IDX_SELL: 0.0},
        )
        return CoidAliasOfflineHedgeBybit(self, run_name, store_ctx)


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
        Scenario(
            name="bybit-usdt-hedge-run-ownership-reversal-restart",
            profile_factory=HedgeBybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "R", "side": "buy", "qty": 0.002}),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
                Step(
                    "expect_bybit_hedge_sequence",
                    values={
                        "sequence": [
                            {
                                "reduceOnly": False,
                                "positionIdx": HEDGE_IDX_BUY,
                                "side": "Buy",
                                "qty": 0.002,
                            }
                        ]
                    },
                ),
                Step("expect_bybit_hedge_ownership", values={"position": 0.002}),
                Step("restart", check_invariants=False),
                Step("expect_bybit_hedge_ownership", values={"position": 0.002}),
                Step("entry", values={"id": "R", "side": "sell", "qty": 0.002}),
                Step("sync", values={"last_price": 100.0, "advance_ms": 0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
                Step(
                    "expect_bybit_hedge_sequence",
                    values={
                        "sequence": [
                            {
                                "reduceOnly": True,
                                "positionIdx": HEDGE_IDX_BUY,
                                "side": "Sell",
                                "qty": 0.002,
                            },
                            {
                                "reduceOnly": False,
                                "positionIdx": HEDGE_IDX_SELL,
                                "side": "Sell",
                                "qty": 0.002,
                            },
                        ]
                    },
                ),
                Step("expect_bybit_hedge_ownership", values={"position": -0.002}),
                Step("restart", check_invariants=False),
                Step("expect_bybit_hedge_ownership", values={"position": -0.002}),
                Step(
                    "close",
                    values={"id": "R", "side": "buy", "qty": 0.002},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
                Step("expect_bybit_hedge_ownership", values={"position": 0.0}),
            ),
        ),
        Scenario(
            name="bybit-usdt-hedge-external-leg-invariant-negative-control",
            profile_factory=ForeignMutationHedgeBybitProfile,
            seed=seed,
            expected_violation="Bybit external hedge leg changed",
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 0.002}),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
            ),
        ),
        Scenario(
            name="bybit-usdt-hedge-same-bar-coid-invariant-negative-control",
            profile_factory=CoidAliasHedgeBybitProfile,
            seed=seed,
            expected_violation="expected Bybit hedge order sequence",
            steps=(
                Step("entry", values={"id": "R", "side": "buy", "qty": 0.002}),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
                Step("restart", check_invariants=False),
                Step("entry", values={"id": "R", "side": "sell", "qty": 0.002}),
                Step("sync", values={"last_price": 100.0, "advance_ms": 0}),
                Step("fill_bybit_hedge_orders", check_invariants=False),
                Step("deliver"),
                Step(
                    "expect_bybit_hedge_sequence",
                    values={
                        "sequence": [
                            {
                                "reduceOnly": True,
                                "positionIdx": HEDGE_IDX_BUY,
                                "side": "Sell",
                                "qty": 0.002,
                            },
                            {
                                "reduceOnly": False,
                                "positionIdx": HEDGE_IDX_SELL,
                                "side": "Sell",
                                "qty": 0.002,
                            },
                        ]
                    },
                ),
            ),
        ),
        Scenario(
            name="bybit-post-write-lost-create-ack-adopts-exactly-once",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("drop_next_bybit_create_ack"),
                Step(
                    "entry",
                    values={"id": "ACK", "side": "buy", "qty": 0.1, "limit": 90.0},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_post_write_ack_loss"),
                Step("expect_bybit_create_count", values={"count": 1}),
                Step("expect_bybit_raw_open_count", values={"count": 1}),
                Step("restart", check_invariants=False),
                Step(
                    "entry",
                    values={"id": "ACK", "side": "buy", "qty": 0.1, "limit": 90.0},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_no_pending_verification"),
                Step("expect_bybit_create_count", values={"count": 1}),
                Step("expect_bybit_raw_open_count", values={"count": 1}),
                Step("cancel", values={"id": "ACK"}),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_raw_open_count", values={"count": 0}),
                Step(
                    "expect",
                    values={"position": 0.0, "engine_position": 0.0, "open_orders": 0},
                ),
            ),
        ),
        Scenario(
            name="bybit-fragmented-partial-fill-ws-gap-rest-recovery",
            profile_factory=FragmentedFillBybitProfile,
            seed=seed,
            steps=(
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "entry",
                    values={"id": "F", "side": "buy", "qty": 0.1, "limit": 90.0},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fragment_bybit_entry_with_ws_gap",
                    values={"id": "F"},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step("run_bybit_deriv_reconcile", check_invariants=False),
                Step("deliver"),
                Step("run_bybit_deriv_reconcile"),
                Step(
                    "expect_bybit_fragment_recovery",
                    values={
                        "position": 0.06,
                        "filled": 0.06,
                        "recovered_counts": [2, 0],
                    },
                ),
                Step("restart"),
                Step("expect", values={"position": 0.06, "engine_position": 0.06}),
                Step("close", values={"id": "F"}),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_request", values={"qty": "0.06"}),
                Step(
                    "fill_bybit_close",
                    values={"id": "F", "qty": 0.06, "fill_id": "frag-close"},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "expect",
                    values={"position": 0.0, "engine_position": 0.0, "open_orders": 0},
                ),
            ),
        ),
        Scenario(
            name="control-bybit-distinct-id-duplicate-fragment-is-detected",
            profile_factory=CorruptDuplicateFragmentBybitProfile,
            seed=seed,
            expected_violation="economic account position mismatch",
            steps=(
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "entry",
                    values={"id": "F", "side": "buy", "qty": 0.1, "limit": 90.0},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fragment_bybit_entry_with_ws_gap",
                    values={"id": "F"},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step("run_bybit_deriv_reconcile", check_invariants=False),
                Step("deliver"),
            ),
        ),
        Scenario(
            name="control-bybit-post-write-ack-loss-reject-causes-duplicate",
            profile_factory=MisclassifiedAckLossBybitProfile,
            seed=seed,
            expected_violation="Bybit exactly-once physical-order invariant violated",
            steps=(
                Step("drop_next_bybit_create_ack"),
                Step(
                    "entry",
                    values={"id": "ACK", "side": "buy", "qty": 0.1, "limit": 90.0},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("sync", values={"last_price": 100.0}),
            ),
        ),
        Scenario(
            name="bybit-market-entry-uses-real-transport-shape",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 0.1}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_request",
                    values={"orderType": "Market", "side": "Buy", "qty": "0.1"},
                ),
            ),
        ),
        Scenario(
            name="bybit-usdc-perpetual-uses-linear-transport-and-discovered-grid",
            profile_factory=UsdcLinearBybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "U", "side": "buy", "qty": 0.01}),
                Step("sync", values={"last_price": 2_000.0}),
                Step(
                    "expect_bybit_request",
                    values={
                        "category": "linear",
                        "symbol": "ETHPERP",
                        "orderType": "Market",
                        "side": "Buy",
                        "qty": "0.01",
                    },
                ),
                Step(
                    "expect_bybit_request_absent",
                    values={"keys": ["marketUnit", "isLeverage"]},
                ),
            ),
        ),
        Scenario(
            name="bybit-usdc-restart-partial-then-full-close-leaves-no-bracket",
            profile_factory=UsdcLinearBybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "U", "side": "buy", "qty": 0.02}),
                Step("sync", values={"last_price": 2_000.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "U", "qty": 0.02, "price": 2_000.0},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "U",
                        "side": "sell",
                        "qty": 0.02,
                        "limit": 3_000.0,
                        "stop": 1_000.0,
                    },
                ),
                Step("sync", values={"last_price": 2_000.0}),
                Step("restart", check_invariants=False),
                Step("sync", values={"last_price": 2_000.0}),
                Step("expect_bybit_raw_open_count", values={"count": 2}),
                Step("close", values={"id": "U", "qty": 0.01}),
                Step("sync", values={"last_price": 2_000.0}),
                Step(
                    "fill_bybit_close",
                    values={"id": "U", "qty": 0.01, "price": 2_000.0},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step("expect_bybit_raw_open_count", values={"count": 2}),
                Step("close", values={"id": "U", "qty": 0.01}),
                Step("sync", values={"last_price": 2_000.0}, check_invariants=False),
                Step(
                    "fill_bybit_close",
                    values={"id": "U", "qty": 0.01, "price": 2_000.0},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step("sync", values={"last_price": 2_000.0}, check_invariants=False),
                Step("expect_bybit_raw_open_count", values={"count": 0}),
            ),
        ),
        Scenario(
            name="bybit-spot-market-entry-uses-base-coin-denomination",
            profile_factory=SpotBybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "S", "side": "buy", "qty": 0.0015}),
                Step("sync", values={"last_price": 100_000.0}),
                Step(
                    "expect_bybit_request",
                    values={
                        "category": "spot",
                        "symbol": "BTCUSDT",
                        "qty": "0.0015",
                        "marketUnit": "baseCoin",
                        "isLeverage": 0,
                    },
                ),
            ),
        ),
        Scenario(
            name="bybit-usdc-spot-inventory-fee-residual-roundtrip",
            profile_factory=UsdcSpotInventoryBybitProfile,
            seed=seed,
            steps=(
                Step("sync", values={"last_price": 2_000.0}),
                Step("entry", values={"id": "S", "side": "buy", "qty": 0.006}),
                Step("sync", values={"last_price": 2_000.0}),
                Step(
                    "expect_bybit_request",
                    values={
                        "category": "spot",
                        "symbol": "ETHUSDC",
                        "qty": "0.006",
                        "marketUnit": "baseCoin",
                        "isLeverage": 0,
                    },
                ),
                Step(
                    "fill_bybit_usdc_spot",
                    values={"side": "buy", "price": "2000"},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "expect_bybit_usdc_spot_inventory",
                    values={
                        "base_delta": "0.005997",
                        "quote_delta": "-12",
                        "fee_currencies": ["ETH"],
                        "sellable": "0.00599",
                        "engine_position": "0.005997",
                    },
                ),
                Step("restart", check_invariants=False),
                Step(
                    "expect",
                    values={"position": 0.005997, "engine_position": 0.005997},
                ),
                Step("close", values={"id": "S"}),
                Step("sync", values={"last_price": 2_000.0}),
                Step("expect_bybit_request", values={"qty": "0.00599"}),
                Step(
                    "fill_bybit_usdc_spot",
                    values={"side": "sell", "price": "2000"},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step(
                    "sync",
                    values={"last_price": 2_000.0, "advance_ms": 60_000},
                    check_invariants=False,
                ),
                Step(
                    "expect_bybit_usdc_spot_inventory",
                    values={
                        "base_delta": "0.000007",
                        "quote_delta": "-0.02599000",
                        "fee_currencies": ["ETH", "USDC"],
                        "sellable": "0.00000",
                        "engine_position": "0",
                    },
                ),
                Step("expect_no_bybit_spot_external_close_warning"),
                Step("restart", check_invariants=False),
                Step(
                    "expect_bybit_usdc_spot_inventory",
                    values={
                        "base_delta": "0.000007",
                        "quote_delta": "-0.02599000",
                        "fee_currencies": ["ETH", "USDC"],
                        "sellable": "0.00000",
                        "engine_position": "0",
                    },
                ),
                Step("expect_bybit_raw_open_count", values={"count": 0}),
            ),
        ),
        Scenario(
            name="bybit-usdc-spot-wrong-buy-fee-currency-negative-control",
            profile_factory=WrongBuyFeeCurrencyBybitProfile,
            seed=seed,
            expected_violation="Bybit spot inventory balance mismatch",
            steps=(
                Step("sync", values={"last_price": 2_000.0}),
                Step("entry", values={"id": "S", "side": "buy", "qty": 0.006}),
                Step("sync", values={"last_price": 2_000.0}),
                Step(
                    "fill_bybit_usdc_spot",
                    values={"side": "buy", "price": "2000"},
                    check_invariants=False,
                ),
                Step("deliver"),
            ),
        ),
        Scenario(
            name="bybit-inverse-market-entry-uses-contract-denomination-and-anchor",
            profile_factory=InverseBybitProfile,
            seed=seed,
            steps=(
                Step("set_bybit_last_price", values={"price": 100_000.0}),
                Step("entry", values={"id": "I", "side": "buy", "qty": 0.0015}),
                Step("sync", values={"last_price": 100_000.0}),
                Step(
                    "expect_bybit_request",
                    values={"category": "inverse", "symbol": "BTCUSD", "qty": "150"},
                ),
                Step(
                    "expect_bybit_request_absent",
                    values={"keys": ["marketUnit", "isLeverage"]},
                ),
            ),
        ),
        Scenario(
            name="bybit-strategy-order-oca-cancel-sweeps-sibling-once",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={
                        "id": "A",
                        "qty": 0.1,
                        "limit": 90.0,
                        "oca_name": "pair",
                        "oca_type": "cancel",
                    },
                ),
                Step(
                    "entry",
                    values={
                        "id": "B",
                        "qty": 0.1,
                        "limit": 89.0,
                        "oca_name": "pair",
                        "oca_type": "cancel",
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_entry", values={"id": "A", "qty": 0.1}),
                Step("deliver"),
                Step(
                    "expect_bybit_endpoint_count",
                    values={
                        "endpoint": "/v5/order/cancel",
                        "count": 1,
                    },
                ),
                Step(
                    "expect_bybit_order_state",
                    values={
                        "id": "B",
                        "orderStatus": "Cancelled",
                    },
                ),
            ),
        ),
        Scenario(
            name="bybit-strategy-order-oca-reduce-amends-sibling-once",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={
                        "id": "A",
                        "qty": 0.1,
                        "limit": 90.0,
                        "oca_name": "pair",
                        "oca_type": "reduce",
                    },
                ),
                Step(
                    "entry",
                    values={
                        "id": "B",
                        "qty": 0.1,
                        "limit": 89.0,
                        "oca_name": "pair",
                        "oca_type": "reduce",
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("fill_bybit_entry", values={"id": "A", "qty": 0.04}),
                Step("duplicate_event", check_invariants=False),
                Step("deliver"),
                Step(
                    "expect_bybit_endpoint_count",
                    values={
                        "endpoint": "/v5/order/amend",
                        "count": 1,
                    },
                ),
                Step("expect_bybit_order_state", values={"id": "B", "qty": "0.06"}),
            ),
        ),
        Scenario(
            name="bybit-working-order-adopted-after-restart",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step(
                    "entry",
                    values={"id": "L", "side": "buy", "qty": 0.1, "limit": 90.04},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("restart"),
                Step(
                    "entry",
                    values={"id": "L", "side": "buy", "qty": 0.1, "limit": 90.04},
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("expect", values={"calls": 1}),
                Step(
                    "expect_bybit_request",
                    values={"orderType": "Limit", "price": "90", "qty": "0.1"},
                ),
            ),
        ),
        Scenario(
            name="bybit-below-grid-entry-is-observable-and-not-sent",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "tiny", "side": "buy", "qty": 0.0005}),
                Step("sync", values={"last_price": 100.0}),
                Step("sync", values={"last_price": 100.0}),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_below_grid_skip"),
                Step("expect_bybit_create_count", values={"count": 0}),
            ),
        ),
        Scenario(
            name="bybit-quantity-grid-residual-is-not-under-rounded",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "grid", "side": "buy", "qty": 0.03 - 0.01}),
                Step("sync", values={"last_price": 1000.0}),
                Step("expect_bybit_request", values={"qty": "0.02"}),
            ),
        ),
        Scenario(
            name="bybit-two-entry-global-bracket-creates-four-physical-legs",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "A", "side": "buy", "qty": 0.01}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "A", "qty": 0.01},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step("entry", values={"id": "B", "side": "buy", "qty": 0.01}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "B", "qty": 0.01},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "A",
                        "side": "sell",
                        "qty": 0.01,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "B",
                        "side": "sell",
                        "qty": 0.01,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step("expect_bybit_create_count", values={"count": 6}),
                Step("expect_bybit_bracket_coverage", values={"qty": 0.01}),
            ),
        ),
        Scenario(
            name="control-bybit-aliased-global-bracket-is-detected",
            profile_factory=AliasingBybitProfile,
            seed=seed,
            expected_violation="take-profit protection coverage shortfall",
            steps=(
                Step("entry", values={"id": "A", "side": "buy", "qty": 0.01}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "A", "qty": 0.01},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step("entry", values={"id": "B", "side": "buy", "qty": 0.01}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "B", "qty": 0.01},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "A",
                        "side": "sell",
                        "qty": 0.01,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "B",
                        "side": "sell",
                        "qty": 0.01,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
            ),
        ),
        Scenario(
            name="bybit-buy-stop-limit-stays-dormant-until-rising-trigger",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("set_bybit_last_price", values={"price": 100.0}),
                Step(
                    "entry",
                    values={
                        "id": "BSL",
                        "side": "buy",
                        "qty": 0.1,
                        "limit": 110.0,
                        "stop": 105.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_request",
                    values={
                        "orderType": "Limit",
                        "price": "110",
                        "triggerPrice": "105",
                        "triggerDirection": 1,
                    },
                ),
            ),
        ),
        Scenario(
            name="bybit-sell-stop-limit-stays-dormant-until-falling-trigger",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("set_bybit_last_price", values={"price": 100.0}),
                Step(
                    "entry",
                    values={
                        "id": "SSL",
                        "side": "sell",
                        "qty": 0.1,
                        "limit": 90.0,
                        "stop": 95.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_request",
                    values={
                        "orderType": "Limit",
                        "price": "90",
                        "triggerPrice": "95",
                        "triggerDirection": 2,
                    },
                ),
            ),
        ),
        Scenario(
            name="bybit-already-crossed-buy-stop-limit-drops-trigger",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("set_bybit_last_price", values={"price": 100.0}),
                Step(
                    "entry",
                    values={
                        "id": "BC",
                        "side": "buy",
                        "qty": 0.1,
                        "limit": 105.0,
                        "stop": 95.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_request_absent",
                    values={"keys": ["triggerPrice", "triggerDirection"]},
                ),
            ),
        ),
        Scenario(
            name="bybit-already-crossed-sell-stop-limit-drops-trigger",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("set_bybit_last_price", values={"price": 100.0}),
                Step(
                    "entry",
                    values={
                        "id": "SC",
                        "side": "sell",
                        "qty": 0.1,
                        "limit": 95.0,
                        "stop": 105.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_request_absent",
                    values={"keys": ["triggerPrice", "triggerDirection"]},
                ),
            ),
        ),
        Scenario(
            name="bybit-bracket-amend-and-cancel-retains-no-orphan-leg",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 0.1}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "L", "qty": 0.1},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "L",
                        "side": "sell",
                        "qty": 0.1,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "amend_exit",
                    values={
                        "id": "X",
                        "from_entry": "L",
                        "limit": 111.0,
                        "stop": 89.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_endpoint_count",
                    values={"endpoint": "/v5/order/amend", "count": 2},
                ),
                Step(
                    "expect_bybit_active_bracket",
                    values={"from_entry": "L", "tp": 111.0, "sl": 89.0},
                ),
                Step("cancel", values={"id": "X"}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "expect_bybit_endpoint_count",
                    values={"endpoint": "/v5/order/cancel", "count": 2},
                ),
                Step("expect", values={"open_orders": 0}),
            ),
        ),
        Scenario(
            name="bybit-filled-bracket-leg-cancels-reduce-only-sibling",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 0.1}),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    values={"id": "L", "qty": 0.1},
                    check_invariants=False,
                ),
                Step("deliver"),
                Step(
                    "exit",
                    values={
                        "id": "X",
                        "from_entry": "L",
                        "side": "sell",
                        "qty": 0.1,
                        "limit": 110.0,
                        "stop": 90.0,
                    },
                ),
                Step("sync", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_bracket_leg",
                    values={"leg": "tp", "price": 110.0},
                    check_invariants=False,
                ),
                Step("deliver", check_invariants=False),
                Step("sync", values={"last_price": 110.0}),
                Step(
                    "expect_bybit_endpoint_count",
                    values={"endpoint": "/v5/order/cancel", "count": 2},
                ),
                Step("expect_bybit_bracket_outcome_swept"),
                Step(
                    "expect",
                    values={"position": 0.0, "engine_position": 0.0, "open_orders": 0},
                ),
            ),
        ),
        Scenario(
            name="bybit-concurrent-runs-restart-and-close-only-owned-exposure",
            profile_factory=BybitProfile,
            runs=("A", "B"),
            seed=seed,
            steps=(
                Step("entry", run="A", values={"id": "A", "side": "buy", "qty": 0.1}),
                Step("sync", run="A", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    run="A",
                    values={"id": "A", "qty": 0.1},
                    check_invariants=False,
                ),
                Step("deliver", run="A"),
                Step("restart", run="B", check_invariants=False),
                Step(
                    "expect", run="B", values={"position": 0.0, "engine_position": 0.0}
                ),
                Step("entry", run="B", values={"id": "B", "side": "buy", "qty": 0.1}),
                Step("sync", run="B", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_entry",
                    run="B",
                    values={"id": "B", "qty": 0.1},
                    check_invariants=False,
                ),
                Step("deliver", run="B"),
                Step("restart", run="A", check_invariants=False),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 0.1,
                        "engine_position": 0.1,
                        "account_position": 0.2,
                    },
                ),
                Step(
                    "expect", run="B", values={"position": 0.1, "engine_position": 0.1}
                ),
                Step(
                    "close", run="A", values={"id": "A", "from_entry": "A", "qty": 0.1}
                ),
                Step("sync", run="A", values={"last_price": 100.0}),
                Step(
                    "fill_bybit_close",
                    run="A",
                    values={"id": "A", "qty": 0.1},
                    check_invariants=False,
                ),
                Step("deliver", run="A"),
                Step(
                    "expect",
                    run="A",
                    values={
                        "position": 0.0,
                        "engine_position": 0.0,
                        "account_position": 0.1,
                    },
                ),
                Step(
                    "expect", run="B", values={"position": 0.1, "engine_position": 0.1}
                ),
            ),
        ),
    ]


def extended_scenarios(seed: int = 0) -> list[Scenario]:
    scenarios = smoke_scenarios(seed)
    axes = {
        "side": ("buy", "sell"),
        "order": ("market", "limit", "stop"),
        "restart": (False, True),
    }
    for index, case in enumerate(pairwise_cases(axes, seed=seed)):
        values: dict[str, Any] = {"id": "E", "side": case["side"], "qty": 0.1}
        if case["order"] == "limit":
            values["limit"] = 90.0 if case["side"] == "buy" else 110.0
        elif case["order"] == "stop":
            values["stop"] = 110.0 if case["side"] == "buy" else 90.0
        steps = [
            Step("entry", values=values),
            Step("sync", values={"last_price": 100.0}),
        ]
        if case["restart"]:
            steps.extend(
                (
                    Step("restart"),
                    Step("entry", values=values),
                    Step("sync", values={"last_price": 100.0}),
                )
            )
        scenarios.append(
            Scenario(
                name=f"bybit-pairwise-{index:03d}",
                profile_factory=BybitProfile,
                seed=seed,
                tags=frozenset({"extended"}),
                steps=tuple(steps),
            )
        )
    return scenarios


def build_suite(*, mode: str, seed: int) -> list[Scenario]:
    return smoke_scenarios(seed) if mode == "smoke" else extended_scenarios(seed)
