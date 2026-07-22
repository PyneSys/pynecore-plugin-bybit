"""Opt-in offline conformance scenarios for the Bybit broker plugin."""

from dataclasses import replace
import logging
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
from pynecore_bybit.positions import POSITION_MODE_ONE_WAY


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


class SpotBybitProfile(BybitProfile):
    """Spot request-denomination profile using the real execution mapper."""

    quantity_step = 0.000001

    def __init__(self) -> None:
        super().__init__()
        self.market = _spot_instrument()


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


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
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
