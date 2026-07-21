"""Opt-in offline conformance scenarios for the Bybit broker plugin."""

from typing import Any

from pynecore.core.broker.models import LegType
from pynecore.testing.broker_lab import Scenario, Step, pairwise_cases
from pynecore.testing.broker_lab.reference import (
    ReferenceVenueProfile,
    VenueOrder,
)
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.models import InstrumentInfo
from pynecore_bybit.positions import POSITION_MODE_ONE_WAY


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
        self._market = _linear_instrument()
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
            return {"orderId": order_id, "orderLinkId": payload.get("orderLinkId")}
        if endpoint == "/v5/order/realtime":
            return {"list": list(self.profile.raw_orders.values()), "nextPageCursor": ""}
        if endpoint == "/v5/position/list":
            size = self.profile.state.positions.get(self.run_name, 0.0)
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
            return {"list": [], "nextPageCursor": ""}
        raise AssertionError(f"unexpected offline Bybit REST call: {endpoint} {params}")

    async def execute_entry(self, envelope):
        orders = await super().execute_entry(envelope)
        for order in orders:
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=LegType.ENTRY,
                intent_key=envelope.intent.intent_key,
            )
        self.profile.state.calls.append((self.run_name, "entry", envelope.intent.intent_key))
        return orders

    async def execute_exit(self, envelope):
        orders = await super().execute_exit(envelope)
        for order in orders:
            leg_type = LegType.STOP_LOSS if order.stop_price is not None else LegType.TAKE_PROFIT
            self.profile.state.orders[order.id] = VenueOrder(
                order=order,
                run_name=self.run_name,
                pine_id=envelope.intent.pine_id,
                leg_type=leg_type,
                intent_key=envelope.intent.intent_key,
            )
        self.profile.state.calls.append((self.run_name, "exit", envelope.intent.intent_key))
        return orders


class BybitProfile(ReferenceVenueProfile):
    """One-way linear venue semantics around the real Bybit plugin."""

    plugin_name = "bybit-offline-lab"
    symbol = "BTCUSDT"
    quantity_step = 0.001

    def __init__(self) -> None:
        super().__init__()
        self.transport_calls: list[tuple[str, dict[str, Any]]] = []
        self.raw_orders: dict[str, dict[str, Any]] = {}

    def create_broker(self, run_name: str, store_ctx: Any) -> OfflineBybit:
        return OfflineBybit(self, run_name, store_ctx)

    def handle_step(self, runner: Any, step: Step) -> bool:
        if step.kind == "expect_bybit_request":
            requests = [body for endpoint, body in self.transport_calls if endpoint == "/v5/order/create"]
            if not requests:
                raise AssertionError("Bybit did not issue an order request")
            body = requests[-1]
            for key, value in step.values.items():
                if body.get(key) != value:
                    raise AssertionError(f"expected Bybit request {key}={value!r}, got {body.get(key)!r}")
            return True
        if step.kind == "expect_bybit_create_count":
            actual = sum(endpoint == "/v5/order/create" for endpoint, _ in self.transport_calls)
            expected = int(step.values["count"])
            if actual != expected:
                raise AssertionError(f"expected {expected} Bybit creates, got {actual}")
            return True
        return super().handle_step(runner, step)


def smoke_scenarios(seed: int = 0) -> list[Scenario]:
    return [
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
            name="bybit-global-bracket-creates-two-physical-legs",
            profile_factory=BybitProfile,
            seed=seed,
            steps=(
                Step("entry", values={"id": "L", "side": "buy", "qty": 0.1}),
                Step("sync", values={"last_price": 100.0}),
                Step("fill", check_invariants=False),
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
                Step("expect_bybit_create_count", values={"count": 3}),
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
        steps = [Step("entry", values=values), Step("sync", values={"last_price": 100.0})]
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
