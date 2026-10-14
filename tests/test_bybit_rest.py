from __future__ import annotations

import httpx
import pytest

from pynecore.core.broker.exceptions import (
    ExchangeConnectionError,
    VenueWritesQuarantinedError,
)
from pynecore_bybit import Bybit, BybitConfig
from pynecore_bybit.exceptions import BybitAPIError, map_broker_error


def __test_authenticated_read_retries_expired_timestamp_with_fresh_signature__(
    monkeypatch,
) -> None:
    timestamps = iter((1_000_000, 1_001_000))
    monkeypatch.setattr("pynecore_bybit.rest._epoch_ms", lambda: next(timestamps))
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={
                    "retCode": 10002,
                    "retMsg": "invalid request timestamp",
                    "result": {},
                },
            )
        return httpx.Response(200, json={"retCode": 0, "retMsg": "OK", "result": {"list": []}})

    plugin = Bybit(
        symbol="ETHPERP",
        timeframe="1",
        config=BybitConfig(api_key="demo-key", api_secret="demo-secret", demo=True),
    )
    plugin._http_client = httpx.Client(
        base_url="https://api-demo.bybit.com",
        transport=httpx.MockTransport(handle),
    )

    result = plugin("/v5/position/list", {"category": "linear"}, auth=True)

    assert result == {"list": []}
    assert len(requests) == 2
    assert requests[0].headers["X-BAPI-TIMESTAMP"] == "1000000"
    assert requests[1].headers["X-BAPI-TIMESTAMP"] == "1001000"
    assert requests[0].headers["X-BAPI-RECV-WINDOW"] == "5000"
    assert requests[1].headers["X-BAPI-RECV-WINDOW"] == "7500"
    assert requests[0].headers["X-BAPI-SIGN"] != requests[1].headers["X-BAPI-SIGN"]


def __test_authenticated_timestamp_retry_is_bounded_and_remains_transient__(
    monkeypatch,
) -> None:
    timestamps = iter((1_000_000, 1_001_000, 1_002_000))
    monkeypatch.setattr("pynecore_bybit.rest._epoch_ms", lambda: next(timestamps))
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "retCode": 10002,
                "retMsg": "invalid request timestamp",
                "result": {},
            },
        )

    plugin = Bybit(
        symbol="ETHPERP",
        timeframe="1",
        config=BybitConfig(api_key="demo-key", api_secret="demo-secret", demo=True),
    )
    plugin._http_client = httpx.Client(
        base_url="https://api-demo.bybit.com",
        transport=httpx.MockTransport(handle),
    )

    with pytest.raises(BybitAPIError) as raised:
        plugin("/v5/position/list", {"category": "linear"}, auth=True)

    assert raised.value.ret_code == 10002
    assert [request.headers["X-BAPI-RECV-WINDOW"] for request in requests] == [
        "5000",
        "7500",
        "10000",
    ]
    assert isinstance(map_broker_error(raised.value), ExchangeConnectionError)


def __test_quarantine_refuses_every_post_before_the_wire_but_lets_reads_through__() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"retCode": 0, "retMsg": "OK", "result": {"list": []}})

    plugin = Bybit(
        symbol="ETHPERP",
        timeframe="1",
        config=BybitConfig(api_key="demo-key", api_secret="demo-secret", demo=True),
    )
    plugin._http_client = httpx.Client(
        base_url="https://api-demo.bybit.com",
        transport=httpx.MockTransport(handle),
    )
    plugin.venue_writes_blocked = lambda: True

    for endpoint in ("/v5/order/create", "/v5/order/cancel-all", "/v5/position/trading-stop"):
        with pytest.raises(VenueWritesQuarantinedError) as raised:
            plugin(endpoint, method="post", body={"category": "linear"}, auth=True)
        assert endpoint in str(raised.value)
    # An unsigned POST is still a write: the gate does not key on ``auth``.
    with pytest.raises(VenueWritesQuarantinedError):
        plugin("/v5/order/cancel", method="post", body={"category": "linear"})
    assert requests == []

    assert plugin("/v5/order/realtime", {"category": "linear"}, auth=True) == {"list": []}
    assert [request.method for request in requests] == ["GET"]

    plugin.venue_writes_blocked = lambda: False
    assert plugin("/v5/order/cancel-all", method="post", body={"category": "linear"},
                  auth=True) == {"list": []}
    assert [request.method for request in requests] == ["GET", "POST"]


def __test_quarantine_latched_during_a_timestamp_retry_refuses_the_retry__() -> None:
    requests: list[httpx.Request] = []
    blocked = False

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal blocked
        requests.append(request)
        # The quarantine latches while the first POST is in flight.
        blocked = True
        return httpx.Response(
            200,
            json={"retCode": 10002, "retMsg": "invalid request timestamp", "result": {}},
        )

    plugin = Bybit(
        symbol="ETHPERP",
        timeframe="1",
        config=BybitConfig(api_key="demo-key", api_secret="demo-secret", demo=True),
    )
    plugin._http_client = httpx.Client(
        base_url="https://api-demo.bybit.com",
        transport=httpx.MockTransport(handle),
    )
    plugin.venue_writes_blocked = lambda: blocked

    with pytest.raises(VenueWritesQuarantinedError):
        plugin("/v5/order/create", method="post", body={"category": "linear"}, auth=True)
    assert [request.method for request in requests] == ["POST"]
