from __future__ import annotations

import httpx
import pytest

from pynecore.core.broker.exceptions import ExchangeConnectionError
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
