"""Per-process endpoint overrides must win over the config file.

A proxy placed in front of the venue for one bot process (the live lab's
chaos cycles) cannot edit the plugin config shared by every other process on
the machine, so the environment has to take precedence.
"""

from pynecore_bybit import Bybit, BybitConfig


def __test_environment_hosts_win_over_config_and_table__(monkeypatch):
    monkeypatch.setenv("PYNE_BYBIT_REST_HOST", "localhost:47001")
    monkeypatch.setenv("PYNE_BYBIT_WS_PUBLIC_HOST", "localhost:47002")
    monkeypatch.setenv("PYNE_BYBIT_WS_PRIVATE_HOST", "localhost:47003")
    plugin = Bybit(config=BybitConfig(demo=True, rest_host="api.bytick.com"),
                   symbol="BTCUSDT", timeframe="1")
    assert (plugin._hosts.rest, plugin._hosts.ws_public, plugin._hosts.ws_private) == (
        "localhost:47001", "localhost:47002", "localhost:47003")


def __test_without_the_environment_the_config_and_table_apply__(monkeypatch):
    for name in ("PYNE_BYBIT_REST_HOST", "PYNE_BYBIT_WS_PUBLIC_HOST", "PYNE_BYBIT_WS_PRIVATE_HOST"):
        monkeypatch.delenv(name, raising=False)
    plugin = Bybit(config=BybitConfig(demo=True, rest_host="api.bytick.com"),
                   symbol="BTCUSDT", timeframe="1")
    assert plugin._hosts.rest == "api.bytick.com"
    assert plugin._hosts.ws_private == "stream-demo.bybit.com"
