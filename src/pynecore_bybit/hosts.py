"""Static host resolution for the Bybit v5 plugin.

Bybit has two independent environment axes: a live/demo axis and a regional
legal-entity axis (global, EU/MiCA, and several country domains). The
``(region, demo)`` pair resolves to a REST + public-WS + private-WS host
triple. This module holds only that static table plus the pure resolver — no
network I/O.

Only documented/verified hosts are listed. Where a regional host family has
no documented WebSocket endpoint, its entry is ``None`` and the caller must
supply an explicit override in the config (see the ``*_host`` fields of
:class:`pynecore_bybit.config.BybitConfig`); the resolver never guesses a
host by symmetry with another region.

Demo has no public stream of its own — demo runs on mainnet prices, so the
demo public-WS host is the region's live (mainnet) public stream, while REST
and the private stream use the dedicated ``*-demo`` hosts.

Verification status (2026-07-15):

* ``api.bybit.eu``, ``api-demo.bybit.eu``, ``api-demo.bybit.com`` returned
  ``retCode:0`` on ``/v5/market/time``.
* ``stream.bybit.eu`` (public+private), ``stream-demo.bybit.com/v5/private``
  and ``stream-demo.bybit.eu/v5/private`` completed the WS handshake (HTTP
  101).
* ``api.bybit.com`` / ``stream.bybit.com`` are the official global hosts.
* Regional REST domains and the ``.tr``/``.id``/``.kz``/``.ge`` stream
  domains are from Bybit's published host list; regions without a documented
  stream carry ``None``.
"""
from typing import NamedTuple


class BybitHosts(NamedTuple):
    """Resolved host triple (bare domains, no scheme or path)."""

    rest: str
    ws_public: str | None
    ws_private: str | None


#: Live (mainnet) host triples per region.
_LIVE: dict[str, BybitHosts] = {
    "global": BybitHosts("api.bybit.com", "stream.bybit.com", "stream.bybit.com"),
    "eu": BybitHosts("api.bybit.eu", "stream.bybit.eu", "stream.bybit.eu"),
    "nl": BybitHosts("api.bybit.nl", None, None),
    "tr": BybitHosts("api.bybit.tr", "stream.bybit.tr", "stream.bybit.tr"),
    "kz": BybitHosts("api.bybit.kz", "stream.bybit.kz", "stream.bybit.kz"),
    "ge": BybitHosts("api.bybitgeorgia.ge", "stream.bybitgeorgia.ge", "stream.bybitgeorgia.ge"),
    "ae": BybitHosts("api.bybit.ae", None, None),
    "id": BybitHosts("api.bybit.id", "stream.bybit.id", "stream.bybit.id"),
    "testnet": BybitHosts("api-testnet.bybit.com", None, None),
}

#: Demo host triples. Only regions with a documented/verified demo host are
#: listed; the public stream points at the region's live (mainnet) stream
#: because demo has no public stream of its own.
_DEMO: dict[str, BybitHosts] = {
    "global": BybitHosts("api-demo.bybit.com", "stream.bybit.com", "stream-demo.bybit.com"),
    "eu": BybitHosts("api-demo.bybit.eu", "stream.bybit.eu", "stream-demo.bybit.eu"),
}

#: Region keys accepted in the config, in documentation order.
REGIONS: tuple[str, ...] = (
    "global", "eu", "nl", "tr", "kz", "ge", "ae", "id", "testnet",
)


def resolve_hosts(
    region: str,
    *,
    demo: bool,
    rest_host: str = "",
    ws_public_host: str = "",
    ws_private_host: str = "",
) -> BybitHosts:
    """Resolve the REST + WS host triple for a ``(region, demo)`` pair.

    Non-empty override arguments win over the table (escape hatch for
    unlisted domains). A region absent from the selected mode's table with no
    override raises :class:`ValueError` — the resolver never guesses a host.

    :param region: One of :data:`REGIONS`.
    :param demo: Select the demo environment instead of live.
    :param rest_host: Optional REST host override.
    :param ws_public_host: Optional public-WS host override.
    :param ws_private_host: Optional private-WS host override.
    :return: The resolved :class:`BybitHosts`.
    :raises ValueError: On an unknown region, or a region unavailable in the
        selected mode with no matching override.
    """
    if region not in REGIONS:
        raise ValueError(
            f"Unknown Bybit region {region!r}; expected one of {', '.join(REGIONS)}"
        )

    table = _DEMO if demo else _LIVE
    base = table.get(region)

    if base is None and not (rest_host and ws_public_host and ws_private_host):
        mode = "demo" if demo else "live"
        raise ValueError(
            f"Bybit region {region!r} has no known {mode} host; set rest_host, "
            f"ws_public_host and ws_private_host overrides in the config to use it"
        )

    if base is None:
        return BybitHosts(rest_host, ws_public_host, ws_private_host)

    return BybitHosts(
        rest=rest_host or base.rest,
        ws_public=ws_public_host or base.ws_public,
        ws_private=ws_private_host or base.ws_private,
    )
