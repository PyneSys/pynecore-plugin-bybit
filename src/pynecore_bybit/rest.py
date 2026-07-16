"""Synchronous + async REST core for the Bybit plugin.

Thin signed ``httpx`` client over the Bybit v5 Open API. ``__call__`` runs
the actual HTTP request synchronously (offline downloads via ``pyne data``
are synchronous by contract); :meth:`_call` offloads it to a worker thread
for async callers so the network never blocks the event loop — the same
split the Capital.com plugin uses.

Authentication is HMAC-SHA256 over ``timestamp + api_key + recv_window +
(query string | body)`` with the ``X-BAPI-*`` headers. Every M1 provider
endpoint is public, but the signer is already wired so the broker mix-ins
only add endpoints, not transport machinery.

State touched: ``_http_client`` (lazy, connection-pooled), ``_instruments``
(the per-``(category, symbol)`` rule cache filled by the provider mix-in).
"""
import asyncio
import hashlib
import hmac
import json as json_module
import time
from json import JSONDecodeError
from urllib.parse import urlencode

import httpx

from ._base import _BybitBase
from .exceptions import BybitAPIError, BybitConnectionError
from .helpers import RECV_WINDOW_MS, REST_TIMEOUT_S


class _RestMixin(_BybitBase):
    """REST surface: signed request dispatcher + async wrapper."""

    def _get_http_client(self) -> httpx.Client:
        """Return the shared ``httpx.Client``, building it on first use.

        Built lazily so plugin discovery / config generation never opens
        sockets. ``httpx.Client`` is thread-safe, so the async wrapper's
        worker threads may share it — connection pooling then works across
        paged downloads instead of re-handshaking TLS per request.
        """
        client = self._http_client
        if client is None:
            client = httpx.Client(
                base_url=f"https://{self._hosts.rest}",
                timeout=REST_TIMEOUT_S,
            )
            self._http_client = client
        return client

    def __call__(self, endpoint: str, params: dict | None = None, *,
                 method: str = 'get', body: dict | None = None,
                 auth: bool = False) -> dict:
        """Call a Bybit v5 REST endpoint (synchronous).

        :param endpoint: Path below the host (e.g. ``"/v5/market/kline"``).
        :param params: Query parameters; ``None`` values are dropped.
        :param method: ``"get"`` or ``"post"``.
        :param body: JSON body for POST requests.
        :param auth: Sign the request with the configured API key pair.
        :return: The ``result`` object of the response envelope.
        :raises BybitAPIError: On a non-zero ``retCode``.
        :raises BybitConnectionError: On transport-level failures.
        """
        query = {k: v for k, v in (params or {}).items() if v is not None}
        method_lc = method.lower()

        headers: dict[str, str] = {}
        content: str | None = None
        if method_lc == 'post':
            content = json_module.dumps(body or {})
            headers['Content-Type'] = 'application/json'
        if auth:
            payload = content if method_lc == 'post' else urlencode(query)
            headers.update(self._sign_headers(payload or ''))

        try:
            res = self._get_http_client().request(
                method_lc.upper(), endpoint,
                params=query or None, content=content, headers=headers,
            )
        except (httpx.TimeoutException, httpx.TransportError) as e:
            raise BybitConnectionError(
                f"Bybit HTTP transport error on {endpoint}: {e}"
            ) from e
        try:
            envelope = res.json()
        except JSONDecodeError as e:
            raise BybitConnectionError(
                f"Bybit returned non-JSON response on {endpoint} "
                f"(HTTP {res.status_code})"
            ) from e

        ret_code = int(envelope.get('retCode', -1))
        if ret_code != 0:
            raise BybitAPIError(
                f"Bybit API error on {endpoint}: retCode={ret_code} "
                f"retMsg={envelope.get('retMsg', '')!r}",
                ret_code=ret_code,
            )
        return envelope.get('result') or {}

    async def _call(self, endpoint: str, params: dict | None = None, *,
                    method: str = 'get', body: dict | None = None,
                    auth: bool = False) -> dict:
        """Async wrapper around the sync REST dispatcher.

        Offloading to a thread keeps the network call off the event loop
        without duplicating the signing / envelope logic in a second async
        client — the request rate of a single plugin instance is orders of
        magnitude below what :func:`asyncio.to_thread` can sustain.
        """
        return await asyncio.to_thread(
            self, endpoint, params, method=method, body=body, auth=auth,
        )

    def _sign_headers(self, payload: str) -> dict[str, str]:
        """Build the ``X-BAPI-*`` header set for a signed request.

        :param payload: The urlencoded query string (GET) or the raw JSON
            body (POST) — the part after the fixed prefix in Bybit's
            signature recipe.
        :return: Headers carrying key, timestamp, recv window and signature.
        """
        assert self.config is not None
        timestamp = str(int(time.time() * 1000))
        prefix = f"{timestamp}{self.config.api_key}{RECV_WINDOW_MS}"
        signature = hmac.new(
            self.config.api_secret.encode(),
            (prefix + payload).encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            'X-BAPI-API-KEY': self.config.api_key,
            'X-BAPI-TIMESTAMP': timestamp,
            'X-BAPI-SIGN': signature,
            'X-BAPI-RECV-WINDOW': str(RECV_WINDOW_MS),
        }

    def _close_http_client(self) -> None:
        """Close and drop the pooled HTTP client (idempotent)."""
        client = self._http_client
        if client is not None:
            self._http_client = None
            client.close()
