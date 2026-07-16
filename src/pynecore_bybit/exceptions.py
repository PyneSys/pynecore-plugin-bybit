"""Bybit plugin exception hierarchy + ``retCode`` -> broker taxonomy mapping.

Provider-side failures subclass :class:`~pynecore.core.plugin.ProviderError`
so the ``pyne data`` CLI reports them as a clean one-line error instead of a
traceback, and so the live runner can classify transient faults through
:func:`~pynecore.core.plugin.is_retryable_provider_error`.

The broker mix-ins translate :class:`BybitAPIError` responses into the
:mod:`pynecore.core.broker.exceptions` taxonomy via
:func:`map_broker_error`; the code sets below carry only codes taken from
the official v5 error-code list, with the trade-path ones (170213 order-not-found,
170131 insufficient balance, 170141 duplicate clientOrderId) measured live
on the global demo on 2026-07-16. Unknown non-zero codes on an
order write default to :class:`ExchangeOrderRejectedError` — a safe
fail-closed reject, never a silent success.
"""
from pynecore.core.broker.exceptions import (
    AuthenticationError,
    BrokerError,
    ExchangeOrderRejectedError,
    ExchangeRateLimitError,
    InsufficientMarginError,
)
from pynecore.core.plugin import ProviderError


class BybitError(ProviderError):
    """Base class for every Bybit plugin exception."""


class BybitConnectionError(BybitError):
    """Transport-level failure (HTTP timeout, connection refused, WS drop).

    Retryable: the live runner waits and reconnects instead of halting."""

    retryable = True


class BybitAPIError(BybitError):
    """The API answered with a non-zero ``retCode``.

    :ivar ret_code: The Bybit ``retCode`` value.
    """

    #: ``retCode`` values that signal a transient server-side condition —
    #: rate limiting (10006), internal errors (10016), timeouts (10000) and
    #: the WS/system-maintenance code (10018). Everything else is treated as
    #: a permanent request problem.
    _RETRYABLE_CODES = frozenset({10000, 10006, 10016, 10018})

    def __init__(self, message: str, *, ret_code: int):
        super().__init__(message)
        self.ret_code = ret_code

    @property
    def retryable(self) -> bool:  # type: ignore[override]
        return self.ret_code in self._RETRYABLE_CODES


class BybitSymbolError(BybitError, ValueError):
    """The requested symbol could not be resolved to a tradable Bybit
    instrument in any supported category, or the instrument is not in
    ``Trading`` status.

    Also subclasses :class:`ValueError` so symbol-validation callers that
    catch ``ValueError`` keep routing it through their existing error path.
    """


class BybitUnsupportedTimeframeError(BybitError, ValueError):
    """Raised when the user-requested TradingView timeframe has no Bybit
    kline interval equivalent. Surfaced at startup so misconfigurations
    fail closed before the first REST call.

    Also subclasses :class:`ValueError` so timeframe-validation callers that
    catch ``ValueError`` keep routing it through their existing error path.
    """


# === retCode -> broker taxonomy ============================================

#: Credential-level rejects — terminal, reconnect cannot recover.
#: 10003 invalid API key, 10004 signature error, 10005 permission denied,
#: 10010 request IP not on the key's whitelist, 33004 API key expired
#: (the ~90-day demo-key expiry surfaces as this one).
AUTH_ERROR_CODES = frozenset({10003, 10004, 10005, 10010, 33004})

#: Rate-limit rejects — the caller should back off and retry.
RATE_LIMIT_CODES = frozenset({10006, 10018})

#: Insufficient balance/margin rejects — non-terminal intent-level rejects
#: the strategy can react to. 170131 is the spot "Balance insufficient"
#: code (verified live on the global demo); 110007 the derivatives
#: "available balance not enough" counterpart.
INSUFFICIENT_BALANCE_CODES = frozenset({170131, 110007})

#: "Order does not exist / already terminal" responses on cancel/amend —
#: benign no-ops under the idempotent cancel contract. 170213 spot
#: (verified live on the global demo), 110001 derivatives.
ORDER_NOT_FOUND_CODES = frozenset({170213, 110001})

#: Duplicate ``orderLinkId`` rejects — the idempotency dedup fired, the
#: original order landed. 170141 spot (measured live on the global demo:
#: "Duplicate clientOrderId."), 110072 derivatives (documented).
DUPLICATE_COID_CODES = frozenset({170141, 110072})

#: Server-side timeout / internal-error responses on a WRITE — ambiguous:
#: the request may have been booked before the error was produced, so the
#: dispatch paths must park these as disposition-unknown (and verify by
#: ``orderLinkId``) instead of treating them as a definitive reject.
#: 10000 "Server Timeout", 10016 "Server error".
AMBIGUOUS_DISPOSITION_CODES = frozenset({10000, 10016})


def is_benign_trading_stop_reject(exc: BybitAPIError) -> bool:
    """Whether a ``/v5/position/trading-stop`` reject is a benign no-op.

    Measured live on the global demo (2026-07-16):

    - Setting a level on a flat/vanished position rejects with the GENERIC
      parameter code 10001 and ``retMsg`` "can not set tp/sl/ts for zero
      position" — the leg is gone, the bracket is moot. The code alone is
      not safe to swallow (10001 also covers genuinely malformed
      requests), so the message is matched too.
    - Re-sending the current levels (idempotent re-amend, or clearing an
      already-clear bracket) rejects with 34040 "not modified".
    """
    if exc.ret_code == 34040:
        return True
    return exc.ret_code == 10001 and 'zero position' in str(exc)


def map_broker_error(exc: BybitAPIError) -> BrokerError | None:
    """Translate a Bybit ``retCode`` reject into the core broker taxonomy.

    Only the classes with an engine-visible semantic difference are mapped;
    ``None`` tells the caller to apply its context-specific default
    (typically :class:`ExchangeOrderRejectedError` on an order write).
    The not-found / duplicate code sets are NOT mapped here — those are
    control-flow signals the dispatch paths check explicitly.

    :param exc: The API error raised by the REST layer.
    :return: The mapped broker error, or ``None`` for the caller's default.
    """
    if exc.ret_code in AUTH_ERROR_CODES:
        return AuthenticationError(str(exc), reason=f"retCode={exc.ret_code}")
    if exc.ret_code in RATE_LIMIT_CODES:
        return ExchangeRateLimitError(str(exc), retry_after=1.0)
    if exc.ret_code in INSUFFICIENT_BALANCE_CODES:
        return InsufficientMarginError(str(exc))
    return None


def reject_error(exc: BybitAPIError) -> ExchangeOrderRejectedError:
    """Build the definitive order-reject for an unmapped trade ``retCode``."""
    return ExchangeOrderRejectedError(str(exc))
