"""Bybit plugin exception hierarchy.

Provider-side failures subclass :class:`~pynecore.core.plugin.ProviderError`
so the ``pyne data`` CLI reports them as a clean one-line error instead of a
traceback, and so the live runner can classify transient faults through
:func:`~pynecore.core.plugin.is_retryable_provider_error`. The broker-side
taxonomy mapping (``retCode`` -> ``pynecore.core.broker.exceptions``) lands
with the execution mix-ins.
"""
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
