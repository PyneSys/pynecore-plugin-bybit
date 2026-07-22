"""Spot inventory port for the Bybit plugin — the venue surface the core
:class:`~pynecore.core.broker.spot_inventory.SpotInventoryManager` drives.

The port is a small standalone object (not the plugin itself) because its
identity attributes (``product_id`` / ``base_asset`` / ``quote_asset``)
depend on the resolved chart instrument, which is only known after symbol
resolution — a plugin-level attribute would have to lie until then.

Execution history is read time-scoped (``cursor_scope='time'``): the
durable cursor is the newest seen ``execTime`` watermark in epoch ms, every
read re-fetches a short overlap (the ledger dedups on ``execId``), and gaps
longer than the endpoint's 7-day window cap are walked window by window.
``GET /v5/execution/list`` pages NEWEST-FIRST inside a window, so a window
must be drained completely before the watermark may advance — a partially
paged window is returned ``conclusive=False`` (fail closed) rather than
risking a skipped older fill.

Attribution: the ledger tracks the BOT's inventory, so only fills that map
back to a dispatch of this plugin enter it — by the echoed ``orderLinkId``
(the deterministic client-order-id) or by an ``orderId`` recorded in the
BrokerStore ref index. A fill with a foreign / absent ``orderLinkId`` is
external activity and belongs to the epoch baseline; if it moved the base
balance, the invariant fires by design.
"""
import logging
from decimal import Decimal
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.spot_inventory import SpotExecution, SpotExecutionBatch

from .exceptions import BybitError
from .helpers import (
    EXECUTION_CURSOR_OVERLAP_MS,
    EXECUTION_PAGE_LIMIT,
    EXECUTION_WINDOW_MS,
)

if TYPE_CHECKING:
    from ._base import _BybitBase
    from .models import InstrumentInfo

logger = logging.getLogger(__name__)

#: Server pages drained per window before the read is declared
#: inconclusive — 200 pages x 100 rows = 20k fills in one 7-day window,
#: far beyond a single-symbol bot's plausible fill rate; hitting it means
#: something is wrong and the fail-closed path is the honest answer.
_MAX_WINDOW_PAGES = 200

#: How long a balance-invariant mismatch may stay pending before it is a
#: confirmed conflict. Spot fills settle instantly on Bybit; the grace
#: only covers ``execution/list`` indexing lag behind the wallet balance.
_SETTLEMENT_GRACE_S = 60.0


def spot_port_for(plugin: '_BybitBase', market: 'InstrumentInfo') -> '_BybitSpotPort':
    """Build the inventory port for the resolved chart instrument."""
    return _BybitSpotPort(plugin, market)


class _BybitSpotPort:
    """:class:`~pynecore.core.broker.spot_inventory.SpotInventoryPort`
    implementation over the Bybit v5 REST endpoints."""

    cursor_scope = 'time'
    #: Wallet balances are exact decimal strings and spot fills settle
    #: atomically, so the invariant needs no numeric slack.
    base_tolerance = Decimal(0)
    settlement_grace_s = _SETTLEMENT_GRACE_S

    def __init__(self, plugin: '_BybitBase', market: 'InstrumentInfo') -> None:
        self._plugin = plugin
        self._market = market
        self.product_id = market.symbol
        self.base_asset = market.base_coin
        self.quote_asset = market.quote_coin
        self.position_dust_threshold = Decimal(market.qty_step_str)

    # --- SpotInventoryPort surface -----------------------------------------

    async def fetch_executions(self, cursor: str | None) -> SpotExecutionBatch:
        """Read the bot's spot executions from the time cursor.

        ``cursor=None`` (first startup) anchors the watermark at the
        venue's current clock and returns an empty batch — the account's
        prior history is foreign inventory, not the bot's ledger.
        """
        now_ms = int(epoch_time() * 1000)
        if cursor is None:
            return SpotExecutionBatch(next_cursor=str(now_ms))
        try:
            watermark = int(cursor)
        except ValueError:
            # A cursor this port cannot parse means a scope/format change
            # slipped past the persisted ``cursor_scope`` guard — fail
            # closed rather than guessing a window.
            logger.error("Bybit spot port: unparsable execution cursor %r", cursor)
            return SpotExecutionBatch(conclusive=False)

        start = max(0, watermark - EXECUTION_CURSOR_OVERLAP_MS)
        # The window is anchored at the OVERLAPPED start — anchoring at the
        # raw watermark would request WINDOW + OVERLAP milliseconds and blow
        # the endpoint's 7-day span limit on a long catch-up gap.
        window_end = min(start + EXECUTION_WINDOW_MS, now_ms)
        rows, conclusive = await self._drain_window(start, window_end)
        if not conclusive:
            return SpotExecutionBatch(
                executions=tuple(rows), conclusive=False,
            )
        # The window was drained completely, so the watermark advances to
        # its end even with no fills in it (a long quiet gap would
        # otherwise be re-walked window by window forever); the fixed
        # overlap on the next read covers list-indexing lag at the edge.
        return SpotExecutionBatch(
            executions=tuple(rows),
            next_cursor=str(window_end),
            has_more=window_end < now_ms,
        )

    async def fetch_base_balance(self) -> Decimal:
        """The account's TOTAL base-coin holdings (locked included).

        ``walletBalance`` is the unified account's total per-coin balance
        including amounts locked in open spot orders (the separate
        ``locked`` field is an informational subset) — verified live in
        the M2 smoke test with a resting sell order.
        """
        entry = await self._plugin._fetch_wallet_coin(self.base_asset)
        raw = str(entry.get('walletBalance') or '0') or '0'
        return Decimal(raw)

    # --- internals -----------------------------------------------------------

    async def _drain_window(
            self, start_ms: int, end_ms: int,
    ) -> tuple[list[SpotExecution], bool]:
        """Drain every execution page of one time window (newest-first API)."""
        rows: list[SpotExecution] = []
        cursor: str | None = None
        for _ in range(_MAX_WINDOW_PAGES):
            try:
                result = await self._plugin._call('/v5/execution/list', {
                    'category': self._market.category,
                    'symbol': self._market.symbol,
                    'startTime': start_ms,
                    'endTime': end_ms,
                    'execType': 'Trade',
                    'limit': EXECUTION_PAGE_LIMIT,
                    'cursor': cursor,
                }, auth=True)
            except BybitError:
                # Transient read trouble: the manager treats a raised
                # error as "abort the read"; inconclusive is the safe
                # in-band equivalent mid-pagination.
                logger.warning(
                    "Bybit spot port: execution/list read failed for %s",
                    self.product_id, exc_info=True,
                )
                return rows, False
            for entry in result.get('list') or []:
                execution = self.to_execution(entry)
                if execution is not None:
                    rows.append(execution)
            cursor = result.get('nextPageCursor') or None
            if not cursor:
                return rows, True
        logger.error(
            "Bybit spot port: execution window exceeded %d pages for %s; "
            "treating as inconclusive", _MAX_WINDOW_PAGES, self.product_id,
        )
        return rows, False

    def to_execution(self, entry: dict) -> SpotExecution | None:
        """Map one execution row (REST list or WS push) to a ledger fill.

        ``None`` for rows that are not the bot's own attributable trades:
        non-``Trade`` exec types (defensive — the REST request already
        filters, the WS push does not), fills without an attributable
        ``orderLinkId`` / ``orderId``, and rows for other symbols. The
        REST list rows and the private-stream push rows share their field
        names, so the event stream reuses this builder.
        """
        if str(entry.get('execType') or 'Trade') != 'Trade':
            return None
        if str(entry.get('symbol') or '') != self._market.symbol:
            return None
        coid = self._attribute(entry)
        if coid is None:
            return None
        try:
            side = str(entry.get('side') or '').lower()
            qty = Decimal(str(entry.get('execQty') or ''))
            price = Decimal(str(entry.get('execPrice') or ''))
            value = Decimal(str(entry.get('execValue') or '')) \
                if entry.get('execValue') else qty * price
            fee = Decimal(str(entry.get('execFee') or '0') or '0')
            ts_ms = int(entry.get('execTime') or 0)
        except (ArithmeticError, TypeError, ValueError):
            logger.error("Bybit spot port: unparsable execution row %r", entry)
            return None
        fee_currency = str(entry.get('feeCurrency') or '')
        if not fee_currency:
            # Spot default fee schedule: a buy pays fee in the received
            # base coin, a sell in the received quote coin.
            fee_currency = (self._market.base_coin if side == 'buy'
                            else self._market.quote_coin)
        base_fee = fee if fee_currency == self._market.base_coin else Decimal(0)
        quote_fee = fee if fee_currency == self._market.quote_coin else Decimal(0)
        if side == 'buy':
            base_delta = qty - base_fee
            quote_delta = -value - quote_fee
        else:
            base_delta = -qty - base_fee
            quote_delta = value - quote_fee
        exec_id = str(entry.get('execId') or '')
        # ``seq`` (the venue's monotonic execution sequence) is present on
        # the WS push but not on the list rows; the numeric ``execId``
        # serves as the same-millisecond tie-breaker there.
        seq_raw = str(entry.get('seq') or '') or exec_id
        try:
            return SpotExecution(
                fill_id=exec_id,
                side=side,
                base_delta=base_delta,
                quote_delta=quote_delta,
                price=price,
                fee_amount=fee,
                fee_currency=fee_currency,
                ts_ms=ts_ms,
                exchange_order_id=str(entry.get('orderId') or '') or None,
                client_order_id=coid,
                venue_seq=int(seq_raw) if seq_raw.isdigit() else None,
            )
        except ValueError:
            # Fail-closed validation of the fill shape (sign/side mismatch,
            # zero quantity, ...) — an execution the ledger cannot represent
            # must not kill the caller's loop; it is logged and left to the
            # balance invariant to surface if it moved real inventory.
            logger.exception(
                "Bybit spot port: execution row failed ledger validation: %r",
                entry,
            )
            return None

    def _attribute(self, entry: dict) -> str | None:
        """Resolve a fill to the bot's own client-order-id, or ``None``.

        A raw ``orderId`` alone is not proof of bot ownership; the echoed
        ``orderLinkId`` is checked against the plugin's identity map and
        the BrokerStore, with the ``order_id`` ref index as fallback for
        a dispatch whose ack was lost before the identity was recorded.
        """
        plugin = self._plugin
        link_id = str(entry.get('orderLinkId') or '')
        if link_id:
            if link_id in plugin._order_identity:
                return link_id
            if plugin.store_ctx is not None \
                    and plugin.store_ctx.get_order(link_id) is not None:
                return link_id
        order_id = str(entry.get('orderId') or '')
        if order_id and plugin.store_ctx is not None:
            row = plugin.store_ctx.find_by_ref('order_id', order_id)
            if row is not None:
                return row.client_order_id
        return None
