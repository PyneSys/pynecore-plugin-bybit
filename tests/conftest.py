"""Shared fixtures of the Bybit plugin tests."""
import sys

import pytest

from pynecore.core.broker.models import OrderEvent


@pytest.fixture(autouse=True)
def _fill_events_carry_the_journal_quantity(monkeypatch):
    """Every fill event the plugin builds carries ``journal_fill_qty``.

    The core's restart check compares the fills it booked with the journal
    rows' ``filled_qty`` through that figure; a fill event without it is a
    port-contract violation the engine can only report.
    """
    init = OrderEvent.__init__

    def checked(self, *args, **kwargs):
        init(self, *args, **kwargs)
        caller = sys._getframe(1).f_globals.get('__name__', '')
        if (caller.startswith('pynecore_bybit') and (self.fill_qty or 0.0) > 0.0
                and self.journal_fill_qty is None):
            raise AssertionError(f"{caller} built a fill event without journal_fill_qty")

    monkeypatch.setattr(OrderEvent, '__init__', checked)
