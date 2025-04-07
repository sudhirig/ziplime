import datetime
from typing import Any

import pandas as pd
from exchange_calendars import ExchangeCalendar


from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger
from ziplime.sources.benchmark_source import BenchmarkSource




class CashFlow:
    """Tracks daily and cumulative cash flow.

    Notes
    -----
    For historical reasons, this field is named 'capital_used' in the packets.
    """

    def start_of_simulation(
            self, ledger: Ledger, emission_rate: datetime.timedelta, trading_calendar: ExchangeCalendar,
            sessions: pd.DatetimeIndex, benchmark_source: BenchmarkSource
    ):
        self._previous_cash_flow = 0.0

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        cash_flow = ledger.portfolio.cash_flow
        packet["minute_perf"]["capital_used"] = cash_flow - self._previous_cash_flow
        packet["cumulative_perf"]["capital_used"] = cash_flow

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       bundle_data: BundleData):
        cash_flow = ledger.portfolio.cash_flow
        packet["daily_perf"]["capital_used"] = cash_flow - self._previous_cash_flow
        packet["cumulative_perf"]["capital_used"] = cash_flow
        self._previous_cash_flow = cash_flow
