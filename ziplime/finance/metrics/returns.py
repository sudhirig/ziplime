import datetime
from functools import partial
from typing import Any

from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger


class Returns:
    """Tracks the daily and cumulative returns of the algorithm."""

    # def _end_of_period(field, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
    #                    bundle_data: BundleData):
    #     packet[field]["returns"] = ledger.todays_returns
    #     packet["cumulative_perf"]["returns"] = ledger.portfolio.returns
    #     packet["cumulative_risk_metrics"][
    #         "algorithm_period_return"
    #     ] = ledger.portfolio.returns

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        packet["minute_perf"]["returns"] = ledger.todays_returns
        packet["cumulative_perf"]["returns"] = ledger.portfolio.returns
        packet["cumulative_risk_metrics"][
            "algorithm_period_return"
        ] = ledger.portfolio.returns

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       bundle_data: BundleData):
        packet["daily_perf"]["returns"] = ledger.todays_returns
        packet["cumulative_perf"]["returns"] = ledger.portfolio.returns
        packet["cumulative_risk_metrics"][
            "algorithm_period_return"
        ] = ledger.portfolio.returns
    # end_of_bar = partial(_end_of_period, "minute_perf")
    # end_of_session = partial(_end_of_period, "daily_perf")
