import datetime
from typing import Any

from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger


class PeriodLabel:
    """Backwards compat, please kill me."""

    def start_of_session(self, ledger, session, bundle_data: BundleData):
        self._label = session.strftime("%Y-%m")

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        packet["cumulative_risk_metrics"]["period_label"] = self._label

    end_of_session = end_of_bar
