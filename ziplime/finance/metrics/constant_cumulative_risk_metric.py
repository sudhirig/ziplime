import datetime
from typing import Any

from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger


class ConstantCumulativeRiskMetric:
    """A metric which does not change, ever.

    Notes
    -----
    This exists to maintain the existing structure of the perf packets. We
    should kill this as soon as possible.
    """

    def __init__(self, field, value):
        self._field = field
        self._value = value

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        packet["cumulative_risk_metrics"][self._field] = self._value

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       bundle_data: BundleData):
        packet["cumulative_risk_metrics"][self._field] = self._value
