import datetime
from typing import Any

from ziplime.data.domain.bundle_data import BundleData
from ziplime.finance.domain.ledger import Ledger


class Positions:
    """Tracks daily positions."""

    def end_of_bar(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                   bundle_data: BundleData):
        packet["minute_perf"]["positions"] = ledger.positions(session)

    def end_of_session(self, packet: dict[str, Any], ledger: Ledger, session: datetime.datetime, session_ix: int,
                       bundle_data: BundleData):
        packet["daily_perf"]["positions"] = ledger.positions(dt=session)
