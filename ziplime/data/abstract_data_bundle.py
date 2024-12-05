import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.domain.column_specification import ColumnSpecification


class AbstractDataBundle:
    def write(
            self, data, calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
            cols: list[ColumnSpecification],
            validate_sessions: bool,
            assets=None, show_progress=False, invalid_data_behavior="warn",
            **kwargs
    ):
        pass
