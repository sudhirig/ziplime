import datetime

import pandas as pd
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.db.asset import Asset
from ziplime.domain.column_specification import ColumnSpecification
from ziplime.domain.data_frequency import DataFrequency
import polars as pl


class AbstractDataBundle:
    def write(
            self, data, calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
            cols: list[ColumnSpecification],
            validate_sessions: bool,
            data_frequency: DataFrequency,
            assets=None, show_progress=False, invalid_data_behavior="warn",
            **kwargs
    ):
        pass

    def load_raw_arrays_limit(self, fields: list[str], limit: int, end_date: datetime.datetime,
                              frequency: datetime.timedelta,
                              assets: list[Asset],
                              include_end_date: bool,
                              ) -> pl.DataFrame:
        pass
