import datetime

from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.db.asset import Asset
from ziplime.domain.column_specification import ColumnSpecification
import polars as pl


class AbstractDataBundle:
    def write(
            self, data, calendar: ExchangeCalendar, start_session: datetime.datetime, end_session: datetime.datetime,
            cols: list[ColumnSpecification],
            validate_sessions: bool,
            data_frequency: datetime.timedelta,
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
