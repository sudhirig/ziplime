import datetime
import polars as pl
from ziplime.assets.entities.asset import Asset
from ziplime.constants.period import Period


class DataSource:

    def __init__(self, name: str):
        self.name = name

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:
        ...

    def get_data_by_limit(self, fields: frozenset[str] | None,
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta | Period,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        ...

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime,
                       frequency: datetime.timedelta):
        ...
