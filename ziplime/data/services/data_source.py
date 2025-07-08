import datetime
import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.constants.period import Period
from ziplime.utils.date_utils import period_to_timedelta


class DataSource:

    def __init__(self, name: str, start_date: datetime.date, end_date: datetime.date,
                 frequency: datetime.timedelta | Period, ):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.frequency_td = period_to_timedelta(self.frequency)

    def get_dataframe(self) -> pl.DataFrame:
        return self.data

    def get_data_by_date(self, fields: frozenset[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta | Period,
                         assets: frozenset[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:

        cols = set(fields.union({"date", "sid"}))
        if include_bounds:
            df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= to_date,
                pl.col("date") >= from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).all()
        else:
            df = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < to_date,
                pl.col("date") > from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).all()
        if self.frequency < frequency:
            df = df.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
        return df.sort(by="date")

    # @lru_cache(maxsize=100)
    def get_data_by_limit(self, fields: frozenset[str] | None,
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta | Period,
                          assets: frozenset[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:
        frequency_td = period_to_timedelta(frequency)
        total_bar_count = limit
        if end_date > self.end_date:
            return self.get_missing_data_by_limit(frequency=frequency, assets=assets, fields=fields,
                                                  limit=limit, include_end_date=include_end_date,
                                                  end_date=end_date
                                                  )  # pl.DataFrame() # we have missing data

        if self.frequency_td < frequency_td:
            multiplier = int(frequency_td / self.frequency_td)
            total_bar_count = limit * multiplier
        df = self.get_dataframe()
        if fields is None:
            fields = frozenset(df.columns)
        cols = list(fields.union({"date", "sid"}))

        if include_end_date:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).tail(total_bar_count).sort(by="date")
        else:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).tail(
                total_bar_count).sort(by="date")

        if self.frequency_td < frequency_td:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields).tail(
                limit)
            return df
        return df_raw

    def get_spot_value(self, assets: frozenset[Asset], fields: frozenset[str], dt: datetime.datetime,
                       frequency: datetime.timedelta):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset, ContinuousFuture, or iterable of same.
            The asset or assets whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        # print(f"get spot value: {assets}, {fields}, {dt}")
        df_raw = self.get_data_by_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=assets,
            include_end_date=True,
        )
        return df_raw

