from dataclasses import asdict
from functools import lru_cache
from pathlib import Path

import orjson
import structlog
from exchange_calendars import ExchangeCalendar
from zipline.data._minute_bar_internal import find_position_of_minute
import polars as pl

from ziplime.assets.domain.asset import Asset
from ziplime.data.abstract_data_bundle import AbstractDataBundle
from ziplime.domain.column_specification import ColumnSpecification

import numpy as np

import datetime
import pandas as pd

from zipline.data.bar_reader import NoDataAfterDate, NoDataBeforeDate, NoDataOnDate
from zipline.utils.calendar_utils import get_calendar
from zipline.utils.cli import maybe_show_progress


class PolarsDataBundle(AbstractDataBundle):
    """Class capable of writing daily OHLCV data to disk in a format that can
    be read efficiently by BcolzDailyOHLCVReader.

    Parameters
    ----------
    root_directory : str
        The location at which we should write our output.
    """

    def __init__(self, root_directory: str):
        super().__init__()
        self._root_directory = root_directory
        self._logger = structlog.getLogger(__name__)
        self._spot_cols = {}
        self.data_path = Path(self._root_directory).joinpath("data.parquet")
        self.metadata_path = Path(self._root_directory).joinpath("metadata.json")

    def write_metadata(self):
        pass

    @lru_cache
    def get_metadata(self):
        with open(self.metadata_path, "r") as f:
            metadata = orjson.loads(f.read())
        return metadata

    @property
    @lru_cache
    def sessions(self):
        calendar = get_calendar(self.get_metadata()["calendar_name"])
        start_session_ns = self.get_metadata()["start_session_ns"]

        start_session = pd.Timestamp(start_session_ns)

        end_session_ns = self.get_metadata()["end_session_ns"]
        end_session = pd.Timestamp(end_session_ns)

        sessions = calendar.sessions_in_range(start_session, end_session)

        return sessions

    @property
    @lru_cache
    def _first_rows(self):
        return {
            int(asset_id): start_index
            for asset_id, start_index in self.get_metadata()["first_row"].items()
        }

    @property
    @lru_cache
    def _last_rows(self):
        return {
            int(asset_id): end_index
            for asset_id, end_index in self.get_metadata()["last_row"].items()
        }

    @property
    @lru_cache
    def _calendar_offsets(self):
        return {
            int(id_): offset
            for id_, offset in self.get_metadata()["calendar_offset"].items()
        }

    @property
    @lru_cache
    def first_trading_day(self):
        return datetime.datetime.strptime(self.get_metadata()["first_trading_day"], "%Y-%m-%d")

    @property
    @lru_cache
    def trading_calendar(self):
        return get_calendar(self.get_metadata()["calendar_name"])

    @property
    @lru_cache
    def _column_specification(self):
        cols = [
            ColumnSpecification(
                name=col["name"],
                original_type=col["original_type"],
                write_type=col["write_type"],
                scaled_type=col["scaled_type"],
                scale_factor=col["scale_factor"]
            )
            for col in self.get_metadata()["column_specification"]
        ]
        return {
            col.name: col
            for col in cols
        }

    @property
    @lru_cache
    def last_available_dt(self):
        return self.sessions[-1]

    def load_raw_arrays(self, fields: list[str], start_date: pd.Timestamp, end_date: pd.Timestamp,
                        assets: list[Asset]):
        df_cols = list(set(fields + ["date", "sid"]))
        df = self.get_dataframe()[df_cols]
        start_date_tz = start_date.to_pydatetime().replace(tzinfo=self.trading_calendar.tz)
        end_date_tz = end_date.to_pydatetime().replace(tzinfo=self.trading_calendar.tz)
        res = df.filter(pl.col("date").is_between(start_date_tz, end_date_tz),
                        pl.col("sid").is_in([asset.sid for asset in assets]))
        return res

    @lru_cache
    def get_dataframe(self) -> pl.DataFrame:
        df = pl.read_parquet(source=self.data_path)
        df = df.with_columns(pl.col("date").dt.convert_time_zone(self.trading_calendar.tz.key))
        return df

    def _spot_col(self, colname):
        """Get the colname from daily_bar_table and read all of it into memory,
        caching the result.

        Parameters
        ----------
        colname : string
            A name of a OHLCV carray in the daily_bar_table

        Returns
        -------
        array (uint32)
            Full read array of the carray in the daily_bar_table with the
            given colname.
        """
        try:
            col = self._spot_cols[colname]
        except KeyError:
            col = self._spot_cols[colname] = self.get_dataframe()[colname]
        return col

    def get_last_traded_dt(self, asset, day):
        volumes = self._spot_col("volume")

        search_day = day

        while True:
            try:
                ix = self.sid_day_index(asset.sid, search_day)
            except NoDataBeforeDate:
                return pd.NaT
            except NoDataAfterDate:
                prev_day_ix = self.sessions.get_loc(search_day) - 1
                if prev_day_ix > -1:
                    search_day = self.sessions[prev_day_ix]
                continue
            except NoDataOnDate:
                return pd.NaT
            if volumes[ix] != 0:
                return search_day
            prev_day_ix = self.sessions.get_loc(search_day) - 1
            if prev_day_ix > -1:
                search_day = self.sessions[prev_day_ix]
            else:
                return pd.NaT

    def sid_day_index(self, sid, day):
        """

        Parameters
        ----------
        sid : int
            The asset identifier.
        day : datetime64-like
            Midnight of the day for which data is requested.

        Returns
        -------
        int
            Index into the data tape for the given sid and day.
            Raises a NoDataOnDate exception if the given day and sid is before
            or after the date range of the equity.
        """
        try:
            day_loc = self.sessions.get_loc(day)
        except Exception as exc:
            raise NoDataOnDate(
                "day={0} is outside of calendar={1}".format(day, self.sessions)
            ) from exc
        offset = day_loc - self._calendar_offsets[sid]
        if offset < 0:
            raise NoDataBeforeDate(
                "No data on or before day={0} for sid={1}".format(day, sid)
            )
        ix = self._first_rows[sid] + offset
        if ix > self._last_rows[sid]:
            raise NoDataAfterDate(
                "No data on or after day={0} for sid={1}".format(day, sid)
            )
        return ix

    def get_value(self, sid: int, dt: pd.Timestamp, field: str):
        """

        Parameters
        ----------
        sid : int
            The asset identifier.
        dt : datetime64-like
            Midnight of the day for which data is requested.
        field : string
            The price field. e.g. ('open', 'high', 'low', 'close', 'volume')

        Returns
        -------
        float
            The spot price for colname of the given sid on the given day.
            Raises a NoDataOnDate exception if the given day and sid is before
            or after the date range of the equity.
            Returns -1 if the day is within the date range, but the price is
            0.
        """

        try:
            minute_pos = self._find_position_of_minute(dt)
        except ValueError as exc:
            raise NoDataOnDate() from exc

        self._last_get_value_dt_value = dt.value
        self._last_get_value_dt_position = minute_pos

        try:
            value = self._open_minute_file(field, sid)[minute_pos]
        except IndexError:
            value = 0
        if value == 0:
            if field == "volume":
                return 0
            else:
                return np.nan

        if field != "volume":
            value *= self._ohlc_ratio_inverse_for_sid(sid)
        return value

        ix = self.sid_day_index(sid, dt)
        price = self._spot_col(field)[ix]
        if field != "volume":
            if price == 0:
                return np.nan
            else:
                return price * 0.001
        else:
            return price

    def progress_bar_item_show_func(self, value):
        return value if value is None else str(value[0])

    def write(
            self, data, calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
            cols: list[ColumnSpecification],
            validate_sessions: bool,
            assets=None, show_progress=False, invalid_data_behavior="warn",
            **kwargs
    ):
        """

        Parameters
        ----------
        data : iterable[tuple[int, pandas.DataFrame or bcolz.ctable]]
            The data chunks to write. Each chunk should be a tuple of sid
            and the data for that asset.
        assets : set[int], optional
            The assets that should be in ``data``. If this is provided
            we will check ``data`` against the assets and provide better
            progress information.
        show_progress : bool, optional
            Whether or not to show a progress bar while writing.
        invalid_data_behavior : {'warn', 'raise', 'ignore'}, optional
            What to do when data is encountered that is outside the range of
            a uint32.

        Returns
        -------
        table : bcolz.ctable
            The newly-written table.
        """
        with maybe_show_progress(
                ((sid, df) for sid, df in data),
                show_progress=show_progress,
                item_show_func=self.progress_bar_item_show_func,
                label="Persisting data",
                length=len(assets) if assets is not None else None,
        ) as it:
            return self._write_internal(iterator=it, assets=assets, calendar=calendar,
                                        start_session=start_session, end_session=end_session,
                                        cols=cols, show_progress=show_progress,
                                        validate_sessions=validate_sessions
                                        )

    def _write_internal(self, iterator, assets,
                        calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
                        show_progress,
                        cols: list[ColumnSpecification],
                        validate_sessions: bool
                        ):
        """Internal implementation of write.

        `iterator` should be an iterator yielding pairs of (asset, ctable).
        """
        total_rows = 0
        first_row = {}
        last_row = {}
        calendar_offset = {}

        earliest_date = None
        sessions = calendar.sessions_in_range(
            start=start_session, end=end_session
        )

        types_map = {
            "float64": pl.Float64,
            "float32": pl.Float32,
            "int": pl.Int64,
            "date": pl.Datetime,
            "datetime": pl.Datetime(time_unit="ns", time_zone="UTC")
        }
        col_types = {c.name: types_map[c.original_type] for c in cols}
        col_types["date"] = pl.Datetime(time_unit="ns", time_zone="UTC")
        col_types["sid"] = pl.Int64
        column_names = [c.name for c in cols] + ["date", "sid"]
        final_df = pl.DataFrame({
            c: [] for c in column_names
        },
            schema=col_types,
        )

        for asset_id, df_table in iterator:

            if assets is not None and asset_id not in assets:
                raise ValueError("unknown asset id %r" % asset_id)
            df_table = df_table.reset_index(drop=False, inplace=False)
            # df_table['date'] = pd.to_datetime(df_table['date'])
            df_table["sid"] = asset_id
            table = df_table[column_names]
            table = table.loc[~table[column_names].duplicated(keep="first")]
            nrows = len(table)
            first_date = table.iloc[0]["date"].date()
            last_date = table.iloc[-1]["date"].date()
            if earliest_date is None:
                earliest_date = first_date
            else:
                earliest_date = min(earliest_date, first_date)

            # Bcolz doesn't support ints as keys in `attrs`, so convert
            # assets to strings for use as attr keys.
            asset_key = str(asset_id)

            # Calculate the index into the array of the first and last row
            # for this asset. This allows us to efficiently load single
            # assets when querying the data back out of the table.
            first_row[asset_key] = total_rows
            last_row[asset_key] = total_rows + nrows - 1
            total_rows += nrows

            asset_first_day = pd.Timestamp(first_date, unit="s").normalize()
            asset_last_day = pd.Timestamp(last_date, unit="s").normalize()

            asset_sessions = sessions[
                sessions.slice_indexer(asset_first_day, asset_last_day)
            ]
            if validate_sessions and len(table) != len(asset_sessions):
                missing_sessions = asset_sessions.difference(
                    pd.to_datetime(np.array(table["date"])),  # unit="s")
                ).tolist()

                extra_sessions = (
                    pd.to_datetime(np.array(table["date"]), unit="s")
                    .difference(asset_sessions)
                    .tolist()
                )
                raise AssertionError(
                    f"Got {len(table)} rows for daily bars table with "
                    f"first day={asset_first_day.date()}, last "
                    f"day={asset_last_day.date()}, expected {len(asset_sessions)} rows.\n"
                    f"Missing sessions: {missing_sessions}\nExtra sessions: {extra_sessions}"
                )

            # Calculate the number of trading days between the first date
            # in the stored data and the first date of **this** asset. This
            # offset used for output alignment by the reader.
            calendar_offset[asset_key] = sessions.get_loc(asset_first_day)
            final_df.extend(pl.from_pandas(schema_overrides=col_types, data=table))

        final_df.write_parquet(self.data_path)
        metadata = {
            "first_row": first_row,
            "last_row": last_row,
            "calendar_offset": calendar_offset,
            "calendar_name": calendar.name,
            "start_session_ns": start_session.value,
            "end_session_ns": end_session.value,
            "first_trading_day": earliest_date,
            "column_specification": [
                asdict(col) for col in cols
            ]
        }
        with open(self.metadata_path, "wb") as f:
            json_metadata = orjson.dumps(metadata)
            f.write(json_metadata)

        return final_df

    @lru_cache
    def get_fields(self) -> list[str]:
        return [col["name"] for col in self.get_metadata()["column_specification"]] + ["price"]
