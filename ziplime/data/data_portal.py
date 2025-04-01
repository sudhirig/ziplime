import datetime
from operator import mul

from typing import Any

import numpy as np
import polars as pl
import pandas as pd
from pandas import isnull
from functools import reduce, lru_cache

from ziplime.assets.domain.db.asset import Asset
from ziplime.assets.domain.equity import Equity
from ziplime.assets.domain.future import Future

from ziplime.assets.domain.continuous_future import ContinuousFuture
from zipline.data.continuous_future_reader import (
    ContinuousFutureSessionBarReader,
    ContinuousFutureMinuteBarReader,
)
from zipline.assets.roll_finder import (
    CalendarRollFinder,
    VolumeRollFinder,
)

from ziplime.data.abstract_data_bundle import AbstractDataBundle

from zipline.data.resample import (
    DailyHistoryAggregator,
    ReindexMinuteBarReader,
    ReindexSessionBarReader,
)
from zipline.data.bar_reader import NoDataOnDate

from zipline.utils.memoize import remember_last
from zipline.errors import HistoryWindowStartsBeforeData

from ziplime.data.domain.bundle_data import BundleData
from ziplime.domain.data_frequency import DataFrequency


class DataPortal:
    """Interface to all of the data that a zipline simulation needs.

    This is used by the simulation runner to answer questions about the data,
    like getting the prices of assets on a given day or to service history
    calls.

    Parameters
    ----------
    asset_repository : ziplime.assets.repositories.sqlite_asset_repository.SqliteAssetRepository
        The AssetFinder instance used to resolve assets.
    trading_calendar: zipline.utils.calendar.exchange_calendar.TradingCalendar
        The calendar instance used to provide minute->session information.
    first_trading_day : datetime.datetime
        The first trading day for the simulation.
    historical_data_reader : BcolzDailyBarReader, optional
        The daily bar reader for equities. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    future_daily_reader : BcolzDailyBarReader, optional
        The daily bar ready for futures. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    future_minute_reader : BcolzFutureMinuteBarReader, optional
        The minute bar reader for futures. This will be used to service
        minute data backtests or minute history calls. This can be used
        to serve daily calls if no daily bar reader is provided.
    adjustment_reader : SQLiteAdjustmentWriter, optional
        The adjustment reader. This is used to apply splits, dividends, and
        other adjustment data to the raw data from the readers.
    last_available_session : datetime.datetime, optional
        The last session to make available in session-level data.
    last_available_minute : datetime.datetime, optional
        The last minute to make available in minute-level data.
    """

    def __init__(
            self,
            bundle_data: BundleData,
            fundamental_data_reader,
            historical_data_reader: AbstractDataBundle,
            future_daily_reader=None,
            future_minute_reader=None,
    ):
        self._data_reader = historical_data_reader
        self._bundle_data = bundle_data
        # caches of sid -> adjustment list
        self._splits_dict = {}
        self._mergers_dict = {}
        self._dividends_dict = {}
        first_trading_day = min(bundle_data.data["date"]).date()
        last_trading_date = max(bundle_data.data["date"])
        self._first_available_session = first_trading_day

        self._last_available_session = last_trading_date.date()


        self._last_available_minute = last_trading_date

        aligned_future_minute_reader = self._ensure_reader_aligned(future_minute_reader)
        aligned_future_session_reader = self._ensure_reader_aligned(future_daily_reader)

        self._roll_finders = {
            "calendar": CalendarRollFinder(self._bundle_data.trading_calendar, self._bundle_data.asset_repository),
        }

        aligned_minute_readers = {}
        aligned_session_readers = {}

        if aligned_future_minute_reader is not None:
            aligned_minute_readers[Future] = aligned_future_minute_reader
            aligned_minute_readers[ContinuousFuture] = ContinuousFutureMinuteBarReader(
                aligned_future_minute_reader,
                self._roll_finders,
            )

        if aligned_future_session_reader is not None:
            aligned_session_readers[Future] = aligned_future_session_reader
            self._roll_finders["volume"] = VolumeRollFinder(
                self._bundle_data.trading_calendar,
                self._bundle_data.asset_repository,
                aligned_future_session_reader,
            )
            aligned_session_readers[
                ContinuousFuture
            ] = ContinuousFutureSessionBarReader(
                aligned_future_session_reader,
                self._roll_finders,
            )

        self._daily_aggregator = DailyHistoryAggregator(
            self._bundle_data.trading_calendar.first_minutes,
            self._data_reader,
            self._bundle_data.trading_calendar,
        )

        self._first_trading_day = first_trading_day

        # Get the first trading minute
        # self._first_trading_minute = (
        #     self._bundle_data.trading_calendar.session_first_minute(self._first_trading_day)
        #     if self._first_trading_day is not None
        #     else (None, None)
        # )

        # # Store the locs of the first day and first minute
        # self._first_trading_day_loc = (
        #     self._bundle_data.trading_calendar.sessions.get_loc(self._first_trading_day)
        #     if self._first_trading_day is not None
        #     else None
        # )

    def _ensure_reader_aligned(self, reader):
        if reader is None:
            return

        if reader.trading_calendar.name == self._bundle_data.trading_calendar.name:
            return reader
        elif reader.data_frequency == "minute":
            return ReindexMinuteBarReader(
                self._bundle_data.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )
        elif reader.data_frequency == "session":
            return ReindexSessionBarReader(
                self._bundle_data.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )

    # def _get_pricing_reader(self, data_frequency: DataFrequency):
    #     return self._pricing_readers[data_frequency]

    def get_last_traded_dt(self, asset, dt, data_frequency: DataFrequency):
        """Given an asset and dt, returns the last traded dt from the viewpoint
        of the given dt.

        If there is a trade on the dt, the answer is dt provided.
        """
        return self._get_pricing_reader(data_frequency=data_frequency).get_last_traded_dt(asset=asset, dt=dt)

    def _get_single_asset_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                frequency: datetime.timedelta) -> pl.DataFrame:

        return self.get_spot_value(
            assets=[asset],
            fields=[field],
            dt=dt,
            frequency=frequency,
        )
        # if field not in self._fields:
        #     raise KeyError("Invalid column: " + str(field))
        #
        # if (
        #         dt < asset.start_date
        #         or (
        #         data_frequency == "daily" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
        #     asset.end_date, tzinfo=datetime.timezone.utc))
        #         or (
        #         data_frequency == "minute" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
        #     asset.end_date, tzinfo=datetime.timezone.utc))
        # ):
        #     if field == "volume":
        #         return 0
        #     elif field == "contract":
        #         return None
        #     elif field != "last_traded":
        #         return np.nan
        #
        # if data_frequency == "daily":
        #     if field == "contract":
        #         return self._get_current_contract(continuous_future=asset, dt=session_label)
        #     else:
        #         return self._get_daily_spot_value(
        #             asset=asset,
        #             column=field,
        #             dt=session_label,
        #         )
        # else:
        #     if field == "last_traded":
        #         return self.get_last_traded_dt(asset, dt, "minute")
        #     elif field == "price":
        #         return self._get_minute_spot_value(
        #             asset=asset,
        #             column="close",
        #             dt=dt,
        #             ffill=True,
        #         )
        #     elif field == "contract":
        #         return self._get_current_contract(continuous_future=asset, dt=dt)
        #     else:
        #         return self._get_minute_spot_value(asset=asset, column=field, dt=dt)

    def get_spot_value(self, assets: list[Asset], fields: list[str], dt: datetime.datetime,
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
        df_raw = self.load_raw_arrays_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=assets,
            include_end_date=True,
        )
        return df_raw

    @lru_cache
    def get_dataframe(self) -> pl.DataFrame:
        # df = pl.read_parquet(source=self.data_path)

        # df = df.with_columns(pl.col("date").dt.convert_time_zone(self.trading_calendar.tz.key))
        df = self._bundle_data.data
        return df


    def load_raw_arrays_limit(self, fields: list[str], limit: int,
                              end_date: datetime.datetime,
                              frequency: datetime.timedelta,
                              assets: list[Asset],
                              include_end_date: bool,
                              ) -> pl.DataFrame:

        total_bar_count = limit
        if self._bundle_data.frequency < frequency:
            multiplier = int(frequency / self._bundle_data.frequency)
            total_bar_count = limit * multiplier

        cols = list(set(fields + ["date", "sid"]))
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
        if self._bundle_data.frequency < frequency:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
            return df
        return df_raw


    def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                    frequency: datetime.timedelta):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset
            The asset or assets whose data is desired. This cannot be
            an arbitrary AssetConvertible.
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
        return self._get_single_asset_value(
            asset=asset,
            field=field,
            dt=dt,
            frequency=frequency,
        )

    def get_adjustments(self, assets: list[Asset], field: str, dt: datetime.datetime, perspective_dt: datetime.datetime):
        """Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of assets

        Parameters
        ----------
        assets : list of type Asset, or Asset
            The asset, or assets whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.

        Returns
        -------
        adjustments : list[Adjustment]
            The adjustments to that field.
        """
        adjustment_ratios_per_asset = []

        def split_adj_factor(x):
            return x if field != "volume" else 1.0 / x

        for asset in assets:
            adjustments_for_asset = []
            split_adjustments = self._get_adjustment_list(
                asset, self._splits_dict, "SPLITS"
            )
            for adj_dt, adj in split_adjustments:
                if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                    adjustments_for_asset.append(split_adj_factor(adj))
                elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                    break

            if field != "volume":
                merger_adjustments = self._get_adjustment_list(
                    asset, self._mergers_dict, "MERGERS"
                )
                for adj_dt, adj in merger_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt > perspective_dt:
                        break

                dividend_adjustments = self._get_adjustment_list(
                    asset,
                    self._dividends_dict,
                    "DIVIDENDS",
                )
                for adj_dt, adj in dividend_adjustments:
                    if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                        break

            ratio = reduce(mul, adjustments_for_asset, 1.0)
            adjustment_ratios_per_asset.append(ratio)

        return adjustment_ratios_per_asset

    def get_adjusted_value(
            self, asset: Asset, field: str, dt: datetime.datetime, perspective_dt: datetime.datetime,
            data_frequency: datetime.timedelta,
            spot_value: float = None
    ):
        """Returns a scalar value representing the value
        of the desired asset's field at the given dt with adjustments applied.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The value of the given ``field`` for ``asset`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open',
            'high', 'low', 'close', or 'price', the value will be a float. If
            the ``field`` is 'volume' the value will be a int. If the ``field``
            is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            spot_value = self.get_spot_value(assets=[asset], fields=[field], dt=dt, data_frequency=data_frequency)

        if isinstance(asset, Equity):
            ratio = self.get_adjustments(assets=[asset], field=field, dt=dt, perspective_dt=perspective_dt)[0]
            spot_value *= ratio

        return spot_value


    @remember_last
    def _get_days_for_window(self, end_date: datetime.datetime, bar_count: int):
        tds = self._bundle_data.trading_calendar.sessions
        end_loc = tds.get_loc(end_date)
        start_loc = end_loc - bar_count + 1
        if start_loc < self._first_trading_day_loc:
            raise HistoryWindowStartsBeforeData(
                first_trading_day=self._first_trading_day.date(),
                max_bar_count=self._first_trading_day_loc - start_loc,
                bar_count=bar_count,
                suggested_start_day=tds[min(self._first_trading_day_loc + bar_count, end_loc)].date(),
            )
        return tds[start_loc: end_loc + 1]


    def get_history_window(
            self, assets: list[Asset],
            end_dt: datetime.datetime,
            bar_count: int,
            fields: list[str],
            frequency: datetime.timedelta,
            include_end: bool = False,
            ffill: bool = True
    ):
        """Public API method that returns a dataframe containing the requested
        history window.  Data is fully adjusted.

        Parameters
        ----------
        assets : list of zipline.data.Asset objects
            The assets whose data is desired.

        bar_count: int
            The number of bars desired.

        frequency: string
            "1d" or "1m"

        field: string
            The desired field of the asset.

        data_frequency: string
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars.

        ffill: boolean
            Forward-fill missing values. Only has effect if field
            is 'price'.

        Returns
        -------
        A dataframe containing the requested data.
        """
        if bar_count < 1:
            raise ValueError(f"bar_count must be >= 1, but got {bar_count}")
        df_raw = self.load_raw_arrays_limit(
            fields=fields,
            limit=bar_count,
            frequency=frequency,
            end_date=end_dt,
            assets=assets,
            include_end_date=False
        )
        return df_raw

    def _get_adjustment_list(self, asset: Asset, adjustments_dict: dict[str, Any], table_name: str):
        """Internal method that returns a list of adjustments for the given sid.

        Parameters
        ----------
        asset : Asset
            The asset for which to return adjustments.

        adjustments_dict: dict
            A dictionary of sid -> list that is used as a cache.

        table_name: string
            The table that contains this data in the adjustments db.

        Returns
        -------
        adjustments: list
            A list of [multiplier, datetime.datetime], earliest first

        """
        if self._bundle_data.adjustment_repository is None:
            return []

        sid = asset.sid

        try:
            adjustments = adjustments_dict[sid]
        except KeyError:
            adjustments = adjustments_dict[
                sid
            ] = self._bundle_data.adjustment_repository.get_adjustments_for_sid(table_name, sid)

        return adjustments

    def get_splits(self, assets: list[Asset], dt: datetime.date):
        """Returns any splits for the given sids and the given dt.

        Parameters
        ----------
        assets : container
            Assets for which we want splits.
        dt : datetime.datetime
            The date for which we are checking for splits. Note: this is
            expected to be midnight UTC.

        Returns
        -------
        splits : list[(asset, float)]
            List of splits, where each split is a (asset, ratio) tuple.
        """
        if self._bundle_data.adjustment_repository is None or not assets:
            return []

        # convert dt to # of seconds since epoch, because that's what we use
        # in the adjustments db
        # seconds = int(dt.value / 1e9)

        splits = self._bundle_data.adjustment_repository.conn.execute(
            "SELECT sid, ratio FROM SPLITS WHERE effective_date = ?", (dt,)
        ).fetchall()

        splits = [split for split in splits if split[0] in assets]
        splits = [
            (self._bundle_data.asset_repository.retrieve_asset(split[0]), split[1]) for split in splits
        ]

        return splits

    def get_stock_dividends(self, sid: int, trading_days: pd.DatetimeIndex):
        """Returns all the stock dividends for a specific sid that occur
        in the given trading range.

        Parameters
        ----------
        sid: int
            The asset whose stock dividends should be returned.

        trading_days: pd.DatetimeIndex
            The trading range.

        Returns
        -------
        list: A list of objects with all relevant attributes populated.
        All timestamp fields are converted to datetime.datetime.
        """

        if self._adjustment_reader is None:
            return []

        if len(trading_days) == 0:
            return []

        start_dt = trading_days[0]
        end_dt = trading_days[-1]

        dividends = self._adjustment_reader.conn.execute(
            "SELECT declared_date, ex_date, pay_date, payment_sid, ratio, "
            "record_date, sid FROM stock_dividend_payouts "
            "WHERE sid = ? AND ex_date > ? AND pay_date < ?",
            (
                int(sid),
                start_dt,
                end_dt,
            ),
        ).fetchall()

        dividend_info = []
        for dividend_tuple in dividends:
            dividend_info.append(
                {
                    "declared_date": pd.Timestamp(dividend_tuple[0], unit="s").to_pydatetime(),
                    "ex_date": pd.Timestamp(dividend_tuple[1], unit="s").to_pydatetime(),
                    "pay_date": pd.Timestamp(dividend_tuple[2], unit="s").to_pydatetime(),
                    "payment_sid": dividend_tuple[3],
                    "ratio": dividend_tuple[4],
                    "record_date": pd.Timestamp(dividend_tuple[5], unit="s").to_pydatetime(),
                    "sid": dividend_tuple[6],
                }
            )

        return dividend_info

    def get_fetcher_assets(self, dt: datetime.datetime):
        """Returns a list of assets for the current date, as defined by the
        fetcher data.

        Returns
        -------
        list: a list of Asset objects.
        """
        # return a list of assets for the current date, as defined by the
        # fetcher source
        if self._extra_source_df is None:
            return []

        # TODO: FIX THIS TZ MESS!
        day = dt.normalize().tz_localize(None)

        if day in self._extra_source_df.index:
            assets = self._extra_source_df.loc[day]["sid"]
        else:
            return []

        if isinstance(assets, pd.Series):
            return [x for x in assets if isinstance(x, Asset)]
        else:
            return [assets] if isinstance(assets, Asset) else []

    def get_current_future_chain(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        """Retrieves the future chain for the contract at the given `dt` according
        the `continuous_future` specification.

        Returns
        -------

        future_chain : list[Future]
            A list of active futures, where the first index is the current
            contract specified by the continuous future definition, the second
            is the next upcoming contract and so on.
        """
        rf = self._roll_finders[continuous_future.roll_style]
        session = self._bundle_data.trading_calendar.minute_to_session(dt)
        contract_center = rf.get_contract_center(
            continuous_future.root_symbol, session, continuous_future.offset
        )
        oc = self._bundle_data.asset_repository.get_ordered_contracts(continuous_future.root_symbol)
        chain = oc.active_chain(contract_center, session.value)
        return self._bundle_data.asset_repository.retrieve_all(sids=chain)

    def _get_current_contract(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_sid = rf.get_contract_center(
            continuous_future.root_symbol, dt, continuous_future.offset
        )
        if contract_sid is None:
            return None
        return self._bundle_data.asset_repository.retrieve_asset(sid=contract_sid)

