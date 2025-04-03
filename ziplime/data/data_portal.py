import datetime
from operator import mul

from typing import Any

import polars as pl
import pandas as pd
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

    def get_last_traded_dt(self, asset, dt, data_frequency: datetime.timedelta):
        """Given an asset and dt, returns the last traded dt from the viewpoint
        of the given dt.

        If there is a trade on the dt, the answer is dt provided.
        """
        return self._get_pricing_reader(data_frequency=data_frequency).get_last_traded_dt(asset=asset, dt=dt)



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
        df_raw = self._bundle_data.get_data_by_limit(
            fields=fields,
            limit=bar_count,
            frequency=frequency,
            end_date=end_dt,
            assets=assets,
            include_end_date=False
        )
        return df_raw
