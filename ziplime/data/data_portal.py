#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from operator import mul

import logging
from typing import Any

import numpy as np
from numpy import float64, int64, nan
import pandas as pd
from pandas import isnull
from functools import reduce

from ziplime.assets.domain.asset import Asset
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

from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.data.dispatch_bar_reader import (
    AssetDispatchMinuteBarReader,
    AssetDispatchSessionBarReader,
)
from zipline.data.resample import (
    DailyHistoryAggregator,
    ReindexMinuteBarReader,
    ReindexSessionBarReader,
)
from ziplime.data.history_loader import (
    PolarsHistoryLoader,
)
from zipline.data.bar_reader import NoDataOnDate

from zipline.utils.memoize import remember_last
from zipline.errors import HistoryWindowStartsBeforeData

from ziplime.domain.data_frequency import DataFrequency
from ziplime.utils.calendar_utils import add_tz_info

log = logging.getLogger("DataPortal")

HISTORY_FREQUENCIES = set(["1m", "1d"])

DEFAULT_MINUTE_HISTORY_PREFETCH = 1560
DEFAULT_DAILY_HISTORY_PREFETCH = 40

_DEF_M_HIST_PREFETCH = DEFAULT_MINUTE_HISTORY_PREFETCH
_DEF_D_HIST_PREFETCH = DEFAULT_DAILY_HISTORY_PREFETCH


class DataPortal:
    """Interface to all of the data that a zipline simulation needs.

    This is used by the simulation runner to answer questions about the data,
    like getting the prices of assets on a given day or to service history
    calls.

    Parameters
    ----------
    asset_repository : ziplime.assets.repositories.asset_repository.AssetRepository
        The AssetFinder instance used to resolve assets.
    trading_calendar: zipline.utils.calendar.exchange_calendar.TradingCalendar
        The calendar instance used to provide minute->session information.
    first_trading_day : pd.Timestamp
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
    last_available_session : pd.Timestamp, optional
        The last session to make available in session-level data.
    last_available_minute : pd.Timestamp, optional
        The last minute to make available in minute-level data.
    """

    def __init__(
            self,
            asset_repository: AssetRepository,
            trading_calendar,
            first_trading_day,
            fields: list[str],
            fundamental_data_reader,
            historical_data_reader=None,
            future_daily_reader=None,
            future_minute_reader=None,
            adjustment_reader=None,
            last_available_session=None,
            last_available_minute=None,
    ):
        self._data_reader = historical_data_reader
        self.trading_calendar = trading_calendar

        self.asset_repository = asset_repository

        self._adjustment_reader = adjustment_reader
        self._fields = fields
        # caches of sid -> adjustment list
        self._splits_dict = {}
        self._mergers_dict = {}
        self._dividends_dict = {}

        # Handle extra sources, like Fetcher.
        # self._augmented_sources_map = {}
        # self._extra_source_df = None

        self._first_available_session = first_trading_day

        if last_available_session:
            self._last_available_session = last_available_session
        else:
            # Infer the last session from the provided readers.
            last_sessions = [
                reader.last_available_dt
                for reader in [historical_data_reader, future_daily_reader]
                if reader is not None
            ]
            if last_sessions:
                self._last_available_session = min(last_sessions)
            else:
                self._last_available_session = None

        if last_available_minute:
            self._last_available_minute = last_available_minute
        else:
            # Infer the last minute from the provided readers.
            last_minutes = [
                reader.last_available_dt
                for reader in [historical_data_reader, future_minute_reader]
                if reader is not None
            ]
            if last_minutes:
                self._last_available_minute = max(last_minutes)
            else:
                self._last_available_minute = None

        aligned_equity_minute_reader = self._ensure_reader_aligned(historical_data_reader)
        aligned_equity_session_reader = self._ensure_reader_aligned(historical_data_reader)
        aligned_future_minute_reader = self._ensure_reader_aligned(future_minute_reader)
        aligned_future_session_reader = self._ensure_reader_aligned(future_daily_reader)

        self._roll_finders = {
            "calendar": CalendarRollFinder(self.trading_calendar, self.asset_repository),
        }

        aligned_minute_readers = {}
        aligned_session_readers = {}

        if aligned_equity_minute_reader is not None:
            aligned_minute_readers[Equity] = aligned_equity_minute_reader
        if aligned_equity_session_reader is not None:
            aligned_session_readers[Equity] = aligned_equity_session_reader

        if aligned_future_minute_reader is not None:
            aligned_minute_readers[Future] = aligned_future_minute_reader
            aligned_minute_readers[ContinuousFuture] = ContinuousFutureMinuteBarReader(
                aligned_future_minute_reader,
                self._roll_finders,
            )

        if aligned_future_session_reader is not None:
            aligned_session_readers[Future] = aligned_future_session_reader
            self._roll_finders["volume"] = VolumeRollFinder(
                self.trading_calendar,
                self.asset_repository,
                aligned_future_session_reader,
            )
            aligned_session_readers[
                ContinuousFuture
            ] = ContinuousFutureSessionBarReader(
                aligned_future_session_reader,
                self._roll_finders,
            )

        _dispatch_minute_reader = AssetDispatchMinuteBarReader(
            self.trading_calendar,
            self.asset_repository,
            aligned_minute_readers,
            self._last_available_minute,
        )

        _dispatch_session_reader = AssetDispatchSessionBarReader(
            self.trading_calendar,
            self.asset_repository,
            aligned_session_readers,
            self._last_available_session,
        )

        self._pricing_readers = {
            "minute": _dispatch_minute_reader,
            "daily": _dispatch_session_reader,
        }

        self._daily_aggregator = DailyHistoryAggregator(
            self.trading_calendar.first_minutes,
            _dispatch_minute_reader,
            self.trading_calendar,
        )
        self._history_loader = PolarsHistoryLoader(
            self.trading_calendar,
            _dispatch_session_reader,
            self._adjustment_reader,
            self.asset_repository,
            self._fields,
            self._roll_finders,
        )

        self._first_trading_day = first_trading_day

        # Get the first trading minute
        self._first_trading_minute = (
            self.trading_calendar.session_first_minute(self._first_trading_day)
            if self._first_trading_day is not None
            else (None, None)
        )

        # Store the locs of the first day and first minute
        self._first_trading_day_loc = (
            self.trading_calendar.sessions.get_loc(self._first_trading_day)
            if self._first_trading_day is not None
            else None
        )

    def _ensure_reader_aligned(self, reader):
        if reader is None:
            return

        if reader.trading_calendar.name == self.trading_calendar.name:
            return reader
        elif reader.data_frequency == "minute":
            return ReindexMinuteBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )
        elif reader.data_frequency == "session":
            return ReindexSessionBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )

    def _reindex_extra_source(self, df, source_date_index):
        return df.reindex(index=source_date_index, method="ffill")

    def _get_pricing_reader(self, data_frequency: DataFrequency):
        return self._pricing_readers[data_frequency]

    def get_last_traded_dt(self, asset, dt, data_frequency: DataFrequency):
        """Given an asset and dt, returns the last traded dt from the viewpoint
        of the given dt.

        If there is a trade on the dt, the answer is dt provided.
        """
        return self._get_pricing_reader(data_frequency=data_frequency).get_last_traded_dt(asset=asset, dt=dt)

    def _get_single_asset_value(self, session_label: pd.Timestamp, asset: Asset, field: str, dt: pd.Timestamp,
                                data_frequency: DataFrequency):

        if field not in self._fields:
            raise KeyError("Invalid column: " + str(field))

        if (
                dt < asset.start_date.tz_localize(dt.tzinfo)
                or (
                data_frequency == "daily" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
            asset.end_date, tzinfo=datetime.timezone.utc))
                or (
                data_frequency == "minute" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
            asset.end_date, tzinfo=datetime.timezone.utc))
        ):
            if field == "volume":
                return 0
            elif field == "contract":
                return None
            elif field != "last_traded":
                return np.nan

        if data_frequency == "daily":
            if field == "contract":
                return self._get_current_contract(continuous_future=asset, dt=session_label)
            else:
                return self._get_daily_spot_value(
                    asset=asset,
                    column=field,
                    dt=session_label,
                )
        else:
            if field == "last_traded":
                return self.get_last_traded_dt(asset, dt, "minute")
            elif field == "price":
                return self._get_minute_spot_value(
                    asset=asset,
                    column="close",
                    dt=dt,
                    ffill=True,
                )
            elif field == "contract":
                return self._get_current_contract(continuous_future=asset, dt=dt)
            else:
                return self._get_minute_spot_value(asset=asset, column=field, dt=dt)

    def get_spot_value(self, assets: list[Asset], field: str, dt: pd.Timestamp, data_frequency: DataFrequency):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset, ContinuousFuture, or iterable of same.
            The asset or assets whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        # assets_is_scalar = False
        # if isinstance(assets, (AssetConvertible, PricingDataAssociable)):
        #     assets_is_scalar = True
        # else:
        #     # If 'assets' was not one of the expected types then it should be
        #     # an iterable.
        #     try:
        #         iter(assets)
        #     except TypeError as exc:
        #         raise TypeError(
        #             "Unexpected 'assets' value of type {}.".format(type(assets))
        #         ) from exc

        session_label = self.trading_calendar.minute_to_session(dt)

        # if assets_is_scalar:
        #     return self._get_single_asset_value(
        #         session_label=session_label,
        #         asset=assets,
        #         field=field,
        #         dt=dt,
        #         data_frequency=data_frequency,
        #     )
        # else:
        # get_single_asset_value =
        return [
            self._get_single_asset_value(
                session_label=session_label,
                asset=asset,
                field=field,
                dt=dt,
                data_frequency=data_frequency,
            )
            for asset in assets
        ]

    def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: pd.Timestamp, data_frequency: DataFrequency):
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
        dt : pd.Timestamp
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        return self._get_single_asset_value(
            session_label=self.trading_calendar.minute_to_session(dt),
            asset=asset,
            field=field,
            dt=dt,
            data_frequency=data_frequency,
        )

    def get_adjustments(self, assets: list[Asset], field: str, dt: pd.Timestamp, perspective_dt: pd.Timestamp):
        """Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of assets

        Parameters
        ----------
        assets : list of type Asset, or Asset
            The asset, or assets whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
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
            self, asset: Asset, field: str, dt: pd.Timestamp, perspective_dt: pd.Timestamp, data_frequency: DataFrequency,
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
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The value of the given ``field`` for ``asset`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open',
            'high', 'low', 'close', or 'price', the value will be a float. If
            the ``field`` is 'volume' the value will be a int. If the ``field``
            is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            # if this a fetcher field, we want to use perspective_dt (not dt)
            # because we want the new value as of midnight (fetcher only works
            # on a daily basis, all timestamps are on midnight)
            # if self._is_extra_source(asset, field, self._augmented_sources_map, fields=self._fields):
            #     spot_value = self.get_spot_value(
            #         assets=[asset], field=field, dt=perspective_dt, data_frequency=data_frequency
            #     )
            # else:
            spot_value = self.get_spot_value(assets=[asset], field=field, dt=dt, data_frequency=data_frequency)

        if isinstance(asset, Equity):
            ratio = self.get_adjustments(assets=[asset], field=field, dt=dt, perspective_dt=perspective_dt)[0]
            spot_value *= ratio

        return spot_value

    def _get_minute_spot_value(self, asset: Asset, column: str, dt: pd.Timestamp, ffill: bool = False):
        reader = self._get_pricing_reader(data_frequency="minute")

        if not ffill:
            try:
                return reader.get_value(sid=asset.sid, dt=dt, field=column)
            except NoDataOnDate:
                if column != "volume":
                    return np.nan
                else:
                    return 0

        # At this point the pairing of column='close' and ffill=True is
        # assumed.
        try:
            # Optimize the best case scenario of a liquid asset
            # returning a valid price.
            result = reader.get_value(sid=asset.sid, dt=dt, field=column)
            if not pd.isnull(result):
                return result
        except NoDataOnDate:
            # Handling of no data for the desired date is done by the
            # forward filling logic.
            # The last trade may occur on a previous day.
            pass
        # If forward filling, we want the last minute with values (up to
        # and including dt).
        query_dt = reader.get_last_traded_dt(asset=asset, dt=dt)

        if pd.isnull(query_dt):
            # no last traded dt, bail
            return np.nan

        result = reader.get_value(sid=asset.sid, dt=query_dt, field=column)

        if (dt == query_dt) or (dt.date() == query_dt.date()):
            return result

        # the value we found came from a different day, so we have to
        # adjust the data if there are any adjustments on that day barrier
        return self.get_adjusted_value(
            asset=asset, field=column, dt=query_dt, perspective_dt=dt, data_frequency="minute", spot_value=result
        )

    def _get_daily_spot_value(self, asset: Asset, column: str, dt: pd.Timestamp):
        reader = self._get_pricing_reader(data_frequency="daily")
        if column == "last_traded":
            last_traded_dt = reader.get_last_traded_dt(asset=asset, dt=dt)

            if isnull(last_traded_dt):
                return pd.NaT
            else:
                return last_traded_dt
        elif column == "price":
            found_dt = dt
            while True:
                try:
                    value = reader.get_value(sid=asset.sid, dt=found_dt, field="close")
                    if not isnull(value):
                        if dt == found_dt:
                            return value
                        else:
                            # adjust if needed
                            return self.get_adjusted_value(
                                asset=asset, field=column, dt=found_dt, perspective_dt=dt, data_frequency="minute",
                                spot_value=value
                            )
                    else:
                        found_dt -= self.trading_calendar.day
                except NoDataOnDate:
                    return np.nan
        elif column in self._fields:
            # don't forward fill
            try:
                return reader.get_value(sid=asset.sid, dt=dt, field=column)
            except NoDataOnDate:
                return np.nan

    @remember_last
    def _get_days_for_window(self, end_date: pd.Timestamp, bar_count: int):
        tds = self.trading_calendar.sessions
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

    def _get_history_daily_window(
            self,
            assets: list[Asset],
            end_dt: pd.Timestamp,
            bar_count: int,
            fields: list[str],
            data_frequency: DataFrequency
    ):
        """Internal method that returns a dataframe containing history bars
        of daily frequency for the given sids.
        """
        session = self.trading_calendar.minute_to_session(end_dt)  # .tz_localize(self.trading_calendar.tz)
        days_for_window = self._get_days_for_window(end_date=session, bar_count=bar_count)

        if len(assets) == 0:
            return pd.DataFrame(None, index=days_for_window, columns=None)

        if data_frequency == "daily":
            # two cases where we use daily data for the whole range:
            # 1) the history window ends at midnight utc.
            # 2) the last desired day of the window is after the
            # last trading day, use daily data for the whole range.
            return self._data_reader.load_raw_arrays(
                fields=fields,
                start_date=days_for_window[0],
                end_date=days_for_window[-1],
                assets=assets,
            )

            #     self._get_daily_window_data(
            #     assets=assets, fields=field_to_use, days_in_window=days_for_window, extra_slot=False
            # )

        else:
            # minute mode, requesting '1d'
            data = self._get_daily_window_data(
                assets=assets, fields=field_to_use, days_in_window=days_for_window[0:-1], extra_slot=True
            )

            if field_to_use == "open":
                minute_value = self._daily_aggregator.opens(assets, end_dt)
            elif field_to_use == "high":
                minute_value = self._daily_aggregator.highs(assets, end_dt)
            elif field_to_use == "low":
                minute_value = self._daily_aggregator.lows(assets, end_dt)
            elif field_to_use == "close":
                minute_value = self._daily_aggregator.closes(assets, end_dt)
            elif field_to_use == "volume":
                minute_value = self._daily_aggregator.volumes(assets, end_dt)
            elif field_to_use == "sid":
                minute_value = [
                    int(self._get_current_contract(asset, end_dt)) for asset in assets
                ]

            # append the partial day.
            data[-1] = minute_value

        return pd.DataFrame(data, index=days_for_window, columns=assets)

    def _handle_minute_history_out_of_bounds(self, bar_count):
        cal = self.trading_calendar

        first_trading_minute_loc = (
            cal.minutes.get_loc(self._first_trading_minute)
            if self._first_trading_minute is not None
            else None
        )

        suggested_start_day = cal.minute_to_session(
            cal.minutes[first_trading_minute_loc + bar_count] + cal.day
        )

        raise HistoryWindowStartsBeforeData(
            first_trading_day=self._first_trading_day.date(),
            bar_count=bar_count,
            suggested_start_day=suggested_start_day.date(),
        )

    def _get_history_minute_window(self, assets, end_dt, bar_count, field_to_use):
        """Internal method that returns a dataframe containing history bars
        of minute frequency for the given sids.
        """
        # get all the minutes for this window
        try:
            minutes_for_window = self.trading_calendar.minutes_window(
                end_dt, -bar_count
            ).tz_convert(self.trading_calendar.tz)
        except KeyError:
            self._handle_minute_history_out_of_bounds(bar_count)

        if minutes_for_window[0] < self._first_trading_minute:
            self._handle_minute_history_out_of_bounds(bar_count)

        asset_minute_data = self._minute_history_loader.history(
            assets, minutes_for_window, field_to_use, False
        )

        return pd.DataFrame(asset_minute_data, index=minutes_for_window, columns=assets)

    def get_history_window(
            self, assets: list[Asset], end_dt: pd.Timestamp, bar_count: int, frequency: DataFrequency,
            fields: list[str],
            data_frequency: DataFrequency, ffill: bool = True
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
        # if field not in OHLCVP_FIELDS and field != "sid":
        #     raise ValueError(f"Invalid field: {field}")

        if bar_count < 1:
            raise ValueError(f"bar_count must be >= 1, but got {bar_count}")
        fields_for_fetch = []
        for idx, field in enumerate(fields):
            if field == "price":
                fields_for_fetch.append("close")
            else:
                fields_for_fetch.append(field)

        if frequency == DataFrequency.DAY:
            df = self._get_history_daily_window(
                assets=assets, end_dt=end_dt, bar_count=bar_count, fields=fields_for_fetch,
                data_frequency=data_frequency
            )
        elif frequency == DataFrequency.MINUTE:
            df = self._get_history_minute_window(assets=assets, end_dt=end_dt, bar_count=bar_count, fields=fields_for_fetch)
        else:
            raise ValueError(f"Invalid frequency: {frequency}")

        # forward-fill price
        # if field == "price":
        #     if frequency == "1m":
        #         ffill_data_frequency = "minute"
        #     elif frequency == "1d":
        #         ffill_data_frequency = "daily"
        #     else:
        #         raise Exception("Only 1d and 1m are supported for forward-filling.")

        # assets_with_leading_nan = np.where(isnull(df.iloc[0]))[0]

        # history_start, history_end = df.index[[0, -1]]
        # if ffill_data_frequency == "daily" and data_frequency == "minute":
        #     # When we're looking for a daily value, but we haven't seen any
        #     # volume in today's minute bars yet, we need to use the
        #     # previous day's ffilled daily price. Using today's daily price
        #     # could yield a value from later today.
        #     history_start -= self.trading_calendar.day

        # initial_values = []
        # for asset in df.columns[assets_with_leading_nan]:
        #     last_traded = self.get_last_traded_dt(
        #         asset=asset,
        #         dt=history_start,
        #         data_frequency=ffill_data_frequency,
        #     )
        #     if isnull(last_traded):
        #         initial_values.append(nan)
        #     else:
        #         initial_values.append(
        #             self.get_adjusted_value(
        #                 asset=asset,
        #                 field=field,
        #                 dt=last_traded,
        #                 perspective_dt=history_end,
        #                 data_frequency=ffill_data_frequency,
        #             )
        #         )

        # Set leading values for assets that were missing data, then ffill.
        # df.iloc[0, assets_with_leading_nan] = np.array(
        #     initial_values, dtype=np.float64
        # )
        # df.ffill(inplace=True)

        # forward-filling will incorrectly produce values after the end of
        # an asset's lifetime, so write NaNs back over the asset's
        # end_date.
        # normed_index = df.index.normalize()
        # for asset in df.columns:
        #     if history_end >= asset.end_date.tz_localize(history_end.tzinfo):
        #         # if the window extends past the asset's end date, set
        #         # all post-end-date values to NaN in that asset's series
        #         df.loc[
        #             normed_index > asset.end_date.tz_localize(normed_index.tz),
        #             asset,
        #         ] = nan

        return df

    def _get_daily_window_data(self, assets: list[Asset], fields: list[str], days_in_window, extra_slot: bool = True):
        """Internal method that gets a window of adjusted daily data for a sid
        and specified date range.  Used to support the history API method for
        daily bars.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.

        start_dt: pandas.Timestamp
            The start of the desired window of data.

        bar_count: int
            The number of days of data to return.

        field: string
            The specific field to return.  "open", "high", "close_price", etc.

        extra_slot: boolean
            Whether to allocate an extra slot in the returned numpy array.
            This extra slot will hold the data for the last partial day.  It's
            much better to create it here than to create a copy of the array
            later just to add a slot.

        Returns
        -------
        A numpy array with requested values.  Any missing slots filled with
        nan.

        """
        # bar_count = len(days_in_window)
        # create an np.array of size bar_count
        # dtype = float64 if field != "sid" else int64
        # if extra_slot:
        #     return_array = np.zeros((bar_count + 1, len(assets)), dtype=dtype)
        # else:
        #     return_array = np.zeros((bar_count, len(assets)), dtype=dtype)
        #
        # if field != "volume":
        #     # volumes default to 0, so we don't need to put NaNs in the array
        #     return_array = return_array.astype(float64)
        #     return_array[:] = np.nan
        # if bar_count != 0:
        data = self._data_reader.load_raw_arrays(
            columns=fields,
            start_date=days_in_window[0],
            end_date=days_in_window[-1],
            assets=assets,
        )
        # data = self._history_loader.history(
        #     assets=assets, dts=days_in_window, field=field, is_perspective_after=extra_slot
        # )
        # if extra_slot:
        #     return_array[: len(return_array) - 1, :] = data
        # else:
        #     return_array[: len(data)] = data
        return data

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
            A list of [multiplier, pd.Timestamp], earliest first

        """
        if self._adjustment_reader is None:
            return []

        sid = int(asset)

        try:
            adjustments = adjustments_dict[sid]
        except KeyError:
            adjustments = adjustments_dict[
                sid
            ] = self._adjustment_reader.get_adjustments_for_sid(table_name, sid)

        return adjustments

    def get_splits(self, assets: list[Asset], dt: pd.Timestamp):
        """Returns any splits for the given sids and the given dt.

        Parameters
        ----------
        assets : container
            Assets for which we want splits.
        dt : pd.Timestamp
            The date for which we are checking for splits. Note: this is
            expected to be midnight UTC.

        Returns
        -------
        splits : list[(asset, float)]
            List of splits, where each split is a (asset, ratio) tuple.
        """
        if self._adjustment_reader is None or not assets:
            return []

        # convert dt to # of seconds since epoch, because that's what we use
        # in the adjustments db
        seconds = int(dt.value / 1e9)

        splits = self._adjustment_reader.conn.execute(
            "SELECT sid, ratio FROM SPLITS WHERE effective_date = ?", (seconds,)
        ).fetchall()

        splits = [split for split in splits if split[0] in assets]
        splits = [
            (self.asset_repository.retrieve_asset(split[0]), split[1]) for split in splits
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
        All timestamp fields are converted to pd.Timestamps.
        """

        if self._adjustment_reader is None:
            return []

        if len(trading_days) == 0:
            return []

        start_dt = trading_days[0].value / 1e9
        end_dt = trading_days[-1].value / 1e9

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
                    "declared_date": pd.Timestamp(dividend_tuple[0], unit="s"),
                    "ex_date": pd.Timestamp(dividend_tuple[1], unit="s"),
                    "pay_date": pd.Timestamp(dividend_tuple[2], unit="s"),
                    "payment_sid": dividend_tuple[3],
                    "ratio": dividend_tuple[4],
                    "record_date": pd.Timestamp(dividend_tuple[5], unit="s"),
                    "sid": dividend_tuple[6],
                }
            )

        return dividend_info

    def get_fetcher_assets(self, dt: pd.Timestamp):
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

    def get_current_future_chain(self, continuous_future: ContinuousFuture, dt: pd.Timestamp):
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
        session = self.trading_calendar.minute_to_session(dt)
        contract_center = rf.get_contract_center(
            continuous_future.root_symbol, session, continuous_future.offset
        )
        oc = self.asset_repository.get_ordered_contracts(continuous_future.root_symbol)
        chain = oc.active_chain(contract_center, session.value)
        return self.asset_repository.retrieve_all(sids=chain)

    def _get_current_contract(self, continuous_future: ContinuousFuture, dt: pd.Timestamp):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_sid = rf.get_contract_center(
            continuous_future.root_symbol, dt, continuous_future.offset
        )
        if contract_sid is None:
            return None
        return self.asset_repository.retrieve_asset(sid=contract_sid)

    @property
    def adjustment_reader(self):
        return self._adjustment_reader
