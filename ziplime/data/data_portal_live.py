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
import logging
import pandas as pd
from lime_trader.models.market import Period
from zipline.data.bar_reader import NoDataOnDate

from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
from ziplime.data.data_portal import DataPortal

class DataPortalLive(DataPortal):
    def __init__(self, broker, market_data_provider: AbstractLiveMarketDataProvider, *args, **kwargs):
        self.broker = broker
        self.market_data_provider = market_data_provider
        self._logger = logging.getLogger(__name__)
        super(DataPortalLive, self).__init__(*args, **kwargs)

    def get_last_traded_dt(self, asset, dt, data_frequency):
        return self.broker.get_last_traded_dt(asset)

    def get_spot_value(self, assets, field, dt, data_frequency):
        return self.broker.get_spot_value(assets, field, dt, data_frequency)

    def get_history_window(self,
                           assets,
                           end_dt: pd.Timestamp,
                           bar_count: int,
                           frequency: str,
                           field: str,
                           data_frequency: str,
                           ffill: bool = True):
        # This method is responsible for merging the ingested historical data
        # with the real-time collected data through the Broker.
        # DataPortal.get_history_window() is called with ffill=False to mark
        # the missing fields with NaNs. After merge on the historical and
        # real-time data the missing values (NaNs) are filled based on their
        # next available values in the requested time window.
        #
        # Warning: setting ffill=True in DataPortal.get_history_window() call
        # results a wrong behavior: The last available value reported by
        # get_spot_value() will be used to fill the missing data - which is
        # always representing the current spot price presented by Broker.
        session = self.trading_calendar.minute_to_session(end_dt)
        days_for_window = self._get_days_for_window(session, bar_count)
        if frequency == "1m":
            date_from = end_dt - datetime.timedelta(minutes=bar_count)
            period = Period.MINUTE
        elif frequency == "1d":
            date_from = end_dt - datetime.timedelta(days=bar_count)
            period = Period.DAY
        else:
            raise Exception(f"Invalid data_frequency: {data_frequency}")

        try:
            historical_bars = super(DataPortalLive, self).get_history_window(
                assets=assets, end_dt=end_dt, bar_count=bar_count, frequency=frequency, field=field,
                data_frequency=data_frequency, ffill=False
            )
            latest_date_historical = max(historical_bars.index)
            latest_trading_date = max(days_for_window)
            if latest_date_historical == latest_trading_date:
                return historical_bars

        except (NoDataOnDate, LookupError) as e:
            historical_bars = None

        realtime_bars = self.market_data_provider.fetch_live_data_table(
            symbols=[a.symbol for a in assets], period=period, date_from=date_from, date_to=end_dt,
            show_progress=False
        )

        # Broker.get_realtime_history() returns the asset as level 0 column,
        # open, high, low, close, volume returned as level 1 columns.
        # To filter for field the levels needs to be swapped
        results = historical_bars or None
        for asset, df in zip(assets, realtime_bars):
            ohlcv_field = 'close' if field == 'price' else field

            # TODO: end_dt is ignored when historical & realtime bars are merged.
            # Should not cause issues as end_dt is set to current time in live
            # trading, but would be more proper if merge would make use of it.
            new_df = pd.DataFrame(df[ohlcv_field]).rename(columns={ohlcv_field: asset})
            if results is not None:
                results = results.combine_first(new_df)
            else:
                results = new_df
        results = results[-bar_count:]

        # results[0].combine_first(new_df)

        if ffill and field == 'price':
            # Simple forward fill is not enough here as the last ingested
            # value might be outside of the requested time window. That case
            # the time series starts with NaN and forward filling won't help.
            # To provide values for such cases we backward fill.
            # Backward fill as a second operation will have no effect if the
            # forward-fill was successful.
            results.ffill(inplace=True)
            results.bfill(inplace=True)
        return results
