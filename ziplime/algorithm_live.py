import logging
from datetime import time
import os.path
import pandas as pd
from ziplime.algorithm import TradingAlgorithm
from ziplime.gens.realtimeclock import RealtimeClock
from ziplime.gens.tradesimulation import AlgorithmSimulator
from ziplime.errors import ScheduleFunctionOutsideTradingStart
from ziplime.utils.api_support import (
    ZiplineAPI,
    api_method,
    allowed_only_in_before_trading_start)

from ziplime.utils.serialization_utils import load_context, store_context

from ziplime.utils.calendar_utils import days_at_time


class LiveAlgorithmExecutor(AlgorithmSimulator):
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)


class LiveTradingAlgorithm(TradingAlgorithm):
    def __init__(self, *args, **kwargs):
        self.broker = kwargs.pop('broker', None)
        self.orders = {}

        self.algo_filename = kwargs.get('algo_filename', "<algorithm>")
        self.state_filename = kwargs.pop('state_filename', None)
        self.realtime_bar_target = kwargs.pop('realtime_bar_target', None)
        self._context_persistence_excludes = []
        self._logger = logging.getLogger(__name__)

        super(self.__class__, self).__init__(*args, **kwargs)

        self._logger.info("initialization done")

    def initialize(self, *args, **kwargs):
        self._context_persistence_excludes = (list(self.__dict__.keys()) +
                                              ['trading_client'])

        # if os.path.isfile(self.state_filename):
        #     self._logger.info("Loading state from {}".format(self.state_filename))
        #     load_context(self.state_filename, context=self, checksum=self.algo_filename)
        #     return

        with ZiplineAPI(self):
            super(self.__class__, self).initialize(*args, **kwargs)
            store_context(self.state_filename,
                          context=self,
                          checksum=self.algo_filename,
                          include_list=[])

    def handle_data(self, data):
        super(self.__class__, self).handle_data(data)
        store_context(self.state_filename,
                      context=self,
                      checksum=self.algo_filename,
                      include_list=[])

    def _create_clock(self):
        minutely_emission = False
        if self.sim_params.data_frequency == "minute":
            minutely_emission = self.sim_params.emission_rate == "minute"

        trading_o_and_c = self.trading_calendar.schedule.loc[self.sim_params.sessions]

        market_opens = trading_o_and_c['open']
        market_closes = trading_o_and_c['close']

        # The calendar's execution times are the minutes over which we actually
        # want to run the clock. Typically the execution times simply adhere to
        # the market open and close times. In the case of the futures calendar,
        # for example, we only want to simulate over a subset of the full 24
        # hour calendar, so the execution times dictate a market open time of
        # 6:31am US/Eastern and a close of 5:00pm US/Eastern.

        before_trading_start_minutes = market_opens.map(lambda x: x - pd.Timedelta(minutes=45))
        return RealtimeClock(
            sessions=self.sim_params.sessions,
            execution_opens=market_opens,
            execution_closes=market_closes,
            before_trading_start_minutes=before_trading_start_minutes,
            minute_emission=minutely_emission,
            time_skew=self.broker.get_time_skew(),
            is_broker_alive=self.broker.is_alive,
            frequency=self.data_frequency
        )

    def _create_generator(self, sim_params):
        # Call the simulation trading algorithm for side-effects:
        # it creates the perf tracker
        TradingAlgorithm._create_generator(self, sim_params)
        self.trading_client = LiveAlgorithmExecutor(
            self,
            sim_params,
            self.data_portal,
            self._create_clock(),
            self._create_benchmark_source(),
            self.restrictions,
            # universe_func=self._calculate_universe
        )

        return self.trading_client.transform()

    def updated_portfolio(self):
        return self.broker.get_portfolio()

    def updated_account(self):
        return self.broker.get_account()

    @api_method
    @allowed_only_in_before_trading_start(
        ScheduleFunctionOutsideTradingStart())
    def schedule_function(self,
                          func,
                          date_rule=None,
                          time_rule=None,
                          half_days=True,
                          calendar=None):
        # If the scheduled_function() is called from initalize()
        # then the state persistence would need to take care of storing and
        # restoring the scheduled functions too (as initialize() only called
        # once in the algorithm's life). Persisting scheduled functions are
        # difficult as they are not serializable by default.
        # We enforce scheduled functions to be called only from
        # before_trading_start() in live trading with a decorator.
        super(self.__class__, self).schedule_function(func,
                                                      date_rule,
                                                      time_rule,
                                                      half_days,
                                                      calendar)

    @api_method
    def symbol(self, symbol_str):
        # This method works around the problem of not being able to trade
        # assets which does not have ingested data for the day of trade.
        # Normally historical data is loaded to bundle and the asset's
        # end_date and auto_close_date is set based on the last entry from
        # the bundle db. LiveTradingAlgorithm does not override order_value(),
        # order_percent() & order_target(). Those higher level ordering
        # functions provide a safety net to not to trade de-listed assets.
        # If the asset is returned as it was ingested (end_date=yesterday)
        # then CannotOrderDelistedAsset exception will be raised from the
        # higher level order functions.
        #
        # Hence, we are increasing the asset's end_date by 10,000 days.
        # The ample buffer is provided for two reasons:
        # 1) assets are often stored in algo's context through initialize(),
        #    which is called once and persisted at live trading. 10,000 days
        #    enables 27+ years of trading, which is more than enough.
        # 2) Tool - 10,000 Days is brilliant!
        asset = super(self.__class__, self).symbol(symbol_str)
        tradeable_asset = asset.to_dict()
        tradeable_asset['end_date'] = (pd.Timestamp('now', tz='UTC') +
                                       pd.Timedelta('10000 days'))
        tradeable_asset['auto_close_date'] = tradeable_asset['end_date']
        return asset.from_dict(tradeable_asset)

    def run(self, *args, **kwargs):
        daily_stats = super(self.__class__, self).run(*args, **kwargs)
        self.on_exit()
        return daily_stats

    def on_exit(self):
        if not self.realtime_bar_target:
            return

        self._logger.info("Storing realtime bars to: {}".format(
            self.realtime_bar_target))

        today = str(pd.to_datetime('today').date())
        subscribed_assets = self.broker.get_subscribed_assets()
        realtime_history = self.broker.get_realtime_bars(subscribed_assets, '1m')

        if not os.path.exists(self.realtime_bar_target):
            os.mkdir(self.realtime_bar_target)

        for asset in subscribed_assets:
            filename = "ZL-%s-%s.csv" % (asset.symbol, today)
            path = os.path.join(self.realtime_bar_target, filename)
            realtime_history[asset].to_csv(path, mode='a',
                                           index_label='datetime',
                                           header=not os.path.exists(path))

    @property
    def portfolio(self):
        portfolio = self.broker.get_portfolio()
        return portfolio
        # self._sync_last_sale_prices()
        # return self.metrics_tracker.portfolio

    @property
    def account(self):
        account = self.broker.get_account()
        return account
        # self._sync_last_sale_prices()
        # return self.metrics_tracker.account
