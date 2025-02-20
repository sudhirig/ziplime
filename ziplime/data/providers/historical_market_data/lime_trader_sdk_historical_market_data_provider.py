import datetime
import logging
import multiprocessing
import sys
from dataclasses import asdict

import pandas as pd
from click import progressbar
from exchange_calendars import ExchangeCalendar
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.data.abstract_historical_market_data_provider import AbstractHistoricalMarketDataProvider
from ziplime.domain.lime_quote import LimeQuote


class LimeTraderSdkHistoricalMarketDataProvider(AbstractHistoricalMarketDataProvider):
    def __init__(self, lime_sdk_credentials_file: str):
        self._lime_sdk_credentials_file = lime_sdk_credentials_file
        self._logger = logging.getLogger(__name__)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    def get_historical_data_table(self, symbols: list[str],
                                  period: Period,
                                  date_from: datetime.datetime,
                                  date_to: datetime.datetime,
                                  show_progress: bool,
                                  exchange_calendar: ExchangeCalendar,
                                  ):

        def fetch_historical(lime_trader_sdk_credentials_file: str, symbol: str):
            lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
            try:
                quotes = lime_client.market.get_quotes_history(
                    symbol=symbol, period=period, from_date=date_from,
                    to_date=date_to
                )
                df = LimeTraderSdkHistoricalMarketDataProvider.load_data_table(
                    quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
                    show_progress=show_progress
                )
            except Exception as e:
                logging.exception(
                    f"Exception fetching historical data for symbol {symbol}, date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

            return df

        total_days = (date_to - date_from).days
        final = pd.DataFrame()

        if show_progress:
            with progressbar(length=len(symbols) * total_days, label="Downloading historical data from LimeTraderSDK",
                             file=sys.stdout) as pbar:
                res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads",
                               return_as="generator_unordered")(
                    delayed(fetch_historical)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)
                for item in res:
                    pbar.update(total_days)
                    if item is None:
                        continue
                    final = pd.concat([final, item])
        else:
            res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads", return_as="generator_unordered")(
                delayed(fetch_historical)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)
            for item in res:
                if item is None:
                    continue
                final = pd.concat([final, item])
        final = final.sort_index()
        return final

    @staticmethod
    def load_data_table(quotes: list[LimeQuote], show_progress: bool = False):
        if not quotes:
            return pd.DataFrame()
        data_table = pd.DataFrame(
            [dict(**asdict(quote_hist.quote_history), symbol=quote_hist.symbol) for quote_hist in quotes], )

        data_table.rename(
            columns={
                "timestamp": "date",
            },
            inplace=True,
            copy=False,
        )
        # data_table = data_table.reset_index()
        data_table = data_table.set_index('date', drop=False)
        return data_table
