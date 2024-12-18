import datetime
import logging
import multiprocessing
from dataclasses import asdict

import pandas as pd
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
from ziplime.domain.lime_quote import LimeQuote


class LimeTraderSdkLiveMarketDataProvider(AbstractLiveMarketDataProvider):
    def __init__(self, lime_sdk_credentials_file: str):
        self._lime_sdk_credentials_file = lime_sdk_credentials_file
        self._logger = logging.getLogger(__name__)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    def fetch_live_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool):


        def fetch_live(lime_trader_sdk_credentials_file: str, symbol: str):
            lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
            try:
                quotes = lime_client.market.get_quotes_history(
                    symbol=symbol, period=period, from_date=date_from,
                    to_date=date_to
                )
                df = LimeTraderSdkLiveMarketDataProvider.load_data_table(
                    quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
                    show_progress=show_progress
                )
            except Exception as e:
                self._logger.error("Error fetching data using lime trader sdk")
                df = pd.DataFrame()
            return df
        res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads", return_as="generator", )(
            delayed(fetch_live)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)

        result = []
        for item in res:
            if item is None:
                continue
            result.append(item)
        if show_progress:
            self._logger.info("Downloading live Lime Trader SDK metadata.")
        return result

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
