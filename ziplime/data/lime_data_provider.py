import datetime
import logging
import sys
import time
from dataclasses import asdict
from queue import Queue

import limexhub
import pandas as pd
from click import progressbar
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.domain.lime_quote import LimeQuote


class LimeDataProvider:
    def __init__(self, limex_api_key: str, lime_sdk_credentials_file: str):
        self._limex_api_key = limex_api_key
        self._lime_sdk_credentials_file = lime_sdk_credentials_file


        self._logger = logging.getLogger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file,logger=self._logger)


    def fetch_historical_data_table(self, symbols: list[str],
                                    period: Period,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    show_progress: bool,
                                    retries: int):

        def fetch_historical(limex_api_key: str, symbol: str):
            limex_client = limexhub.RestAPI(token=limex_api_key)
            df = limex_client.candles(symbol=symbol,
                                      from_date=date_from,
                                      to_date=date_to,
                                      timeframe=3)

            # "date",
            # "ex-dividend",
            # "split_ratio",

            if len(df) > 0:
                df = df.reset_index()
                df = df.rename(columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "Date": "date"})
                df = df.set_index('date', drop=False)
                df.index = pd.to_datetime(df.index, utc=True)
                df["symbol"] = symbol
                df['dividend'] = 0
                df['split'] = 0
            return df

        total_days = (date_to - date_from).days
        final = pd.DataFrame()

        if show_progress:
            with progressbar(length=len(symbols) * total_days, label="Downloading historical data from LimexHub",
                             file=sys.stdout) as pbar:
                res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
                    delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
                for item in res:
                    pbar.update(total_days)
                    final = pd.concat([final, item])
        else:
            res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
                delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
            for item in res:
                final = pd.concat([final, item])
        final = final.sort_index()
        return final

    def fetch_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool,
            retries: int):
        historical_data = self.fetch_historical_data_table(symbols=symbols, period=period, date_from=date_from,
                                                           date_to=date_to,
                                                           show_progress=show_progress, retries=retries)

        yield historical_data

        live_data_start_date = max(historical_data.index) if len(historical_data) > 0 else date_from

        for quotes in self.fetch_live_data_table(symbols=symbols, period=period, date_from=live_data_start_date,
                                                 date_to=date_to,
                                                 show_progress=show_progress, retries=retries):
            yield quotes

    def fetch_live_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool,
            retries: int):

        live_data_queue = Queue()

        def fetch_live(lime_trader_sdk_credentials_file: str, symbol: str):
            latest_date = date_from
            lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
            current_date = datetime.datetime.now(tz=datetime.timezone.utc)
            quotes = lime_client.market.get_quotes_history(
                symbol=symbol, period=period, from_date=latest_date,
                to_date=current_date
            )
            while True:
                df = LimeDataProvider.load_data_table(
                    quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
                    show_progress=show_progress
                )
                live_data_queue.put(df)
                if quotes:
                    latest_date = quotes[-1].timestamp
                time.sleep(2)
                current_date = datetime.datetime.now(tz=datetime.timezone.utc)
                quotes = lime_client.market.get_quotes_history(
                    symbol=symbol, period=period, from_date=latest_date,
                    to_date=current_date
                )

        res = Parallel(n_jobs=len(symbols), prefer="threads", return_as="generator_unordered")(
            delayed(fetch_live)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)

        if show_progress:
            self._logger.info("Downloading live Lime Trader SDK metadata.")

        while True:
            item = live_data_queue.get()
            yield item

    @staticmethod
    def load_data_table(quotes: list[LimeQuote], show_progress: bool = False):
        data_table = pd.DataFrame(
            [dict(**asdict(quote_hist.quote_history), symbol=quote_hist.symbol) for quote_hist in quotes],)

        data_table.rename(
            columns={
                "timestamp": "date",
                # "ex-dividend": "ex_dividend",
            },
            inplace=True,
            copy=False,
        )
        # data_table = data_table.reset_index()
        data_table = data_table.set_index('date', drop=False)
        return data_table
