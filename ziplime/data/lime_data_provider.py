import datetime
import logging
import multiprocessing
import sys
from dataclasses import asdict
from queue import Queue

import limexhub
import numpy
import pandas as pd
from click import progressbar
from joblib import Parallel, delayed
from lime_trader import LimeClient
from lime_trader.models.market import Period

from ziplime.constants.fundamental_data import FundamentalData
from ziplime.domain.lime_quote import LimeQuote


class LimeDataProvider:
    def __init__(self, limex_api_key: str, lime_sdk_credentials_file: str):
        self._limex_api_key = limex_api_key
        self._lime_sdk_credentials_file = lime_sdk_credentials_file

        self._logger = logging.getLogger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    def get_fundamental_data(self, limex_client: limexhub.RestAPI,
                             symbol: str,
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             period: Period,
                             fundamental_data_list: set[str]
                             ):
        # print("FUNDAMENTAL DATA")
        fundamental = limex_client.fundamental(
            symbol=symbol,
            from_date=date_from,
            to_date=date_to,
            fields=None
        )

        dr = pd.date_range(date_from, date_to, freq='D')
        fundamental_new = pd.DataFrame(columns=["date", "symbol"])
        fundamental_new.set_index(keys=["date", "symbol"], inplace=True, drop=True)
        use_datetime = True
        for fund_col in FundamentalData:

            # print(fund_col.value)

            col_name = fund_col.value
            ttm_col = f"{col_name}_ttm"
            value_col = f"{col_name}_value"
            add_value_col = f"{col_name}_add_value"
            columns = ["date", "symbol"]
            if ttm_col in fundamental_data_list:
                columns.append(ttm_col)
            if value_col in fundamental_data_list:
                columns.append(value_col)
            if add_value_col in fundamental_data_list:
                columns.append(add_value_col)

            res_df = pd.DataFrame(columns=columns)

            if "field" in fundamental.columns:
                values_for_col = fundamental[fundamental.field == col_name]
                for index, row in values_for_col.iterrows():
                    if use_datetime:
                        dt = datetime.datetime.combine(row.date, time=datetime.time(), tzinfo=datetime.timezone.utc)
                    else:
                        dt = row.date

                    vals = [dt, symbol]
                    if ttm_col in columns:
                        vals.append(row.ttm)
                    if value_col in columns:
                        vals.append(row.value)
                    if add_value_col in columns:
                        vals.append(row.add_value)

                    res_df = pd.concat([pd.DataFrame([
                        vals,
                    ], columns=res_df.columns), res_df], ignore_index=True)
            else:
                vals = [date_from, symbol]
                if ttm_col in columns:
                    vals.append(numpy.nan)
                if value_col in columns:
                    vals.append(numpy.nan)
                if add_value_col in columns:
                    vals.append(numpy.nan)

                res_df = pd.concat([pd.DataFrame([
                    vals,
                ], columns=res_df.columns), res_df], ignore_index=True)
            res_df.set_index("date", inplace=True, drop=True)
            # if fund_col.value == "dividend_yield":
            #     print("A")
            res_df = res_df.reindex(dr, fill_value=None)  # .ffill().bfill()
            res_df["symbol"] = symbol
            res_df.reset_index(inplace=True)
            res_df.rename(columns={"index": "date"}, inplace=True)
            res_df.set_index(["date", "symbol"], inplace=True)
            fundamental_new = pd.concat([res_df, fundamental_new], ignore_index=False, axis=1)

        fundamental_new.reset_index(inplace=True)
        filtered = fundamental_new[fundamental_new.symbol.notnull()]
        filtered.set_index(["date"], inplace=True)

        return filtered

    def fetch_historical_data_table(self, symbols: list[str],
                                    period: Period,
                                    date_from: datetime.datetime,
                                    date_to: datetime.datetime,
                                    show_progress: bool,
                                    fundamental_data_list: set[str]
                                    ):

        def fetch_historical(limex_api_key: str, symbol: str):
            limex_client = limexhub.RestAPI(token=limex_api_key)
            timeframe = 3
            if period == Period.MINUTE:
                timeframe = 1
            elif period == Period.HOUR:
                timeframe = 2
            elif period == Period.DAY:
                timeframe = 3
            elif period == Period.WEEK:
                timeframe = 4
            elif period == Period.MONTH:
                timeframe = 5
            elif period == Period.QUARTER:
                timeframe = 6
            df = limex_client.candles(symbol=symbol,
                                      from_date=date_from,
                                      to_date=date_to,
                                      timeframe=timeframe)

            fundamental = self.get_fundamental_data(limex_client, symbol, date_from, date_to, period=period,
                                                    fundamental_data_list=fundamental_data_list)
            if len(df) > 0:
                df = df.reset_index()
                df = df.rename(
                    columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "Date": "date"})
                df = df.set_index('date', drop=False)
                df.index = pd.to_datetime(df.index, utc=True)
                df['dividend'] = 0
                df['split'] = 0
                final_df = pd.concat([df, fundamental], ignore_index=False, axis=1)
                final_df = final_df[final_df.date.notnull()]
                final_df["symbol"] = symbol
                return final_df
            return df

        total_days = (date_to - date_from).days
        final = pd.DataFrame()

        if show_progress:
            with progressbar(length=len(symbols) * total_days, label="Downloading historical data from LimexHub",
                             file=sys.stdout) as pbar:
                res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads",
                               return_as="generator_unordered")(
                    delayed(fetch_historical)(self._limex_api_key, symbol) for symbol in symbols)
                for item in res:
                    pbar.update(total_days)
                    final = pd.concat([final, item])
        else:
            res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads", return_as="generator_unordered")(
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
            fundamental_data_list: list[str]):
        historical_data = self.fetch_historical_data_table(symbols=symbols, period=period, date_from=date_from,
                                                           date_to=date_to,
                                                           show_progress=show_progress,
                                                           fundamental_data_list=fundamental_data_list)

        yield historical_data

        # live_data_start_date = max(historical_data.index)[0].to_pydatetime().replace(
        #     tzinfo=datetime.timezone.utc) if len(historical_data) > 0 else date_from

        # for quotes in self.fetch_live_data_table(symbols=symbols, period=period, date_from=live_data_start_date,
        #                                          date_to=date_to,
        #                                          show_progress=show_progress):
        #     yield quotes

    def fetch_live_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool):

        live_data_queue = Queue()

        def fetch_live(lime_trader_sdk_credentials_file: str, symbol: str):
            lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
            try:
                quotes = lime_client.market.get_quotes_history(
                    symbol=symbol, period=period, from_date=date_from,
                    to_date=date_to
                )
                df = LimeDataProvider.load_data_table(
                    quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
                    show_progress=show_progress
                )
            except Exception as e:
                self._logger.error("Error fetching data using lime trader sdk")
                df = pd.DataFrame()
            live_data_queue.put(df)

        res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads", return_as="generator_unordered", )(
            delayed(fetch_live)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)

        if show_progress:
            self._logger.info("Downloading live Lime Trader SDK metadata.")
        processed_symbols = 0
        while True:
            if processed_symbols == len(symbols):
                break
            item = live_data_queue.get()
            yield item
            processed_symbols += 1

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
