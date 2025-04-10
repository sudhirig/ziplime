import asyncio
import datetime
import logging
from lime_trader import AsyncLimeClient, LimeClient
from lime_trader.models.market import Period

from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
import polars as pl

from ziplime.data.services.bundle_data_source import BundleDataSource


class LimeTraderSdkDataSource(BundleDataSource):
    def __init__(self, lime_sdk_credentials_file: str | None):
        super().__init__()
        self._lime_sdk_credentials_file = lime_sdk_credentials_file
        self._logger = logging.getLogger(__name__)
        if lime_sdk_credentials_file is None:
            self._lime_sdk_client = AsyncLimeClient.from_env(logger=self._logger)
            self._lime_sdk_client_sync = LimeClient.from_env(logger=self._logger)
        else:
            self._lime_sdk_client = AsyncLimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)
            self._lime_sdk_client_sync = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

    # def fetch_live_data_table(
    #         self,
    #         symbols: list[str],
    #         period: Period,
    #         date_from: datetime.datetime,
    #         date_to: datetime.datetime,
    #         show_progress: bool):
    #
    #
    #     def fetch_live(lime_trader_sdk_credentials_file: str, symbol: str):
    #         lime_client = LimeClient.from_file(lime_trader_sdk_credentials_file, logger=self._logger)
    #         try:
    #             quotes = lime_client.market.get_quotes_history(
    #                 symbol=symbol, period=period, from_date=date_from,
    #                 to_date=date_to
    #             )
    #             df = LimeTraderSdkLiveMarketDataProvider.load_data_table(
    #                 quotes=[LimeQuote(symbol=symbol, quote_history=quote) for quote in quotes],
    #                 show_progress=show_progress
    #             )
    #         except Exception as e:
    #             self._logger.error("Error fetching data using lime trader sdk")
    #             df = pd.DataFrame()
    #         return df
    #     res = Parallel(n_jobs=multiprocessing.cpu_count() * 2, prefer="threads", return_as="generator", )(
    #         delayed(fetch_live)(self._lime_sdk_credentials_file, symbol) for symbol in symbols)
    #
    #     result = []
    #     for item in res:
    #         if item is None:
    #             continue
    #         result.append(item)
    #     if show_progress:
    #         self._logger.info("Downloading live Lime Trader SDK metadata.")
    #     return result
    #
    # @staticmethod
    # def load_data_table(quotes: list[LimeQuote], show_progress: bool = False):
    #     if not quotes:
    #         return pd.DataFrame()
    #     data_table = pd.DataFrame(
    #         [dict(**asdict(quote_hist.quote_history), symbol=quote_hist.symbol) for quote_hist in quotes], )
    #
    #     data_table.rename(
    #         columns={
    #             "timestamp": "date",
    #         },
    #         inplace=True,
    #         copy=False,
    #     )
    #     # data_table = data_table.reset_index()
    #     data_table = data_table.set_index('date', drop=False)
    #     return data_table
    #

    def get_data_sync(self, symbols: list[str],
                      frequency: datetime.timedelta,
                      date_from: datetime.datetime,
                      date_to: datetime.datetime,
                      ) -> pl.DataFrame:

        results = [self._lime_sdk_client_sync.market.get_quotes_history(
            symbol=symbol, period=self._frequency_to_period(frequency=frequency), from_date=date_from,
            to_date=date_to
        ) for symbol in symbols]

        cols = {"open": [], "close": [], "high": [], "low": [], "volume": [], "date": [], "exchange": [],
                "symbol": [], "exchange_country": []}
        for results, symbol in zip(results, symbols):
            for result in results:
                cols["open"].append(result.open)
                cols["close"].append(result.close)
                cols["high"].append(result.high)
                cols["low"].append(result.low)
                cols["volume"].append(result.volume)
                cols["date"].append(result.timestamp.astimezone(date_to.tzinfo))
                cols["exchange"].append("LIME")
                cols["exchange_country"].append("US")
                cols["symbol"].append(symbol)
        df = pl.DataFrame(cols, schema=[("open", pl.Float64), ("close", pl.Float64),
                                        ("high", pl.Float64), ("low", pl.Float64),
                                        ("volume", pl.Int64),
                                        ("date", pl.Datetime), ("exchange", pl.String),
                                        ("exchange_country", pl.String), ("symbol", pl.String)
                                        ])
        return df.filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
        for result in results:

            if len(df) > 0:
                df = df.rename(
                    {
                        "o": "open",
                        "h": "high",
                        "l": "low",
                        "c": "close",
                        "v": "volume",
                        "Date": "date"
                    }
                )
                df = df.with_columns(
                    pl.lit(symbol).alias("symbol"),
                    pl.lit("LIME").alias("exchange"),
                    pl.lit("US").alias("exchange_country"),
                    date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)),
                ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
                return df

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       ) -> pl.DataFrame:

        quotes_tasks = [self._lime_sdk_client.market.get_quotes_history(
            symbol=symbol, period=self._frequency_to_period(frequency=frequency), from_date=date_from,
            to_date=date_to
        ) for symbol in symbols]

        results = await asyncio.gather(*quotes_tasks)

        for result in results:
            if len(df) > 0:
                df = df.rename(
                    {
                        "o": "open",
                        "h": "high",
                        "l": "low",
                        "c": "close",
                        "v": "volume",
                        "Date": "date"
                    }
                )
                df = df.with_columns(
                    pl.lit(symbol).alias("symbol"),
                    pl.lit("LIME").alias("exchange"),
                    pl.lit("US").alias("exchange_country"),
                    date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo)),
                ).filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
                return df

    def _frequency_to_period(self, frequency: datetime.timedelta) -> Period:
        match frequency:
            case datetime.timedelta(seconds=60):
                return Period.MINUTE
            case datetime.timedelta(seconds=300):
                return Period.MINUTE_5
            case datetime.timedelta(seconds=900):
                return Period.MINUTE_15
            case datetime.timedelta(seconds=1800):
                return Period.MINUTE_30
            case datetime.timedelta(seconds=3600):
                return Period.HOUR
            case datetime.timedelta(days=7):
                return Period.WEEK
            case datetime.timedelta(days=7):
                return Period.WEEK
            case datetime.timedelta(days=30):
                return Period.MONTH
            case datetime.timedelta(days=90):
                return Period.QUARTER
            case datetime.timedelta(days=365):
                return Period.YEAR
            case _:
                raise ValueError(f"Unknown frequency {frequency} for lime trader sdk")
