import datetime
import logging
import multiprocessing

import limexhub
import numpy
import pandas as pd
from joblib import Parallel, delayed

from ziplime.constants.fundamental_data import FundamentalData
from ziplime.data.abstract_fundamendal_data_provider import AbstractFundamentalDataProvider


def _get_fundamental_data_task(
        symbol: str,
        date_from: datetime.datetime,
        date_to: datetime.datetime,
        frequency: datetime.timedelta,
        fundamental_data_list: set[str],
        limex_api_key: str
):
    # print("FUNDAMENTAL DATA")
    limex_client = limexhub.RestAPI(token=limex_api_key)

    dr = pd.date_range(date_from, date_to, freq='D')

    if fundamental_data_list:
        fundamental = limex_client.fundamental(
            symbol=symbol,
            from_date=date_from,
            to_date=date_to,
            fields=None
        )
        if fundamental.empty:
            logging.warning(f"No fundamental data for {symbol} for period {date_from}-{date_to}.")
    else:
        fundamental = pd.DataFrame(columns=["date", "symbol"])
        fundamental.set_index("date", inplace=True, drop=True)
        fundamental = fundamental.reindex(dr, fill_value=None)
        fundamental["symbol"] = symbol
        fundamental.reset_index(inplace=True)
        fundamental.rename(columns={"index": "date"}, inplace=True)
        fundamental.reset_index(inplace=True)
        return fundamental
    if not fundamental.empty:
        fundamental["date"] = pd.to_datetime(fundamental.date, utc=True)
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
        if col_name in fundamental_data_list:
            columns.append(ttm_col)
            columns.append(value_col)
            columns.append(add_value_col)
        else:
            if ttm_col in fundamental_data_list:
                columns.append(ttm_col)
            if value_col in fundamental_data_list:
                columns.append(value_col)
            if add_value_col in fundamental_data_list:
                columns.append(add_value_col)

        res_df = pd.DataFrame(columns=columns)
        if len(columns) <= 2:
            continue
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
                vals_df = pd.DataFrame([
                    vals,
                ], columns=res_df.columns)
                res_df = (
                    res_df.copy()
                    if vals_df.empty
                    else vals_df
                    if res_df.empty
                    else pd.concat([res_df, vals_df], ignore_index=True)
                )
        else:
            vals = [date_from, symbol]
            if ttm_col in columns:
                vals.append(numpy.nan)
            if value_col in columns:
                vals.append(numpy.nan)
            if add_value_col in columns:
                vals.append(numpy.nan)
            vals_df = pd.DataFrame([
                vals,
            ], columns=res_df.columns)

            res_df = (
                res_df.copy()
                if vals_df.empty
                else vals_df
                if res_df.empty
                else pd.concat([vals_df, res_df], ignore_index=True)
            )
        res_df = res_df.drop_duplicates(subset=['date', 'symbol'], keep='last')
        res_df.set_index("date", inplace=True, drop=True)
        res_df = res_df.reindex(dr, fill_value=None)
        res_df["symbol"] = symbol
        res_df.reset_index(inplace=True)
        res_df.rename(columns={"index": "date"}, inplace=True)
        res_df.set_index(["date", "symbol"], inplace=True)
        fundamental_new = pd.concat([res_df, fundamental_new], ignore_index=False, axis=1)

    fundamental_new.reset_index(inplace=True)
    filtered = fundamental_new[fundamental_new.symbol.notnull()]
    # filtered.set_index(["date", "symbol"], inplace=True)

    return filtered


class LimexHubFundamentalDataProvider(AbstractFundamentalDataProvider):
    def __init__(self, limex_api_key: str, maximum_threads: int | None = None):
        self._limex_api_key = limex_api_key
        self._logger = logging.getLogger(__name__)
        self._limex_client = limexhub.RestAPI(token=limex_api_key)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    def get_fundamental_data(self,
                             symbols: list[str],
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             frequency: datetime.timedelta,
                             fundamental_data_list: set[str]
                             ):

        def fetch_fundamental(limex_api_key: str, symbol: str):
            try:
                result = _get_fundamental_data_task(date_from=date_from, date_to=date_to,
                                                    limex_api_key=limex_api_key, symbol=symbol, frequency=frequency,
                                                    fundamental_data_list=fundamental_data_list)
                return result
            except Exception as e:
                logging.exception(
                    f"Exception fetching fundamental data for symbol {symbol}, date_from={date_from}, date_to={date_to}. Skipping."
                )
                return None

        data = pd.DataFrame()

        res = Parallel(n_jobs=self._maximum_threads, prefer="threads",
                       return_as="generator_unordered")(
            delayed(fetch_fundamental)(self._limex_api_key, symbol) for symbol in symbols)
        for item in res:
            if item is None:
                continue
            data = pd.concat([data, item], axis=0, ignore_index=False)
        data.set_index(["date", "symbol"], inplace=True)

        return data

    def get_fundamental_data_column_names(self, fundamental_data_fields: frozenset[str]) -> set[str]:
        columns = set()
        for fund_col in FundamentalData:
            col_name = fund_col.value
            ttm_col = f"{col_name}_ttm"
            value_col = f"{col_name}_value"
            add_value_col = f"{col_name}_add_value"
            if col_name in fundamental_data_fields:
                columns.add(ttm_col)
                columns.add(value_col)
                columns.add(add_value_col)
            else:
                if ttm_col in fundamental_data_fields:
                    columns.add(ttm_col)
                if value_col in fundamental_data_fields:
                    columns.add(value_col)
                if add_value_col in fundamental_data_fields:
                    columns.add(add_value_col)

        return columns
