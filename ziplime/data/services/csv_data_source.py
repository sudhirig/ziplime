import datetime
import multiprocessing
import os
import sys
from typing import Self

import limexhub
import structlog
from asyncclick import progressbar
from joblib import Parallel, delayed

import polars as pl
from ziplime.data.services.data_bundle_source import DataBundleSource


class CSVDataSource(DataBundleSource):
    def __init__(self, csv_file_name: str, column_mapping: dict[str, str],
                 date_column_name: str,
                 date_format: str):
        super().__init__()
        self._csv_file_name = csv_file_name
        self._column_mapping = column_mapping
        self._logger = structlog.get_logger(__name__)
        self._date_format = date_format
        self._date_column_name = date_column_name

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:
        df = pl.read_csv(self._csv_file_name)  # s, schema_overrides={self._date_column_name: pl.Datetime})

        # Parse "date" column with your format
        df = df.with_columns(
            pl.col(self._date_column_name).str.strptime(pl.Datetime("us"), format=self._date_format).alias(
                self._date_column_name)
        )

        df = df.rename(self._column_mapping)
        df.with_columns(
            pl.col("close").alias("price"),
            date=pl.col("date").dt.replace_time_zone(str(date_from.tzinfo))
        )
        return df
