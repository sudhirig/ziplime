import datetime
from dataclasses import dataclass

import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.assets.repositories.adjustments_repository import AdjustmentRepository
from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.data.abstract_data_bundle import AbstractDataBundle


@dataclass
class BundleData:
    name: str
    version: str

    start_date: datetime.date
    end_date: datetime.date
    trading_calendar: ExchangeCalendar
    frequency: datetime.timedelta
    timestamp: datetime.datetime
    asset_repository: AssetRepository
    adjustment_repository: AdjustmentRepository


    historical_data_reader: AbstractDataBundle
    fundamental_data_reader: AbstractDataBundle


    data: pl.DataFrame




