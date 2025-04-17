import datetime
from pathlib import Path

import uvloop
from exchange_calendars import get_calendar, ExchangeCalendar

from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES
from ziplime.assets.repositories.sqlite_adjustments_repository import SQLiteAdjustmentRepository
from ziplime.assets.repositories.sqlite_asset_repository import SqliteAssetRepository
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.data.services.file_system_parquet_bundle_storage import FileSystemParquetBundleStorage
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource


async def run_ingest(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: ExchangeCalendar,
        bundle_name: str,
        symbols: list[str],
        frequency: datetime.timedelta,
        bundle_storage_path: str = str(Path(Path.home(), ".ziplime", "data")),

):
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_storage = FileSystemParquetBundleStorage(base_data_path=bundle_storage_path, compression_level=5)
    bundle_data_source = LimexHubDataSource.from_env()


    bundle_version = str(int(datetime.datetime.now(tz=trading_calendar.tz).timestamp()))
    assets_repository = SqliteAssetRepository(base_storage_path=bundle_storage_path,
                                              bundle_name=bundle_name,
                                              bundle_version=bundle_version,
                                              future_chain_predicates=CHAIN_PREDICATES)
    adjustments_repository = SQLiteAdjustmentRepository(base_storage_path=bundle_storage_path,
                                                        bundle_name=bundle_name,
                                                        bundle_version=bundle_version)

    await bundle_service.ingest_bundle(
        date_start=start_date.replace(tzinfo=trading_calendar.tz),
        date_end=end_date.replace(tzinfo=trading_calendar.tz),
        bundle_storage=bundle_storage,
        bundle_data_source=bundle_data_source,
        frequency=frequency,
        symbols=symbols,
        name=bundle_name,
        bundle_version=bundle_version,
        trading_calendar=trading_calendar,
        assets_repository=assets_repository,
        adjustments_repository=adjustments_repository,
    )


if __name__ == "__main__":
    calendar = get_calendar("NYSE")
    start = datetime.datetime(year=2024, month=10, day=5, tzinfo=calendar.tz)
    end = datetime.datetime(year=2024, month=10, day=12, tzinfo=calendar.tz)
    data_frequency=datetime.timedelta(minutes=1)
    bundle_name = "limex_us_polars_minute"
    symbols = ["AAPL", "AMZN"]
    uvloop.run(run_ingest(start_date=start, end_date=end,bundle_name=bundle_name, frequency=data_frequency,
                          trading_calendar=calendar, symbols=symbols))

