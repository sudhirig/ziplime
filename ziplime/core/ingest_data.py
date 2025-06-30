import datetime
import os
from pathlib import Path

import asyncio
from exchange_calendars import get_calendar, ExchangeCalendar

from ziplime.assets.domain.ordered_contracts import CHAIN_PREDICATES
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.currency_symbol_mapping import CurrencySymbolMapping
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.equity_symbol_mapping import EquitySymbolMapping
from ziplime.assets.models.exchange_info import ExchangeInfo
from ziplime.assets.repositories.sqlalchemy_adjustments_repository import SqlAlchemyAdjustmentRepository
from ziplime.assets.repositories.sqlalchemy_asset_repository import SqlAlchemyAssetRepository
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.stock_symbols import ALL_US_STOCK_SYMBOLS
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.data.services.file_system_parquet_bundle_storage import FileSystemParquetBundleStorage
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource


async def add_default_assets(asset_service: AssetService):
    asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
    asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

    usd_currency = Currency(
        asset_name="USD",
        symbol_mapping={
            "LIME": CurrencySymbolMapping(
                symbol="USD",
                exchange_name="LIME",
                start_date=asset_start_date,
                end_date=asset_end_date
            )
        },
        sid=None,
        start_date=asset_start_date,
        end_date=asset_end_date,
        auto_close_date=asset_end_date,
        first_traded=asset_start_date
    )

    equities = [
        Equity(
            asset_name=symbol,
            symbol_mapping={
                "LIME": EquitySymbolMapping(
                    symbol=symbol,
                    exchange_name="LIME",
                    start_date=asset_start_date,
                    end_date=asset_end_date,
                    company_symbol="",
                    share_class_symbol=""
                )
            },
            sid=None,
            start_date=asset_start_date,
            end_date=asset_end_date,
            auto_close_date=asset_end_date,
            first_traded=asset_start_date
        ) for symbol in ALL_US_STOCK_SYMBOLS

    ]

    await asset_service.save_exchanges(
        exchanges=[ExchangeInfo(exchange="LIME", canonical_name="LIME", country_code="US")])
    await asset_service.save_currencies([usd_currency])
    await asset_service.save_equities(equities)


async def _ingest_data(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        trading_calendar: ExchangeCalendar,
        bundle_name: str,
        symbols: list[str],
        frequency: datetime.timedelta,
        forward_fill_missing_ohlcv_data: bool = True,
        bundle_storage_path: str = str(Path(Path.home(), ".ziplime", "data")),
):
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_storage = FileSystemParquetBundleStorage(base_data_path=bundle_storage_path, compression_level=5)
    data_bundle_source = LimexHubDataSource.from_env()

    bundle_version = str(int(datetime.datetime.now(tz=trading_calendar.tz).timestamp()))

    db_path = str(Path(Path.home(), ".ziplime", "assets.sqlite").absolute())
    if os.path.exists(db_path):
        os.remove(db_path)
    db_url = f"sqlite+aiosqlite:///{db_path}"
    assets_repository = SqlAlchemyAssetRepository(db_url=db_url, future_chain_predicates=CHAIN_PREDICATES)
    adjustments_repository = SqlAlchemyAdjustmentRepository(db_url=db_url)
    asset_service = AssetService(asset_repository=assets_repository, adjustments_repository=adjustments_repository)

    await add_default_assets(asset_service=asset_service)

    await bundle_service.ingest_bundle(
        date_start=start_date.replace(tzinfo=trading_calendar.tz),
        date_end=end_date.replace(tzinfo=trading_calendar.tz),
        bundle_storage=bundle_storage,
        data_bundle_source=data_bundle_source,
        frequency=frequency,
        symbols=symbols,
        name=bundle_name,
        bundle_version=bundle_version,
        trading_calendar=trading_calendar,
        asset_service=asset_service,
        forward_fill_missing_ohlcv_data=forward_fill_missing_ohlcv_data,
    )


def ingest_data(start_date: datetime.datetime, end_date: datetime.datetime,
                symbols: list[str],
                trading_calendar: str,
                bundle_name: str,
                data_frequency: datetime.timedelta = datetime.timedelta(minutes=1)):
    calendar = get_calendar(trading_calendar)
    asyncio.run(
        _ingest_data(start_date=start_date, end_date=end_date, bundle_name=bundle_name, frequency=data_frequency,
                     trading_calendar=calendar, symbols=symbols))
