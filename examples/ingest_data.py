import asyncio
import datetime

from ziplime.core.ingest_data import ingest_custom_data, ingest_default_assets, get_asset_service, ingest_market_data, \
    ingest_symbol_universes
from ziplime.data.data_sources.limex_hub_asset_data_source import LimexHubAssetDataSource
from ziplime.data.data_sources.limex_hub_fundamental_data_source import LimexHubFundamentalDataSource
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource

if __name__ == "__main__":
    asset_data_source = LimexHubAssetDataSource.from_env()
    # assets = asyncio.run(asset_data_source.get_assets([]))
    asset_service = get_asset_service(clear_asset_db=True)
    asyncio.run(ingest_default_assets(asset_service=asset_service, asset_data_source=asset_data_source))
    asyncio.run(ingest_symbol_universes(asset_service=asset_service, asset_data_source=asset_data_source))

    market_data_bundle_source = LimexHubDataSource.from_env()
    ingest_market_data(
        start_date=datetime.datetime(year=2023, month=1, day=3, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2025, month=5, day=11, tzinfo=datetime.timezone.utc),
        symbols=['VOO', 'AAPL', 'AMD', 'AMZN', 'AMGN', 'NVDA'],
        trading_calendar="NYSE",
        bundle_name="limex_us_polars_minute",
        data_bundle_source=market_data_bundle_source,
        data_frequency=datetime.timedelta(minutes=1),
        asset_service=asset_service
    )
    # ingest fundamental data from limex hub
    data_bundle_source = LimexHubFundamentalDataSource.from_env()
    ingest_custom_data(
        start_date=datetime.datetime(year=2023, month=1, day=3, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2025, month=5, day=11, tzinfo=datetime.timezone.utc),
        symbols=['VOO', 'AAPL', 'AMD', 'AMZN', 'AMGN', 'NVDA'],
        trading_calendar="NYSE",
        bundle_name="limex_us_fundamental_data",
        data_bundle_source=data_bundle_source,
        data_frequency="1mo",
        data_frequency_use_window_end=True,
        asset_service=asset_service
    )
