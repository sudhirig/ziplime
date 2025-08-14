import asyncio
import datetime
import sys

from ziplime.core.ingest_data import ingest_custom_data, ingest_default_assets, get_asset_service, ingest_market_data, \
    ingest_symbol_universes
from ziplime.data.data_sources.limex_hub_asset_data_source import LimexHubAssetDataSource
from ziplime.data.data_sources.limex_hub_fundamental_data_source import LimexHubFundamentalDataSource
from ziplime.data.data_sources.yahoo_finance_asset_data_source import YahooFinanceAssetDataSource
from ziplime.data.services.csv_data_source import CSVDataSource
from ziplime.data.services.limex_hub_data_source import LimexHubDataSource
from ziplime.data.data_sources.yahoo_finance_data_source import YahooFinanceDataSource

if __name__ == "__main__":
    asset_data_source = LimexHubAssetDataSource.from_env()
    # yfinance_data_source = YahooFinanceAssetDataSource(maximum_threads=1)

    # assets =  asyncio.run(yfinance_data_source.search_assets("a"))
    # sys.exit(0)
    # assets = asyncio.run(asset_data_source.get_assets([]))
    start_date = datetime.datetime(year=2025, month=8, day=5, tzinfo=datetime.timezone.utc)
    end_date = datetime.datetime(year=2025, month=8, day=11, tzinfo=datetime.timezone.utc)
    # market_data_bundle_source = LimexHubDataSource.from_env()
    market_data_bundle_source = YahooFinanceDataSource()
    # asyncio.run(market_data_bundle_source.get_data(
    #     symbols=["AAPL", "AMZN"],
    #     date_from=start_date,
    #     date_to=end_date,
    #     frequency=datetime.timedelta(days=1),
    #
    # ))
    asset_service = get_asset_service(clear_asset_db=True)
    asyncio.run(ingest_default_assets(asset_service=asset_service, asset_data_source=asset_data_source))
    asyncio.run(ingest_symbol_universes(asset_service=asset_service, asset_data_source=asset_data_source))

    ingest_market_data(
        start_date=start_date,
        end_date=end_date,
        symbols=['VOO', 'AAPL', 'AMD', 'AMZN', 'AMGN', 'NVDA'],
        trading_calendar="NYSE",
        bundle_name="limex_us_polars_minute",
        data_bundle_source=market_data_bundle_source,
        data_frequency=datetime.timedelta(days=1),
        asset_service=asset_service
    )
    # ingest fundamental data from limex hub
    # data_bundle_source = LimexHubFundamentalDataSource.from_env()
    # ingest_custom_data(
    #     start_date=start_date,
    #     end_date=end_date,
    #     symbols=['VOO', 'AAPL', 'AMD', 'AMZN', 'AMGN', 'NVDA'],
    #     trading_calendar="NYSE",
    #     bundle_name="limex_us_fundamental_data",
    #     data_bundle_source=data_bundle_source,
    #     data_frequency="1mo",
    #     data_frequency_use_window_end=True,
    #     asset_service=asset_service
    # )
