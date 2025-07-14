import asyncio
import datetime
from pathlib import Path

import pytz

from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.bundle_service import BundleService
from ziplime.data.services.csv_data_source import CSVDataSource
from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from exchange_calendars import get_calendar


async def _run_simulation():
    bundle_storage_path = str(Path(Path.home(), ".ziplime", "data"))
    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)
    bundle_service = BundleService(bundle_registry=bundle_registry)
    asset_service = get_asset_service(clear_asset_db=False)

    market_data_bundle = await bundle_service.load_bundle(bundle_name="limex_us_polars_minute", bundle_version=None,
                                                          frequency=datetime.timedelta(days=1),
                                                          start_date=datetime.datetime(year=2024, month=4, day=14,
                                                                                       tzinfo=pytz.timezone("America/New_York")),
                                                          end_date=datetime.datetime(year=2025, month=4, day=18,
                                                                                     tzinfo=pytz.timezone("America/New_York")),
                                                          symbols=["AAPL", "NVDA", "AMD", "AMGN", "VOO"]

                                                          )

    custom_data_sources = []
    custom_data_sources.append(
        await bundle_service.load_bundle(bundle_name="limex_us_fundamental_data", bundle_version=None))
    # custom_data_bundles.append(await bundle_service.load_bundle(bundle_name="custom_minute_bars", bundle_version=None))
    data_bundle_source = CSVDataSource(
        csv_file_name="/home/user/Downloads/minute-bars(1).csv",
        column_mapping={
            "Time": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
            "EventSymbol": "symbol"
        },
        date_format="%Y%m%d-%H%M%S%z",
        name="custom_minute_bars",
        date_column_name="Time",
        frequency=datetime.timedelta(minutes=1),
        trading_calendar=get_calendar("NYSE"),
        asset_service=asset_service,
        data_frequency_use_window_end=False,
        symbols=["SPX"]
    )
    await data_bundle_source.load_data_in_memory()
    custom_data_sources.append(data_bundle_source)
    # res, errors = run_simulation(
    #     start_date=datetime.datetime(year=2023, month=2, day=1, tzinfo=datetime.timezone.utc),
    #     end_date=datetime.datetime(year=2023, month=2, day=10, tzinfo=datetime.timezone.utc),
    #     trading_calendar="NYSE",
    #     algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
    #     total_cash=100000.0,
    #     bundle_name="limex_us_polars_minute",
    #     config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
    #     emission_rate=datetime.timedelta(seconds=60),
    #     benchmark_asset_symbol="VOO",
    #     benchmark_returns=None,
    #     stop_on_error=False
    # )
    # print(errors)

    # print(res["result"].orders)
    # daily
    res, errors = await run_simulation(
        start_date=datetime.datetime(year=2024, month=4, day=14, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2025, month=4, day=18, tzinfo=datetime.timezone.utc),
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        market_data_source=market_data_bundle,
        custom_data_sources=custom_data_sources,
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="VOO",
        benchmark_returns=None,
        stop_on_error=True
    )
    print(errors)
    print(res.head(n=10).to_markdown())


if __name__ == "__main__":
    asyncio.run(
        _run_simulation(

        )
    )
