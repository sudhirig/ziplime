import asyncio
import datetime
from decimal import Decimal
from pathlib import Path

from ziplime.core.run_simulation import run_simulation
from ziplime.data.services.csv_data_source import CSVDataSource

if __name__ == "__main__":
    # res, errors = run_simulation(
    #     start_date=datetime.datetime(year=2023, month=2, day=1, tzinfo=datetime.timezone.utc),
    #     end_date=datetime.datetime(year=2023, month=2, day=10, tzinfo=datetime.timezone.utc),
    #     trading_calendar="NYSE",
    #     algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
    #     total_cash=Decimal(100000.0),
    #     bundle_name="limex_us_polars_minute",
    #     config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
    #     emission_rate=datetime.timedelta(seconds=60),
    #     benchmark_asset_symbol="VOO",
    #     benchmark_returns=None,
    #     stop_on_error=False
    # )
    # print(errors)
    data_bundle_source = CSVDataSource(csv_file_name="/home/user/Downloads/minute-bars(1).csv", column_mapping={
        "Time": "date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    }, date_format="%Y-%m-%d-%H%M%S%%z")

    # daily
    res, errors = run_simulation(
        start_date=datetime.datetime(year=2023, month=4, day=14, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2025, month=4, day=18, tzinfo=datetime.timezone.utc),
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=Decimal(100000.0),
        market_data_bundle_name="limex_us_polars_minute",
        custom_bundles=["limex_us_fundamental_data"],
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="VOO",
        benchmark_returns=None,
        stop_on_error=True
    )
    # print(res["result"].orders)
    print(errors)
    print(res.head(n=10).to_markdown())
