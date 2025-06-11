import asyncio
import datetime
from decimal import Decimal
from pathlib import Path

from ziplime.core.run_simulation import run_simulation

if __name__ == "__main__":

    res, errors = run_simulation(
        start_date=datetime.datetime(year=2023, month=2, day=1, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2023, month=2, day=10, tzinfo=datetime.timezone.utc),
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=Decimal(100000.0),
        bundle_name="limex_us_polars_minute",
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(seconds=60),
        benchmark_asset_symbol="VOO",
        benchmark_returns=None,
        stop_on_error=False
    )
    print(errors)

    #daily
    res, errors = run_simulation(
        start_date=datetime.datetime(year=2022, month=1, day=1, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(year=2024, month=2, day=4, tzinfo=datetime.timezone.utc),
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=Decimal(100000.0),
        bundle_name="limex_us_polars_minute",
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(days=1),
        benchmark_asset_symbol="VOO",
        benchmark_returns=None,
        stop_on_error=False
    )
    print(errors)
    print(res.head(n=10))
