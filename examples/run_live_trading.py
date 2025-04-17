import datetime
from pathlib import Path

import uvloop
from exchange_calendars import get_calendar

from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.data.services.lime_trader_sdk_data_source import LimeTraderSdkDataSource
from ziplime.domain.benchmark_spec import BenchmarkSpec
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.finance.metrics import default_metrics
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.gens.exchanges.lime_trader_sdk_exchange import LimeTraderSdkExchange
from ziplime.utils.run_algo import run_algorithm


async def run_live_trading(
        simulation_parameters: SimulationParameters,
        algorithm_file: Path,
        lime_sdk_credentials_file: str = None,
        bundle_storage_path: str = Path(Path.home(), ".ziplime", "data"),

):
    # benchmark_spec = BenchmarkSpec(
    #     benchmark_returns=None,
    #     benchmark_sid=benchmark_sid,
    #     benchmark_symbol=benchmark_symbol,
    #     benchmark_file=benchmark_file,
    #     no_benchmark=no_benchmark,
    # )
    benchmark_spec = BenchmarkSpec(
        benchmark_returns=None,
        benchmark_sid=None,
        benchmark_symbol=None,
        benchmark_file=None,
        no_benchmark=True,
    )
    timedelta_diff_from_current_time = datetime.timedelta(seconds=0)

    clock = RealtimeClock(
        sessions=sim_params.sessions,
        market_opens=sim_params.market_opens,
        market_closes=sim_params.market_closes,
        before_trading_start_minutes=sim_params.before_trading_start_minutes,
        emission_rate=sim_params.emission_rate,
        timezone=sim_params.trading_calendar.tz,
        timedelta_diff_from_current_time=-timedelta_diff_from_current_time
    )

    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)

    missing_data_source = LimeTraderSdkDataSource(lime_sdk_credentials_file=lime_sdk_credentials_file)

    return await run_algorithm(
        algorithm_file=str(algorithm_file),
        print_algo=True,
        metrics_set=default_metrics(),
        benchmark_spec=benchmark_spec,
        custom_loader=None,
        missing_bundle_data_source=missing_data_source,
        bundle_registry=bundle_registry,
        simulation_params=simulation_parameters,
        clock=clock
    )


if __name__ == "__main__":
    lime_credentials_file = None

    exchange_class = LimeTraderSdkExchange(
        name="LIME",
        lime_sdk_credentials_file=lime_credentials_file
    )

    calendar = get_calendar("NYSE")
    algorithm_file = Path("algorithms/test_algo.py").absolute()
    # TODO: currently start time needs to be at moment when trading was open. Fix it to allow any date
    start_date = datetime.datetime.now(tz=calendar.tz) - datetime.timedelta(hours=16)
    sim_params = SimulationParameters(
        start_date=start_date,
        end_date=start_date + datetime.timedelta(days=2),
        trading_calendar=calendar,
        capital_base=100000.0,
        emission_rate=datetime.timedelta(seconds=60),
        max_shares=10000,
        exchange=exchange_class,
        bundle_name="limex_us_polars_minute",
    )
    result = uvloop.run(run_live_trading(simulation_parameters=sim_params, algorithm_file=algorithm_file,
                                         lime_sdk_credentials_file=lime_credentials_file))

    print(result.head())
