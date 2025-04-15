import datetime
from pathlib import Path

import uvloop
from exchange_calendars import get_calendar

from ziplime.data.services.file_system_bundle_registry import FileSystemBundleRegistry
from ziplime.domain.benchmark_spec import BenchmarkSpec
from ziplime.finance.commission import PerShare, DEFAULT_PER_SHARE_COST, DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE, \
    PerContract, DEFAULT_PER_CONTRACT_COST, DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE
from ziplime.finance.constants import FUTURE_EXCHANGE_FEES_BY_SYMBOL
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.finance.metrics import default_metrics
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.finance.slippage.slippage_model import DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT
from ziplime.finance.slippage.volatility_volume_share import VolatilityVolumeShare
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.gens.exchanges.simulation_exchange import SimulationExchange
from ziplime.utils.run_algo import run_algorithm


async def run_simulation(
        simulation_parameters: SimulationParameters,
        algorithm_file: Path,
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
    clock = SimulationClock(
        sessions=sim_params.sessions,
        market_opens=sim_params.market_opens,
        market_closes=sim_params.market_closes,
        before_trading_start_minutes=sim_params.before_trading_start_minutes,
        emission_rate=sim_params.emission_rate,
        timezone=sim_params.trading_calendar.tz,
    )
    with open(algorithm_file, "r") as f:
        algotext = f.read()

    bundle_registry = FileSystemBundleRegistry(base_data_path=bundle_storage_path)

    return await run_algorithm(
        algofile=getattr(algorithm_file, "name", "<algorithm>"),
        algotext=algotext,
        print_algo=True,
        metrics_set=default_metrics(),
        benchmark_spec=benchmark_spec,
        custom_loader=None,
        missing_bundle_data_source=None,
        bundle_registry=bundle_registry,
        simulation_params=simulation_parameters,
        clock=clock
    )


if __name__ == "__main__":
    exchange_class = SimulationExchange(
        name="LIME",
        equity_slippage=FixedBasisPointsSlippage(),
        equity_commission=PerShare(
            cost=DEFAULT_PER_SHARE_COST,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_EQUITY_TRADE,

        ),
        future_slippage=VolatilityVolumeShare(
            volume_limit=DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT,
        ),
        future_commission=PerContract(
            cost=DEFAULT_PER_CONTRACT_COST,
            exchange_fee=FUTURE_EXCHANGE_FEES_BY_SYMBOL,
            min_trade_cost=DEFAULT_MINIMUM_COST_PER_FUTURE_TRADE
        ),
    )
    calendar = get_calendar("NYSE")
    algorithm_file = Path("algorithms/test_algo.py").absolute()
    sim_params = SimulationParameters(
        start_date=datetime.datetime(year=2024, month=10, day=7, tzinfo=calendar.tz),
        end_date=datetime.datetime(year=2024, month=10, day=24, tzinfo=calendar.tz),
        trading_calendar=calendar,
        capital_base=100000.0,
        emission_rate=datetime.timedelta(minutes=1),
        max_shares=10000,
        exchange=exchange_class,
        bundle_name="limex_us_polars_minute",
    )

    result = uvloop.run(run_simulation(simulation_parameters=sim_params, algorithm_file=algorithm_file))
    print(result.head())
