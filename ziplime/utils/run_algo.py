import datetime
import sys

import structlog

from ziplime.data.services.bundle_data_source import BundleDataSource
from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_service import BundleService

from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.sources.benchmark_source import BenchmarkSource

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False

from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.pipeline.data import USEquityPricing
from ziplime.pipeline.loaders import USEquityPricingLoader

from ziplime.algorithm import TradingAlgorithm, NoBenchmark

logger = structlog.get_logger(__name__)


async def run_algorithm(
        algorithm_file: str,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        benchmark_spec,
        clock: TradingClock,
        missing_bundle_data_source: BundleDataSource,
        simulation_params: SimulationParameters,
        bundle_registry: BundleRegistry,
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`ziplime.run_algo`.
    """
    bundle_service = BundleService(bundle_registry=bundle_registry)

    bundle_data = await bundle_service.load_bundle(bundle_name=simulation_params.bundle_name, bundle_version=None,
                                                   missing_bundle_data_source=missing_bundle_data_source)
    # date parameter validation
    if simulation_params.trading_calendar.sessions_distance(simulation_params.start_session,
                                                            simulation_params.end_session) < 1:
        raise Exception(
            f"There are no trading days between {simulation_params.start_session} and {simulation_params.end_session}")

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=bundle_data.asset_repository,
        start_date=simulation_params.start_session,
        end_date=simulation_params.end_session,
    )

    if print_algo:
        with open(algorithm_file, "r") as f:
            algotext = f.read()
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            logger.info(f"\n{algotext}")

    pipeline_loader = USEquityPricingLoader.without_fx(
        bundle_data,
    )

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        try:
            return custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)

    if benchmark_sid is not None:
        benchmark_asset = bundle_data.asset_repository.retrieve_asset(sid=benchmark_sid)
        benchmark_returns = None
    else:
        benchmark_asset = None
        benchmark_returns = benchmark_returns
    benchmark_source = BenchmarkSource(
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=simulation_params.trading_calendar,
        sessions=simulation_params.sessions,
        bundle_data=bundle_data,
        emission_rate=simulation_params.emission_rate,
        timedelta_period=simulation_params.emission_rate,
        benchmark_fields=["close"]
    )

    tr = TradingAlgorithm(
        exchange=simulation_params.exchange,
        bundle_data=bundle_data,
        get_pipeline_loader=choose_loader,
        sim_params=simulation_params,
        metrics_set=metrics_set,
        blotter=InMemoryBlotter(exchange=simulation_params.exchange, cancel_policy=None),
        benchmark_source=benchmark_source,
        algorithm_file=algorithm_file,
        clock=clock
    )
    start_time = datetime.datetime.now(tz=simulation_params.trading_calendar.tz)
    perf = await tr.run()
    end_time = datetime.datetime.now(tz=simulation_params.trading_calendar.tz)
    logger.info(f"Backtest completed in {int((end_time - start_time).total_seconds())} seconds.")
    return perf
