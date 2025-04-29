import datetime
import sys

import structlog

from ziplime.assets.services.asset_service import AssetService
from ziplime.core.algorithm_file import AlgorithmFile
from ziplime.domain.benchmark_spec import BenchmarkSpec
from ziplime.exchanges.exchange import Exchange

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


def get_benchmark(clock: TradingClock):
    benchmark_spec = BenchmarkSpec(
        benchmark_returns=None,
        benchmark_sid=None,
        benchmark_symbol=None,
        benchmark_file=None,
        no_benchmark=True,
    )
    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=None,
        start_date=clock.start_session,
        end_date=clock.end_session,
    )

    if benchmark_sid is not None:
        benchmark_asset = data_bundle.asset_repository.retrieve_asset(sid=benchmark_sid)
        benchmark_returns = None
    else:
        benchmark_asset = None
        benchmark_returns = benchmark_returns

    benchmark_source = BenchmarkSource(
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=clock.trading_calendar,
        sessions=clock.sessions,
        data_bundle=None,
        emission_rate=clock.emission_rate,
        timedelta_period=clock.emission_rate,
        benchmark_fields=["close"]
    )

    return benchmark_source
async def run_algorithm(
        algorithm: AlgorithmFile,
        asset_service: AssetService,
        print_algo: bool,
        exchanges: list[Exchange],
        metrics_set: str,
        custom_loader,
        # benchmark_spec,
        clock: TradingClock,
):
    """Run a backtest for the given algorithm.
    This is shared between the cli and :func:`ziplime.run_algo`.
    """

    if print_algo:

        if PYGMENTS:
            highlight(
                algorithm.algorithm_text,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            logger.info(f"\n{algorithm.algorithm_text}")
    exchanges_dict = {exchange.name: exchange for exchange in exchanges}
    pipeline_loader = USEquityPricingLoader.without_fx(None)  # TODO: fix pipeline

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        try:
            return custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)

    tr = TradingAlgorithm(
        exchanges=exchanges_dict,
        asset_service=asset_service,
        get_pipeline_loader=choose_loader,
        metrics_set=metrics_set,
        blotter=InMemoryBlotter(exchanges=exchanges_dict, cancel_policy=None),
        benchmark_source=get_benchmark(clock=clock),
        algorithm=algorithm,
        clock=clock,
    )

    start_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    perf = await tr.run()
    end_time = datetime.datetime.now(tz=clock.trading_calendar.tz)
    logger.info(f"Backtest completed in {int((end_time - start_time).total_seconds())} seconds.")
    return perf
