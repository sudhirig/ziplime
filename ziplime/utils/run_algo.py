import datetime

import click
import os
import sys
from exchange_calendars import ExchangeCalendar

from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_service import BundleService

from ziplime.finance.blotter.in_memory_blotter import InMemoryBlotter
from ziplime.gens.domain.realtime_clock import RealtimeClock
from ziplime.gens.domain.simulation_clock import SimulationClock
from ziplime.gens.domain.trading_clock import TradingClock
from ziplime.gens.exchanges.exchange import Exchange
from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
from ziplime.sources.benchmark_source import BenchmarkSource

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False
import logging

from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from ziplime.pipeline.data import USEquityPricing
from ziplime.pipeline.loaders import USEquityPricingLoader

from ziplime.algorithm import TradingAlgorithm, NoBenchmark

log = logging.getLogger(__name__)


class _RunAlgoError(click.ClickException, ValueError):
    """Signal an error that should have a different message if invoked from
    the cli.

    Parameters
    ----------
    pyfunc_msg : str
        The message that will be shown when called as a python function.
    cmdline_msg : str, optional
        The message that will be shown on the command line. If not provided,
        this will be the same as ``pyfunc_msg`
    """

    exit_code = 1

    def __init__(self, pyfunc_msg, cmdline_msg=None):
        if cmdline_msg is None:
            cmdline_msg = pyfunc_msg

        super(_RunAlgoError, self).__init__(cmdline_msg)
        self.pyfunc_msg = pyfunc_msg

    def __str__(self):
        return self.pyfunc_msg


# TODO: simplify
# flake8: noqa: C901
async def run_algorithm(
        algofile: str,
        algotext: str,
        output,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        benchmark_spec,
        clock: TradingClock,
        market_data_provider: AbstractLiveMarketDataProvider,
        simulation_params: SimulationParameters,
        bundle_registry: BundleRegistry,
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.
    """
    # benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)
    bundle_service = BundleService(bundle_registry=bundle_registry)

    bundle_data = await bundle_service.load_bundle(bundle_name=simulation_params.bundle_name, bundle_version=None)
    # date parameter validation
    if simulation_params.trading_calendar.sessions_distance(simulation_params.start_session, simulation_params.end_session) < 1:
        raise _RunAlgoError(f"There are no trading days between {simulation_params.start_session} and {simulation_params.end_session}")

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=bundle_data.asset_repository,
        start_date=simulation_params.start_session,
        end_date=simulation_params.end_session,
    )

    if print_algo:
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            click.echo(algotext)

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

    # clock = SimulationClock(
    #     sessions=sim_params.sessions,
    #     market_opens=sim_params.market_opens,
    #     market_closes=sim_params.market_closes,
    #     before_trading_start_minutes=sim_params.before_trading_start_minutes,
    #     emission_rate=sim_params.emission_rate,
    #     timezone=sim_params.trading_calendar.tz
    # )
    # timedelta_diff_from_current_time = datetime.datetime.now(tz=simulation_params.trading_calendar.tz) - start_date.replace(tzinfo=simulation_params.trading_calendar.tz)
    # clock = RealtimeClock(
    #     sessions=simulation_params.sessions,
    #     market_opens=simulation_params.market_opens,
    #     market_closes=simulation_params.market_closes,
    #     before_trading_start_minutes=simulation_params.before_trading_start_minutes,
    #     emission_rate=simulation_params.emission_rate,
    #     timezone=simulation_params.trading_calendar.tz,
    #     timedelta_diff_from_current_time=-timedelta_diff_from_current_time
    # )


    tr = TradingAlgorithm(
        exchange=simulation_params.exchange,
        bundle_data=bundle_data,
        get_pipeline_loader=choose_loader,
        sim_params=simulation_params,
        metrics_set=metrics_set,
        blotter=InMemoryBlotter(exchange=simulation_params.exchange, cancel_policy=None),
        benchmark_source=benchmark_source,
        algo_filename=algofile,
        script=algotext,
        clock=clock
    )
        # else:
        #
        #     blotter_live = BlotterLive(data_frequency=emission_rate, exchange=exchange)
        #     tr = LiveTradingAlgorithm(
        #         exchange=exchange,
        #         state_filename=state_filename,
        #         realtime_bar_target=realtime_bar_target,
        #         bundle_data=bundle_data,
        #         get_pipeline_loader=choose_loader,
        #         sim_params=sim_params,
        #         metrics_set=metrics_set,
        #         blotter=blotter_live,
        #         benchmark_source=benchmark_source,
        #         algo_filename=algofile,
        #         script=algotext,
        #     )
        # tr.bundle_data = bundle_data
        # tr.fundamental_data_bundle = bundle_data.fundamental_data_reader
    perf = await tr.run()

    if output == "-":
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf