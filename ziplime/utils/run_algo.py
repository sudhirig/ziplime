import datetime

import click
import os
import sys

from exchange_calendars import ExchangeCalendar

from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_service import BundleService

from ziplime.algorithm_live import LiveTradingAlgorithm
from ziplime.finance.blotter.blotter_live import BlotterLive
from ziplime.finance.blotter.simulation_blotter import SimulationBlotter
from ziplime.finance.metrics import default_metrics
from ziplime.gens.brokers.broker import Broker
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
        emission_rate: datetime.timedelta,
        capital_base: float,
        bundle_name: str,
        bundle_timestamp,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        output,
        trading_calendar: ExchangeCalendar,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        benchmark_spec,
        broker: Broker,
        market_data_provider: AbstractLiveMarketDataProvider,
        bundle_registry: BundleRegistry,
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.
    """
    # benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)
    bundle_service = BundleService(bundle_registry=bundle_registry)

    bundle_data = await bundle_service.load_bundle(bundle_name=bundle_name, bundle_version=None)
    # date parameter validation
    if not broker and trading_calendar.sessions_distance(start_date, end_date) < 1:
        raise _RunAlgoError(f"There are no trading days between {start_date.date()} and {end_date.date()}")


    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=bundle_data.asset_repository,
        start_date=start_date.date(),
        end_date=end_date.date(),
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


    state_filename = None
    realtime_bar_target = None
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

    # metrics_set = metrics.load(metrics_set)
    metrics_set = default_metrics()

    sim_params = SimulationParameters(
        start_session=start_date,
        end_session=end_date,
        trading_calendar=trading_calendar,
        capital_base=capital_base,
        emission_rate=emission_rate,
        data_frequency=emission_rate,
    )

    if benchmark_sid is not None:
        benchmark_asset = bundle_data.asset_repository.retrieve_asset(sid=benchmark_sid)
        benchmark_returns = None
    else:
        benchmark_asset = None
        benchmark_returns = benchmark_returns
    benchmark_source = BenchmarkSource(
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=trading_calendar,
        sessions=sim_params.sessions,
        bundle_data=bundle_data,
        emission_rate=sim_params.emission_rate,
        timedelta_period=emission_rate,
        benchmark_fields=["close"]
    )

    try:
        if broker is None:
            tr = TradingAlgorithm(
                bundle_data=bundle_data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=SimulationBlotter(),
                benchmark_source=benchmark_source,
                algo_filename=algofile,
                script=algotext
            )
        else:

            blotter_live = BlotterLive(data_frequency=emission_rate, broker=broker)
            tr = LiveTradingAlgorithm(
                broker=broker,
                state_filename=state_filename,
                realtime_bar_target=realtime_bar_target,
                bundle_data=bundle_data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=blotter_live,
                benchmark_source=benchmark_source,
                algo_filename=algofile,
                script=algotext,
            )
        # tr.bundle_data = bundle_data
        # tr.fundamental_data_bundle = bundle_data.fundamental_data_reader
        perf = await tr.run()
    except NoBenchmark:
        raise _RunAlgoError(
            (
                "No ``benchmark_spec`` was provided, and"
                " ``zipline.api.set_benchmark`` was not called in"
                " ``initialize``."
            ),
            (
                "Neither '--benchmark-symbol' nor '--benchmark-sid' was"
                " provided, and ``zipline.api.set_benchmark`` was not called"
                " in ``initialize``. Did you mean to pass '--no-benchmark'?"
            ),
        )

    if output == "-":
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf

async def run_algo_new(
        algofile: str,
        algotext: str,
        emission_rate: datetime.timedelta,
        bundle_name: str,
        bundle_timestamp,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        output,
        trading_calendar: ExchangeCalendar,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        benchmark_spec,
        broker: Broker,
        market_data_provider: AbstractLiveMarketDataProvider,
        bundle_registry: BundleRegistry,
        simulation_params: SimulationParameters
):
    bundle_service = BundleService(bundle_registry=bundle_registry)
    bundle_data = await bundle_service.load_bundle(bundle_name=bundle_name, bundle_version=None)
    # date parameter validation
    if not broker and trading_calendar.sessions_distance(start_date, end_date) < 1:
        raise _RunAlgoError(f"There are no trading days between {start_date.date()} and {end_date.date()}")

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=bundle_data.asset_repository,
        start_date=start_date.date(),
        end_date=end_date.date(),
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


    state_filename = None
    realtime_bar_target = None
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

    # metrics_set = metrics.load(metrics_set)
    metrics_set = default_metrics()


    if benchmark_sid is not None:
        benchmark_asset = bundle_data.asset_repository.retrieve_asset(sid=benchmark_sid)
        benchmark_returns = None
    else:
        benchmark_asset = None
        benchmark_returns = benchmark_returns
    benchmark_source = BenchmarkSource(
        benchmark_asset=benchmark_asset,
        benchmark_returns=benchmark_returns,
        trading_calendar=trading_calendar,
        sessions=sim_params.sessions,
        bundle_data=bundle_data,
        emission_rate=sim_params.emission_rate,
        timedelta_period=emission_rate,
        benchmark_fields=["close"]
    )

    try:
        if broker is None:
            tr = TradingAlgorithm(
                bundle_data=bundle_data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=SimulationBlotter(),
                benchmark_source=benchmark_source,
                algo_filename=algofile,
                script=algotext
            )
        else:

            blotter_live = BlotterLive(data_frequency=emission_rate, broker=broker)
            tr = LiveTradingAlgorithm(
                broker=broker,
                state_filename=state_filename,
                realtime_bar_target=realtime_bar_target,
                bundle_data=bundle_data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=blotter_live,
                benchmark_source=benchmark_source,
                algo_filename=algofile,
                script=algotext,
            )
        # tr.bundle_data = bundle_data
        # tr.fundamental_data_bundle = bundle_data.fundamental_data_reader
        perf = await tr.run()
    except NoBenchmark:
        raise _RunAlgoError(
            (
                "No ``benchmark_spec`` was provided, and"
                " ``zipline.api.set_benchmark`` was not called in"
                " ``initialize``."
            ),
            (
                "Neither '--benchmark-symbol' nor '--benchmark-sid' was"
                " provided, and ``zipline.api.set_benchmark`` was not called"
                " in ``initialize``. Did you mean to pass '--no-benchmark'?"
            ),
        )

    if output == "-":
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf
