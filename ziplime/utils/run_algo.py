import datetime

import click
import os
import sys

from exchange_calendars import ExchangeCalendar
from zipline.utils.paths import data_path

from ziplime.algorithm_live import LiveTradingAlgorithm
from ziplime.domain.data_frequency import DataFrequency
from ziplime.finance.blotter.blotter_live import BlotterLive
from ziplime.finance.blotter.simulation_blotter import SimulationBlotter
from ziplime.finance.metrics import default_metrics
from ziplime.gens.brokers.broker import Broker
from ziplime.data.abstract_live_market_data_provider import AbstractLiveMarketDataProvider
from ziplime.data.data_portal_live import DataPortalLive

try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False
import logging

from ziplime.data import bundles
from ziplime.data.data_portal import DataPortal
from zipline.finance import metrics
from ziplime.finance.domain.simulation_paremeters import SimulationParameters
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.loaders import USEquityPricingLoader

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
def run_algorithm(
        algofile: str,
        algotext,
        data_frequency: DataFrequency,
        capital_base: float,
        bundle: str,
        bundle_timestamp,
        start: datetime.datetime,
        end: datetime.datetime,
        output,
        trading_calendar: ExchangeCalendar,
        print_algo: bool,
        metrics_set: str,
        custom_loader,
        benchmark_spec,
        broker: Broker,
        market_data_provider: AbstractLiveMarketDataProvider,
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.
    """
    # benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)

    bundle_data = bundles.load(
        name=bundle,
        timestamp=bundle_timestamp,
        period=data_frequency,
    )

    # date parameter validation
    if not broker and trading_calendar.sessions_distance(start, end) < 1:
        raise _RunAlgoError(
            "There are no trading days between %s and %s"
            % (
                start.date(),
                end.date(),
            ),
        )

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_repository=bundle_data.asset_repository,
        start_date=start.date(),
        end_date=end.date(),
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

    first_trading_day = bundle_data.historical_data_reader.first_trading_day

    state_filename = None
    realtime_bar_target = None
    # emission_rate = data_frequency
    if broker:
        data = DataPortalLive(
            asset_repository=bundle_data.asset_repository,
            broker=broker,
            trading_calendar=trading_calendar,
            first_trading_day=first_trading_day,
            historical_data_reader=bundle_data.historical_data_reader,
            adjustment_reader=bundle_data.adjustment_reader,
            future_minute_reader=bundle_data.historical_data_reader,
            future_daily_reader=bundle_data.historical_data_reader,
            market_data_provider=market_data_provider,
            fundamental_data_reader=bundle_data.fundamental_data_reader,
            fields=bundle_data.historical_data_reader.get_fields()
        )
        state_filename = f"{data_path(['state'])}"
        realtime_bar_target = f"{data_path(['realtime'])}"
        # emission_rate = 'minute'
    else:
        data = DataPortal(
            bundle_data.asset_repository,
            trading_calendar=trading_calendar,
            first_trading_day=first_trading_day,
            historical_data_reader=bundle_data.historical_data_reader,
            fundamental_data_reader=bundle_data.fundamental_data_reader,
            adjustment_reader=bundle_data.adjustment_reader,
            future_minute_reader=bundle_data.historical_data_reader,
            future_daily_reader=bundle_data.historical_data_reader,
            fields=bundle_data.historical_data_reader.get_fields()
        )

    pipeline_loader = USEquityPricingLoader.without_fx(
        bundle_data.historical_data_reader,
        bundle_data.adjustment_reader,
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
        start_session=start,
        end_session=end,
        trading_calendar=trading_calendar,
        capital_base=capital_base,
        emission_rate=data_frequency,
        data_frequency=data_frequency,
    )

    try:
        if broker is None:
            tr = TradingAlgorithm(
                data_portal=data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=SimulationBlotter(),
                benchmark_returns=benchmark_returns,
                benchmark_sid=benchmark_sid,
                algo_filename=algofile,
                script=algotext
            )
        else:

            blotter_live = BlotterLive(data_frequency=data_frequency, broker=broker)
            tr = LiveTradingAlgorithm(
                broker=broker,
                state_filename=state_filename,
                realtime_bar_target=realtime_bar_target,
                data_portal=data,
                get_pipeline_loader=choose_loader,
                sim_params=sim_params,
                metrics_set=metrics_set,
                blotter=blotter_live,
                benchmark_returns=benchmark_returns,
                benchmark_sid=benchmark_sid,
                algo_filename=algofile,
                script=algotext,
            )
        tr.bundle_data = bundle_data
        tr.fundamental_data_bundle = bundle_data.fundamental_data_reader
        perf = tr.run()
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

#
# def run_algorithm(
#         start: datetime.datetime,
#         end: datetime.datetime,
#         capital_base: float,
#         data_frequency: DataFrequency,
#         before_trading_start=None,
#         analyze=None,
#         bundle: str = "lime",
#         bundle_timestamp=None,
#         trading_calendar=None,
#         metrics_set="default",
#         benchmark_returns=None,
#         environ=os.environ,
#         custom_loader=None,
#         print_algo: bool = False,
#         algotext=None,
#         algofile=None,
#         market_data_provider: AbstractLiveMarketDataProvider = None,
#         broker: Broker = None
# ):
#     """
#     Run a trading algorithm.
#
#     Parameters
#     ----------
#     start : datetime
#         The start date of the backtest.
#     end : datetime
#         The end date of the backtest..
#     initialize : callable[context -> None]
#         The initialize function to use for the algorithm. This is called once
#         at the very begining of the backtest and should be used to set up
#         any state needed by the algorithm.
#     capital_base : float
#         The starting capital for the backtest.
#     handle_data : callable[(context, BarData) -> None], optional
#         The handle_data function to use for the algorithm. This is called
#         every minute when ``data_frequency == 'minute'`` or every day
#         when ``data_frequency == 'daily'``.
#     before_trading_start : callable[(context, BarData) -> None], optional
#         The before_trading_start function for the algorithm. This is called
#         once before each trading day (after initialize on the first day).
#     analyze : callable[(context, pd.DataFrame) -> None], optional
#         The analyze function to use for the algorithm. This function is called
#         once at the end of the backtest and is passed the context and the
#         performance data.
#     data_frequency : {'daily', 'minute'}, optional
#         The data frequency to run the algorithm at.
#     bundle : str, optional
#         The name of the data bundle to use to load the data to run the backtest
#         with. This defaults to 'quantopian-quandl'.
#     bundle_timestamp : datetime, optional
#         The datetime to lookup the bundle data for. This defaults to the
#         current time.
#     trading_calendar : TradingCalendar, optional
#         The trading calendar to use for your backtest.
#     metrics_set : iterable[Metric] or str, optional
#         The set of metrics to compute in the simulation. If a string is passed,
#         resolve the set with :func:`zipline.finance.metrics.load`.
#     benchmark_returns : pd.Series, optional
#         Series of returns to use as the benchmark.
#     environ : mapping[str -> str], optional
#         The os environment to use. Many extensions use this to get parameters.
#         This defaults to ``os.environ``.
#
#     Returns
#     -------
#     perf : pd.DataFrame
#         The daily performance of the algorithm.
#
#     See Also
#     --------
#     zipline.data.bundles.bundles : The available data bundles.
#     """
#
#     benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)
#
#     return _run(
#         before_trading_start=before_trading_start,
#         analyze=analyze,
#         algofile=algofile,
#         algotext=algotext,
#         data_frequency=data_frequency,
#         capital_base=capital_base,
#         bundle=bundle,
#         bundle_timestamp=bundle_timestamp,
#         start=start,
#         end=end,
#         output=os.devnull,
#         trading_calendar=trading_calendar,
#         print_algo=print_algo,
#         metrics_set=metrics_set,
#         local_namespace=False,
#         environ=environ,
#         custom_loader=custom_loader,
#         benchmark_spec=benchmark_spec,
#         broker=broker,
#         market_data_provider=market_data_provider,
#     )
