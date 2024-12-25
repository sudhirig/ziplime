import datetime
import logging
import os

import pandas as pd
from zipline.__main__ import ipython_only

from zipline.utils.calendar_utils import get_calendar
from zipline.utils.cli import Date

from ziplime.constants.default_columns import DEFAULT_COLUMNS, OHLCV_COLUMNS
from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.data.storages.bcolz_data_bundle import BcolzDataBundle
from ziplime.utils.run_algo import _run, BenchmarkSpec

import click
import zipline
from ziplime.data import bundles as bundles_module

from click import DateTime
from lime_trader.models.market import Period
from zipline.utils.cli import Timestamp

from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle

from zipline import __main__ as zipline__main__
from zipline.utils.run_algo import load_extensions
from zipline.extensions import create_args

from ziplime.utils.bundle_utils import register_default_bundles, get_historical_market_data_provider, \
    get_fundamental_data_provider, get_live_market_data_provider, get_broker

DEFAULT_BUNDLE = "lime"


def validate_date_range(date_min: datetime.datetime, date_max: datetime.datetime):
    def _validate_date_range(ctx, param, value):
        if not date_min < value.replace(tzinfo=datetime.timezone.utc) < date_max:
            raise click.BadParameter(f"Must be between {date_min} and {date_max}")
        return value

    return _validate_date_range


@click.group()
@click.option(
    "-e",
    "--extension",
    multiple=True,
    help="File or module path to a zipline extension to load.",
)
@click.option(
    "--strict-extensions/--non-strict-extensions",
    is_flag=True,
    help="If --strict-extensions is passed then zipline will not "
         "run if it cannot load all of the specified extensions. "
         "If this is not passed or --non-strict-extensions is passed "
         "then the failure will be logged but execution will continue.",
)
@click.option(
    "--default-extension/--no-default-extension",
    is_flag=True,
    default=True,
    help="Don't load the default zipline extension.py file in $ZIPLINE_HOME.",
)
@click.option(
    "-x",
    multiple=True,
    help="Any custom command line arguments to define, in key=value form.",
)
@click.pass_context
def main(ctx, extension, strict_extensions, default_extension, x):
    """Top level ziplime entry point."""
    # install a logging handler before performing any other operations

    logging.basicConfig(
        format="[%(asctime)s-%(levelname)s][%(name)s]\n %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    register_default_bundles()

    create_args(x, zipline.extension_args)
    load_extensions(
        default_extension,
        extension,
        strict_extensions,
        os.environ,
    )


@main.command(context_settings=dict(
    # ignore_unknown_options=True,
    # allow_extra_args=True,
))
@click.option(
    "-b",
    "--bundle",
    default=DEFAULT_BUNDLE,
    metavar="BUNDLE-NAME",
    show_default=True,
    help="The data bundle to ingest.",
)
@click.option(
    "-c",
    "--calendar",
    default="NYSE",
    show_default=True,
    help="Default calendar to use.",
)
@click.option('--new-bundle-name', default=None)
@click.option(
    "--start-date",
    type=DateTime(),
    callback=validate_date_range(date_min=datetime.datetime(year=1800,
                                                            month=1,
                                                            day=1,
                                                            tzinfo=datetime.timezone.utc
                                                            ),
                                 date_max=datetime.datetime.now(tz=datetime.timezone.utc),
                                 )
)
@click.option(
    "--end-date",
    type=DateTime(),
    callback=validate_date_range(date_min=datetime.datetime(year=1800,
                                                            month=1,
                                                            day=1,
                                                            tzinfo=datetime.timezone.utc
                                                            ),
                                 date_max=datetime.datetime.now(tz=datetime.timezone.utc),
                                 )
)
@click.option(
    "--period",
    default="day",
    type=click.Choice(['minute', 'hour', 'day', 'week', 'month', 'quarter'], case_sensitive=False),
)
@click.option("-s", '--symbols')
@click.option('--fundamental-data')
@click.option(
    "--assets-version",
    type=int,
    multiple=True,
    help="Version of the assets db to which to downgrade.",
)
@click.option(
    "--show-progress/--no-show-progress",
    default=True,
    help="Print progress information to the terminal.",
)
@click.option(
    "--historical-market-data-provider",
    default="limex-hub",
    help="Market data provider for historical data",
    show_default=True,
)
@click.option(
    "--fundamental-data-provider",
    default="limex-hub",
    help="Fundamental data provider",
    show_default=True,
)
@click.pass_context
def ingest(ctx, bundle, new_bundle_name, start_date, end_date, period, symbols, fundamental_data, show_progress,
           assets_version, calendar,
           historical_market_data_provider,
           fundamental_data_provider
           ):
    """Top level ziplime entry point."""
    symbols_parsed = symbols.split(',') if symbols else None
    fundamental_data_list = fundamental_data.split(",") if fundamental_data else None
    if new_bundle_name:
        bundle_name = f"{DEFAULT_BUNDLE}-{new_bundle_name}"
        ctx.args = ['-b', new_bundle_name] + ctx.args
    else:
        bundle_name = bundle
        ctx.args = ['-b', bundle] + ctx.args
    # install a logging handler before performing any other operations
    register_lime_symbol_list_equities_bundle(
        bundle_name=bundle_name,
        symbols=symbols_parsed,
        start_session=start_date,
        end_session=end_date,
        period=Period(period),
        calendar_name=calendar,
        fundamental_data_list=fundamental_data_list if fundamental_data_list is not None else [
            col.name for col in
            FUNDAMENTAL_DATA_COLUMNS
        ]
    )

    new_params = dict(**ctx.params)

    # clean up lime only params and set new bundle name
    new_params["bundle"] = bundle_name

    fundamental_data_provider_instance = get_fundamental_data_provider(code=fundamental_data_provider)
    fundamental_data_column_names = fundamental_data_provider_instance.get_fundamental_data_column_names(
        fundamental_data_fields=set(fundamental_data_list))
    fundamental_data_cols = (
        [
            col
            for col in FUNDAMENTAL_DATA_COLUMNS
            if col.name in fundamental_data_column_names
        ]
        if fundamental_data_list is not None
        else DEFAULT_COLUMNS
    )
    bundles_module.ingest(
        name=bundle_name,
        environ=os.environ,
        timestamp=pd.Timestamp.utcnow(),
        assets_version=assets_version,
        show_progress=show_progress,
        historical_market_data_provider=get_historical_market_data_provider(code=historical_market_data_provider),
        fundamental_data_provider=fundamental_data_provider_instance,
        data_bundle_writer_class=BcolzDataBundle,
        fundamental_data_writer_class=BcolzDataBundle,
        market_data_fields=OHLCV_COLUMNS,
        fundamental_data_fields=fundamental_data_cols,
    )


@main.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option(
    "-b",
    "--bundle",
    default=DEFAULT_BUNDLE,
    metavar="BUNDLE-NAME",
    show_default=True,
    help="The data bundle to clean.",
)
@click.option(
    "-e",
    "--before",
    type=Timestamp(),
    help="Clear all data before TIMESTAMP."
         " This may not be passed with -k / --keep-last",
)
@click.option(
    "-a",
    "--after",
    type=Timestamp(),
    help="Clear all data after TIMESTAMP"
         " This may not be passed with -k / --keep-last",
)
@click.option(
    "-k",
    "--keep-last",
    type=int,
    metavar="N",
    help="Clear all but the last N downloads."
         " This may not be passed with -e / --before or -a / --after",
)
@click.pass_context
def clean(ctx, bundle, before, after, keep_last):
    """Top level ziplime entry point."""

    func = getattr(zipline__main__, "clean")
    ctx.forward(func)


@main.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
def bundles(ctx):
    """Top level ziplime entry point."""
    func = getattr(zipline__main__, "bundles")
    ctx.forward(func)


@main.command()
@click.option(
    "-f",
    "--algofile",
    default=None,
    type=click.File("r"),
    help="The file that contains the algorithm to run.",
)
@click.option(
    "-t",
    "--algotext",
    help="The algorithm script to run.",
)
@click.option(
    "-D",
    "--define",
    multiple=True,
    help="Define a name to be bound in the namespace before executing"
         " the algotext. For example '-Dname=value'. The value may be any "
         "python expression. These are evaluated in order so they may refer "
         "to previously defined names.",
)
@click.option(
    "--data-frequency",
    type=click.Choice({"daily", "minute"}),
    default="daily",
    show_default=True,
    help="The data frequency of the simulation.",
)
@click.option(
    "--capital-base",
    type=float,
    default=10e6,
    show_default=True,
    help="The starting capital for the simulation.",
)
@click.option(
    "-b",
    "--bundle",
    default=DEFAULT_BUNDLE,
    metavar="BUNDLE-NAME",
    show_default=True,
    help="The data bundle to use for the simulation.",
)
@click.option(
    "--bundle-timestamp",
    type=Timestamp(),
    default=pd.Timestamp.utcnow(),
    show_default=False,
    help="The date to lookup data on or before.\n" "[default: <current-time>]",
)
@click.option(
    "-bf",
    "--benchmark-file",
    default=None,
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=str),
    help="The csv file that contains the benchmark returns",
)
@click.option(
    "--benchmark-symbol",
    default=None,
    type=click.STRING,
    help="The symbol of the instrument to be used as a benchmark "
         "(should exist in the ingested bundle)",
)
@click.option(
    "--benchmark-sid",
    default=None,
    type=int,
    help="The sid of the instrument to be used as a benchmark "
         "(should exist in the ingested bundle)",
)
@click.option(
    "--no-benchmark",
    is_flag=True,
    default=False,
    help="If passed, use a benchmark of zero returns.",
)
@click.option(
    "-s",
    "--start",
    type=Date(as_timestamp=True),
    help="The start date of the simulation.",
)
@click.option(
    "-e",
    "--end",
    type=Date(as_timestamp=True),
    help="The end date of the simulation.",
)
@click.option(
    "-o",
    "--output",
    default="-",
    metavar="FILENAME",
    show_default=True,
    help="The location to write the perf data. If this is '-' the perf will"
         " be written to stdout.",
)
@click.option(
    "--trading-calendar",
    metavar="TRADING-CALENDAR",
    default="NYSE",
    help="The calendar you want to use e.g. XLON. XNYS is the default.",
)
@click.option(
    "--print-algo/--no-print-algo",
    is_flag=True,
    default=False,
    help="Print the algorithm to stdout.",
)
@click.option(
    "--metrics-set",
    default="default",
    help="The metrics set to use. New metrics sets may be registered in your"
         " extension.py.",
)
@click.option(
    "--blotter",
    default="default",
    help="The blotter to use.",
    show_default=True,
)
@click.option(
    "--broker",
    default=None,
    help="The broker to use for live trading.",
    show_default=True,
)
@click.option(
    "--live-market-data-provider",
    default="lime-trader-sdk",
    help="Market data provider for live trading",
    show_default=True,
)
@ipython_only(
    click.option(
        "--local-namespace/--no-local-namespace",
        is_flag=True,
        default=None,
        help="Should the algorithm methods be " "resolved in the local namespace.",
    )
)
@click.pass_context
def run(
        ctx,
        algofile,
        algotext,
        define,
        data_frequency,
        capital_base,
        bundle,
        bundle_timestamp,
        benchmark_file,
        benchmark_symbol,
        benchmark_sid,
        no_benchmark,
        start,
        end,
        output,
        trading_calendar,
        print_algo,
        metrics_set,
        local_namespace,
        blotter,
        broker: str | None,
        live_market_data_provider: str | None,
):
    """Run a backtest for the given algorithm."""
    # check that the start and end dates are passed correctly
    if start is None and end is None:
        # check both at the same time to avoid the case where a user
        # does not pass either of these and then passes the first only
        # to be told they need to pass the second argument also
        ctx.fail(
            "must specify dates with '-s' / '--start' and '-e' / '--end'",
        )
    if start is None:
        ctx.fail("must specify a start date with '-s' / '--start'")
    if end is None:
        ctx.fail("must specify an end date with '-e' / '--end'")

    if (algotext is not None) == (algofile is not None):
        ctx.fail(
            "must specify exactly one of '-f' / "
            "'--algofile' or"
            " '-t' / '--algotext'",
        )

    trading_calendar = get_calendar(trading_calendar)

    benchmark_spec = BenchmarkSpec.from_cli_params(
        no_benchmark=no_benchmark,
        benchmark_sid=benchmark_sid,
        benchmark_symbol=benchmark_symbol,
        benchmark_file=benchmark_file,
    )
    if broker is not None:
        start = pd.Timestamp.now(tz=datetime.timezone.utc).replace(tzinfo=None)# - pd.Timedelta(days=3)
        end = start + pd.Timedelta('5 day')
    return _run(
        initialize=None,
        handle_data=None,
        before_trading_start=None,
        analyze=None,
        algofile=algofile,
        algotext=algotext,
        defines=define,
        data_frequency=data_frequency,
        capital_base=capital_base,
        bundle=bundle,
        bundle_timestamp=bundle_timestamp,
        start=start,
        end=end,
        output=output,
        trading_calendar=trading_calendar,
        print_algo=print_algo,
        metrics_set=metrics_set,
        local_namespace=local_namespace,
        environ=os.environ,
        blotter=blotter,
        benchmark_spec=benchmark_spec,
        custom_loader=None,
        broker=get_broker(broker) if broker is not None else None,
        market_data_provider=get_live_market_data_provider(
            live_market_data_provider) if live_market_data_provider is not None else None,
    )


if __name__ == "__main__":
    main()
