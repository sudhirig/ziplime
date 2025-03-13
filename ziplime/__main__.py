import datetime
import errno
import logging
import os

import pandas as pd
from zipline.__main__ import ipython_only

from zipline.utils.calendar_utils import get_calendar

from ziplime.constants.default_columns import OHLCV_COLUMNS_POLARS, DEFAULT_COLUMNS_POLARS
from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.data.storages.bcolz_data_bundle import BcolzDataBundle
from ziplime.data.storages.polars_data_bundle import PolarsDataBundle
from ziplime.domain.data_frequency import DataFrequency
from ziplime.utils.run_algo import _run, BenchmarkSpec

import click
from ziplime.data import bundles as bundles_module

from click import DateTime
from lime_trader.models.market import Period
from zipline.utils.cli import Timestamp

from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle

from zipline import __main__ as zipline__main__

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
@click.pass_context
def main(ctx):
    """Top level ziplime entry point."""
    # install a logging handler before performing any other operations

    logging.basicConfig(
        format="[%(asctime)s-%(levelname)s][%(name)s]\n %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    register_default_bundles()


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
    type=click.Choice(['limex-hub','lime-trader-sdk']),
    default="limex-hub",
    help="Market data provider for historical data",
    show_default=True,
)
@click.option(
    "--fundamental-data-provider",
    type=click.Choice(['limex-hub',]),
    default="limex-hub",
    help="Fundamental data provider",
    show_default=True,
)
@click.option(
    "--skip-fundamental-data",
    default=False,
    is_flag=True,
    help="If passed, fundamental data won't be ingested.",
)
@click.pass_context
def ingest(ctx, bundle, new_bundle_name, start_date, end_date, period, symbols, fundamental_data, show_progress,
           assets_version, calendar,
           historical_market_data_provider,
           fundamental_data_provider,
           skip_fundamental_data
           ):
    """Top level ziplime entry point."""
    symbols_parsed = symbols.split(',') if symbols else None
    if skip_fundamental_data:
        fundamental_data_list = set()
    else:
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
        ],
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
        else DEFAULT_COLUMNS_POLARS
    )
    bundles_module.ingest(
        name=bundle_name,
        environ=os.environ,
        timestamp=pd.Timestamp.utcnow(),
        assets_version=assets_version,
        show_progress=show_progress,
        historical_market_data_provider=get_historical_market_data_provider(code=historical_market_data_provider),
        fundamental_data_provider=fundamental_data_provider_instance,
        data_bundle_writer_class=PolarsDataBundle,
        fundamental_data_writer_class=BcolzDataBundle,
        market_data_fields=OHLCV_COLUMNS_POLARS,
        fundamental_data_fields=fundamental_data_cols,
        period=Period(period)
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
    for bundle in sorted(bundles_module.bundles.keys()):
        if bundle.startswith("."):
            # hide the test data
            continue
        try:
            ingestions = list(map(str, bundles_module.ingestions_for_bundle(bundle)))
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
            ingestions = []

        # If we got no ingestions, either because the directory didn't exist or
        # because there were no entries, print a single message indicating that
        # no ingestions have yet been made.
        for timestamp in ingestions or ["<no ingestions>"]:
            click.echo("%s %s" % (bundle, timestamp))


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
    "--data-frequency",
    type=click.Choice({"daily", "minute"}),
    default="daily",
    show_default=True,
    help="The data frequency of the simulation.",
)
@click.option(
    "--capital-base",
    type=float,
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
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="The start date of the simulation.",
)
@click.option(
    "-e",
    "--end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
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
    "--broker",
    default=None,
    type=click.Choice(['lime-trader-sdk']),
    help="The broker to use for live trading.",
    show_default=True,
)
@click.option(
    "--live-market-data-provider",
    type=click.Choice(['lime-trader-sdk']),
    default=None,
    help="Market data provider for live trading",
    # show_default=True,
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
        broker: str | None,
        live_market_data_provider: str | None,
):
    """Run a backtest for the given algorithm."""
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
        data_frequency=DataFrequency.MINUTE if data_frequency == "minute" else DataFrequency.DAY,
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
        benchmark_spec=benchmark_spec,
        custom_loader=None,
        broker=get_broker(broker) if broker is not None else None,
        market_data_provider=get_live_market_data_provider(
            live_market_data_provider) if live_market_data_provider is not None else None,
    )


if __name__ == "__main__":
    main()
