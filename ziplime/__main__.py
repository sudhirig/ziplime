import datetime
import logging
import os
from pathlib import Path

import click
import zipline

from click import DateTime
from lime_trader.models.market import Period
from zipline.utils.cli import Timestamp
from zipline.utils.paths import data_root

from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle

from zipline import __main__ as zipline__main__
from zipline.utils.run_algo import load_extensions
from zipline.extensions import create_args

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

    data_path = data_root()
    lime_bundle_names = [x for x in [x for x in next(os.walk(data_path))][1] if x.startswith(DEFAULT_BUNDLE)]
    for bundle in lime_bundle_names:
        register_lime_symbol_list_equities_bundle(
            bundle_name=bundle,
            symbols=[],
            start_session=datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0),
            end_session=datetime.datetime.utcnow().replace(minute=0, hour=0, second=0, microsecond=0),
            period=Period("day"),
        )

    create_args(x, zipline.extension_args)
    load_extensions(
        default_extension,
        extension,
        strict_extensions,
        os.environ,
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
    help="The data bundle to ingest.",
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
@click.pass_context
def ingest(ctx, bundle, new_bundle_name, start_date, end_date, period, symbols):
    """Top level ziplime entry point."""
    symbols_parsed = symbols.split(',') if symbols else None

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
    )

    new_params = dict(**ctx.params)

    # clean up lime only params and set new bundle name
    new_params["bundle"] = bundle_name
    del new_params["start_date"]
    del new_params["end_date"]
    del new_params["symbols"]
    del new_params["new_bundle_name"]
    del new_params["period"]

    ctx.params = new_params
    func = getattr(zipline__main__, "ingest")
    ctx.forward(func)


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


@main.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.pass_context
def run(ctx):
    """Top level ziplime entry point."""
    new_params = dict(**ctx.params)
    ctx.params = new_params
    func = getattr(zipline__main__, "run")
    ctx.forward(func)


if __name__ == "__main__":
    main()
