import logging

from click import DateTime

from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle

import click
from zipline import __main__ as zipline__main__

DEFAULT_BUNDLE = "ziplime"


@click.group()
@click.pass_context
def main(ctx):
    """Top level ziplime entry point."""

    logging.basicConfig(level=logging.INFO)
    # install a logging handler before performing any other operations
    pass


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
)
@click.option(
    "--end-date",
    type=DateTime(),
)
@click.option("-s", '--symbols')
@click.pass_context
def ingest(ctx, bundle, new_bundle_name, start_date, end_date, symbols):
    """Top level zipline entry point."""
    symbols_parsed = symbols.split(',') if symbols else None

    if new_bundle_name:
        bundle_name = new_bundle_name
        ctx.args = ['-b', new_bundle_name] + ctx.args
    else:
        bundle_name = bundle
        ctx.args = ['-b', bundle] + ctx.args
    # install a logging handler before performing any other operations
    register_lime_symbol_list_equities_bundle(
        bundle_name=bundle_name,
        symbols=symbols_parsed,
        start_session=start_date,
        end_session=end_date
    )

    new_params = dict(**ctx.params)

    # clean up lime only params and set new bundle name
    new_params["bundle"] = bundle_name
    del new_params["start_date"]
    del new_params["end_date"]
    del new_params["symbols"]
    del new_params["new_bundle_name"]

    ctx.params = new_params
    func = getattr(zipline__main__, "ingest")
    ctx.forward(func)


#
#
# @main.command()
# @click.option(
#     "-b",
#     "--bundle",
#     default=DEFAULT_BUNDLE,
#     metavar="BUNDLE-NAME",
#     show_default=True,
#     help="The data bundle to ingest.",
# )
# @click.option(
#     "--assets-version",
#     type=int,
#     multiple=True,
#     help="Version of the assets db to which to downgrade.",
# )
# @click.option(
#     "--show-progress/--no-show-progress",
#     default=True,
#     help="Print progress information to the terminal.",
# )
# def ingest(bundle, assets_version, show_progress):
#     """Ingest the data for the given bundle."""
#     bundles_module.ingest(
#         bundle,
#         os.environ,
#         pd.Timestamp.utcnow(),
#         assets_version,
#         show_progress,
#     )
#
#
# @main.command()
# @click.option(
#     "-b",
#     "--bundle",
#     default=DEFAULT_BUNDLE,
#     metavar="BUNDLE-NAME",
#     show_default=True,
#     help="The data bundle to clean.",
# )
# @click.option(
#     "-e",
#     "--before",
#     type=Timestamp(),
#     help="Clear all data before TIMESTAMP."
#     " This may not be passed with -k / --keep-last",
# )
# @click.option(
#     "-a",
#     "--after",
#     type=Timestamp(),
#     help="Clear all data after TIMESTAMP"
#     " This may not be passed with -k / --keep-last",
# )
# @click.option(
#     "-k",
#     "--keep-last",
#     type=int,
#     metavar="N",
#     help="Clear all but the last N downloads."
#     " This may not be passed with -e / --before or -a / --after",
# )
# def clean(bundle, before, after, keep_last):
#     """Clean up data downloaded with the ingest command."""
#     bundles_module.clean(
#         bundle,
#         before,
#         after,
#         keep_last,
#     )
#
#
# @main.command()
# def bundles():
#     """List all of the available data bundles."""
#     for bundle in sorted(bundles_module.bundles.keys()):
#         if bundle.startswith("."):
#             # hide the test data
#             continue
#         try:
#             ingestions = list(map(str, bundles_module.ingestions_for_bundle(bundle)))
#         except OSError as e:
#             if e.errno != errno.ENOENT:
#                 raise
#             ingestions = []
#
#         # If we got no ingestions, either because the directory didn't exist or
#         # because there were no entries, print a single message indicating that
#         # no ingestions have yet been made.
#         for timestamp in ingestions or ["<no ingestions>"]:
#             click.echo("%s %s" % (bundle, timestamp))


# def run_strategy():
#     register_lime_equities_bundle(
#         bundle_name="lime_bundle_test",
#         start_session=datetime.datetime(year=2024, day=1, month=1),
#         end_session=datetime.datetime(year=2024, day=1, month=2),
#         symbol_list=["AAPL"],
#     )


# if __name__ == '__main__':
#     run_strategy()

if __name__ == "__main__":
    main()
