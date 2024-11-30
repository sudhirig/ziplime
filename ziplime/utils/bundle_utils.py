import datetime
import os

from lime_trader.models.market import Period
from zipline.utils.paths import data_root

from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle
from ziplime.constants.bundles import DEFAULT_BUNDLE
from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMN_NAMES
from ziplime.data.bundles.core import BundleData


def register_default_bundles(calendar_name: str = "NYSE",
                             fundamental_data_list: list[str] = FUNDAMENTAL_DATA_COLUMN_NAMES):
    data_path = data_root()
    if not next(os.walk(data_path), None):
        lime_bundle_names = []
    else:
        lime_bundle_names = [x for x in [x for x in next(os.walk(data_path), [])][1] if x.startswith(DEFAULT_BUNDLE)]
    for bundle in lime_bundle_names:
        register_lime_symbol_list_equities_bundle(
            bundle_name=bundle,
            symbols=[],
            start_session=None,#datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0),
            end_session=None,#datetime.datetime.now().replace(minute=0, hour=0, second=0, microsecond=0),
            period=Period("day"),
            calendar_name=calendar_name,
            fundamental_data_list=fundamental_data_list
        )


def get_fundamental_data_for_period(bundle: BundleData):
    pass
