import os

from lime_trader.models.market import Period
from zipline.utils.paths import data_root

from ziplime.gens.brokers.lime_trader_sdk_broker import LimeTraderSdkBroker
from ziplime.config.register_bundles import register_lime_symbol_list_equities_bundle
from ziplime.constants.bundles import DEFAULT_BUNDLE
from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMN_NAMES
from ziplime.data.providers.fundamental_data.limex_hub_fundamental_data_provider import LimexHubFundamentalDataProvider
from ziplime.data.providers.historical_market_data.limex_hub_historical_market_data_provider import \
    LimexHubHistoricalMarketDataProvider
from ziplime.data.providers.live_market_data.lime_trader_sdk_live_market_data_provider import \
    LimeTraderSdkLiveMarketDataProvider


def register_default_bundles(calendar_name: str = "NYSE",
                             fundamental_data_list: set[str] = FUNDAMENTAL_DATA_COLUMN_NAMES):
    data_path = data_root()
    if not next(os.walk(data_path), None):
        lime_bundle_names = []
    else:
        lime_bundle_names = [x for x in [x for x in next(os.walk(data_path), [])][1] if x.startswith(DEFAULT_BUNDLE)]
    for bundle in lime_bundle_names:
        register_lime_symbol_list_equities_bundle(
            bundle_name=bundle,
            symbols=[],
            start_session=None,
            end_session=None,
            period=Period("day"),
            calendar_name=calendar_name,
            fundamental_data_list=fundamental_data_list
        )


def get_historical_market_data_provider(code: str):
    if code == "limex-hub":
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return LimexHubHistoricalMarketDataProvider(limex_api_key=limex_hub_key)
    raise Exception("Unsupported historical market data provider!")


def get_live_market_data_provider(code: str):
    if code == "lime-trader-sdk":
        lime_trader_sdk_credentials = os.environ.get("LIME_SDK_CREDENTIALS_FILE", None)
        if lime_trader_sdk_credentials is None:
            raise ValueError("Missing LIME_SDK_CREDENTIALS_FILE environment variable.")
        return LimeTraderSdkLiveMarketDataProvider(lime_sdk_credentials_file=lime_trader_sdk_credentials)
    raise Exception("Unsupported live market data provider!")


def get_fundamental_data_provider(code: str):
    if code == "limex-hub":
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return LimexHubFundamentalDataProvider(limex_api_key=limex_hub_key)
    raise Exception("Unsupported fundamental data provider!")

def get_broker(code: str):
    if code == "lime-trader-sdk":
        lime_trader_sdk_credentials = os.environ.get("LIME_SDK_CREDENTIALS_FILE", None)
        if lime_trader_sdk_credentials is None:
            raise ValueError("Missing LIME_SDK_CREDENTIALS_FILE environment variable.")
        return LimeTraderSdkBroker(lime_sdk_credentials_file=lime_trader_sdk_credentials)
    raise Exception("Unsupported live market data provider!")
