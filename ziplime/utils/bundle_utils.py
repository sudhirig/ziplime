import os

from ziplime.data.services.lime_trader_sdk_data_source import LimeTraderSdkDataSource
from ziplime.exchanges.lime_trader_sdk.lime_trader_sdk_exchange import LimeTraderSdkExchange
from ziplime.data.data_sources.limex_hub_fundamental_data_source import LimexHubFundamentalDataSource


def get_data_source(code: str):
    if code == "lime-trader-sdk":
        lime_trader_sdk_credentials = os.environ.get("LIME_SDK_CREDENTIALS_FILE", None)
        if lime_trader_sdk_credentials is None:
            raise ValueError("Missing LIME_SDK_CREDENTIALS_FILE environment variable.")
        return LimeTraderSdkDataSource(lime_sdk_credentials_file=lime_trader_sdk_credentials)
    raise Exception("Unsupported live market data provider!")

def get_fundamental_data_provider(code: str):
    if code == "limex-hub":
        limex_hub_key = os.environ.get("LIMEX_API_KEY", None)
        maximum_threads = os.environ.get("LIMEX_HUB_MAXIMUM_THREADS", None)
        if limex_hub_key is None:
            raise ValueError("Missing LIMEX_API_KEY environment variable.")
        return LimexHubFundamentalDataSource.from_env()
    raise Exception("Unsupported fundamental data provider!")

def get_exchange(code: str):
    if code == "lime-trader-sdk":
        lime_trader_sdk_credentials = os.environ.get("LIME_SDK_CREDENTIALS_FILE", None)
        if lime_trader_sdk_credentials is None:
            raise ValueError("Missing LIME_SDK_CREDENTIALS_FILE environment variable.")
        return LimeTraderSdkExchange(lime_sdk_credentials_file=lime_trader_sdk_credentials)
    raise Exception("Unsupported live market data provider!")
