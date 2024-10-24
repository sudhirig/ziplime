from zipline._protocol import BarData

from ziplime.constants.fundamental_data import FundamentalData, FundamentalDataValueType
from zipline.api import symbol, order_target_percent
from zipline import TradingAlgorithm



def initialize(context: TradingAlgorithm):
    context.assets = [symbol('AAPL'), symbol('AMZN')]
    context.target_percent = 1.0 / len(context.assets)


def handle_data(context: TradingAlgorithm, data: BarData):
    for asset in context.assets:
        total_share_holder_equity_value = context.bundle_data.equity_daily_bar_reader.get_value(
            asset.sid, data.current_session,
            FundamentalData.RETURN_ON_TANGIBLE_EQUITY.property(FundamentalDataValueType.VALUE))
        print(total_share_holder_equity_value)
        order_target_percent(asset, context.target_percent)
