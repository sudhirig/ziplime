from zipline._protocol import BarData

from ziplime.constants.fundamental_data import FundamentalData, FundamentalDataValueType
from zipline.api import symbol, order_target, record
from zipline import TradingAlgorithm


def initialize(context: TradingAlgorithm):
    context.asset = symbol('AAPL')
    context.i = 0


def handle_data(context: TradingAlgorithm, data: BarData):
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d").mean()
    long_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d").mean()
    return_on_tangible_equity_mean = data.history(
        context.asset, 'return_on_tangible_equity_value', bar_count=200,
        frequency="1d"
    ).mean()
    # Trading logic
    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.asset, 100)
    elif short_mavg < long_mavg:
        order_target(context.asset, 0)

    # Save values for later inspection
    record(AAPL=data.current(context.asset, 'price'),
           short_mavg=short_mavg,
           long_mavg=long_mavg)
