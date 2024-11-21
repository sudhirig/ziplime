import pandas as pd
from zipline._protocol import BarData
from zipline.api import symbol, order_target, record
from zipline import TradingAlgorithm

from ziplime.utils.bundle_utils import register_default_bundles
from ziplime.utils.data import get_fundamental_data

from ziplime.data.bundles import load


def initialize(context: TradingAlgorithm):
    context.asset = symbol('AAPL')
    context.i = 0


def handle_data(context: TradingAlgorithm, data: BarData):
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 30:
        return

    # Compute averages
    # data.history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d").mean()
    long_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d").mean()
    return_on_tangible_equity_mean = get_fundamental_data(
        data, context.asset, 'return_on_tangible_equity_value', bar_count=30,
        frequency="1q"
    )

    print(return_on_tangible_equity_mean)
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


def get_benchmark_returns(start, end):
    register_default_bundles()
    bundle_data = load('lime')

    spx_asset = bundle_data.asset_finder.lookup_symbol('AAPL', as_of_date=None)
    spx_data = bundle_data.equity_daily_bar_reader.load_raw_arrays(
        columns=['open', 'close'],
        start_date=start,
        end_date=end,
        assets=[spx_asset],
    )
    spx_prices = pd.Series(spx_data[1].flatten(), index=pd.to_datetime(spx_data[0].flatten(), unit='s'))
    spx_returns = spx_prices.pct_change().dropna()
    return spx_returns

#
# start_session = pd.Timestamp('2024-06-04')
# end_session = pd.Timestamp('2024-10-01')
#
# benchmark_returns = get_benchmark_returns(start_session, end_session)
#
# result = run_algorithm(
#     start=start_session,
#     end=end_session,
#     initialize=initialize,
#     handle_data=handle_data,
#     capital_base=100000,
#     data_frequency='daily',
#     bundle='lime',
#     benchmark_returns=None
# )
