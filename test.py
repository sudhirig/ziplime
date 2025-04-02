import datetime

from ziplime.algorithm import TradingAlgorithm
from ziplime.domain.bar_data import BarData



async def initialize(context):
    context.assets = [await context.symbol('AAPL')]
    context.assett = await context.symbol("AMZN")  # Apple and Amazon
    context.counter = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    context.counter += 1
    print(f"Handle data for: {context.datetime}")

    for asset in context.assets:
        if context.counter > 10:
            long_mavg_df = data.history(
                assets=[asset, context.assett],
                fields=['close'],
                bar_count=10,
                frequency=datetime.timedelta(minutes=3)
            )
            asset_series = long_mavg_df.filter(long_mavg_df["sid"] == asset.sid)["close"]
            print(asset_series.head(1))
            context.order(asset=asset, amount=10)
            # print(asset_series)
#
# start_session = pd.Timestamp('2025-02-14')
#
# end_session = pd.Timestamp('2025-02-14')
#
# param_capital = 100000
#
# param_frequency = 'minute'
#
# param_bundle = 'limex_us_equity_min'
#
# result = run_algorithm(start=start_session,
#                        end=end_session,
#                        initialize=initialize,
#                        handle_data=handle_data,
#                        capital_base=param_capital,
#                        data_frequency=param_frequency,
#                        bundle=param_bundle)
#
# # report stats
#
# result_returns = result.returns
#
# print(result)
