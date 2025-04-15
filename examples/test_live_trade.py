import pandas as pd

from ziplime.domain.benchmark_spec import BenchmarkSpec
# from ziplime.protocol import BarData

benchmark_spec = BenchmarkSpec(benchmark_returns=[],
                                       benchmark_file=None,
                                       benchmark_sid=None,
                                       benchmark_symbol=None,
                                       no_benchmark=False,
                                       )

#
# def initialize(context: TradingAlgorithm):
#     context.asset = symbol('AAPL')
#     context.i = 0
#     # context.set_benchmark(get_benchmark_returns)
#
#
# def handle_data(context: TradingAlgorithm, data: BarData):
#     # Skip first 300 days to get full windows
#     print(f"Running handle_data for date {data.current_session}, current time is {data.current_dt}")
#     #
#     order_target_percent(context.asset, target=0.78, )
#     print(context.portfolio)
#     blotter: BlotterLive = context.blotter
#
#
#     # if hasattr(blotter, "exchange"):
#     #     # Example for fetching portfolio in live trading
#     #     # TODO: make this work with simulation also
#     #     portfolio = blotter.exchange.get_portfolio()
#     #     account = blotter.exchange.get_account()
#     #     positions = blotter.exchange.get_positions()
#     #     print(f"Portfolio: {portfolio}")
#     #     print(f"Positions: {positions}")
#     #     print(f"Account: {account}")
#     context.i += 1
#     # if context.i < 50000:
#     #     return
#     # Compute averages
#     # data.history() has to be called with the same params
#     # from above and returns a pandas dataframe.
#     # short_mavg = data.history([context.asset, symbol('NFLX')], 'price', bar_count=1, frequency="1d").mean()
#     # long_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d").mean()
#     # # long_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1m")
#     # long_mavg = data.history(context.asset, 'price', bar_count=1, frequency="1d")
#     # return_on_tangible_equity_mean = get_fundamental_data(
#     #     bar_data=data, context=context, assets=context.asset,
#     #     fields='pe_ratio_value', bar_count=32,
#     #     frequency="1q", fillna=None
#     # )
#
#     # order_target(context.asset, 100, style=MarketOrder())
#     # order_target_value(context.asset, target=10, style=LimitOrder(limit_price=100))
#
#     # order_target(context.asset, 100)
#
#     # print(return_on_tangible_equity_mean)
#     # # Trading logic
#     # if short_mavg > long_mavg:
#     #     # order_target orders as many shares as needed to
#     #     # achieve the desired number of shares.
#     #     order_target(context.asset, 100)
#     # elif short_mavg < long_mavg:
#     #     order_target(context.asset, 0)
#     #
#     # # Save values for later inspection
#     # record(
#     #     AAPL=data.current(context.asset, 'price'),
#     #     short_mavg=short_mavg,
#     #     long_mavg=long_mavg
#     # )
#
# def get_benchmark_returns(start, end):
#     register_default_bundles()
#     bundle_data = load('lime')
#
#     spx_asset = bundle_data.asset_repository.lookup_symbol('AAPL', as_of_date=None)
#     spx_data = bundle_data.historical_data_reader.load_raw_arrays(
#         columns=['open', 'close'],
#         start_date=start,
#         end_date=end,
#         assets=[spx_asset],
#     )
#     spx_prices = pd.Series(spx_data[1].flatten(), index=pd.to_datetime(spx_data[0].flatten(), unit='s'))
#     spx_returns = spx_prices.pct_change().dropna()
#     return spx_returns
#
# #
# #
# # start_session = pd.Timestamp('2024-06-04')
# # end_session = pd.Timestamp('2024-10-01')
# #
# # benchmark_returns = get_benchmark_returns(start_session, end_session)
# #
# # result = run_algorithm(
# #     start=start_session,
# #     end=end_session,
# #     initialize=initialize,
# #     handle_data=handle_data,
# #     capital_base=100000,
# #     data_frequency='daily',
# #     bundle='lime',
# #     benchmark_returns=None
# # )
