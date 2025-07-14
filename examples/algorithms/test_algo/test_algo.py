import datetime
import logging

import numpy as np
import talib

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm


async def initialize(context):
    assets = [
        await context.symbol("AAPL"),
        await context.symbol("NVDA"),
        await context.symbol("AMD"),
        await context.symbol("AMGN")
    ]
    context.assets = assets


async def handle_data(context, data: BarData):
    # asset = await context.symbol("NVDA")
    # asset_spx = await context.symbol("SPX")
    for asset in context.assets:
        await context.order(asset=asset, amount=1, style=MarketOrder())
    # df_aapl = data.history(assets=[asset], fields=["close"], bar_count=10,
    #                        frequency=datetime.timedelta(minutes=1)
    #                        )
    # df_aapl = data.current(assets=[asset], fields=["close"]
    #                        )
    # print(df_aapl.head(n=10))
    # # df_aapl = data.history(
    #     assets=[asset], bar_count=10,
    #     frequency="1mo",
    #     data_source="limex_us_fundamental_data"
    # )

    # df_spx = data.history(
    #     assets=[asset_spx], bar_count=10,
    #     frequency="1mo",
    #     data_source="custom_minute_bars"
    # )
    # print(df_spx.head(n=10))

#
#  async def initialize(context):
#     pass
#
#
# async def handle_data(context, data):
#     asset = await context.symbol("AAPL")
#     df_history = data.history(assets=[asset], fields=["close"], bar_count=20)
#     if len(df_history) >= 20:
#         closes_history = df_history["close"].cast(pl.Float64).to_numpy()
#         upper, middle, lower = talib.BBANDS(closes_history, timeperiod=20, nbdevup=2, nbdevdn=2)
#         sma_values = talib.SMA(closes_history, timeperiod=14)
#         if len(upper) > 0 and len(sma_values) > 0:
#             upper_last = upper[-1]
#             sma_last = sma_values[-1]
#             df_current = data.current(assets=[asset], fields=["close"])
#             if len(df_current) > 0:
#                 current_close = df_current["close"][0]
#                 if current_close > upper_last:
#                     await context.order_target_percent(asset=asset, target=0.01, style=MarketOrder())
#                 elif current_close < sma_last and context.portfolio.positions.get(asset, None) and context.portfolio.positions[asset].amount > 0:
#                     await context.order_target_percent(asset=asset, target=0.0, style=MarketOrder())
