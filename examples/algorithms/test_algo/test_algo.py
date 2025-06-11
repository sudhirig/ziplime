import datetime
import logging
from decimal import Decimal

import numpy as np
import talib
import polars as pl
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm



async def initialize(context):
    context.aapl = await context.symbol("AAPL")
    context.amzn = await context.symbol("AMZN")

async def handle_data(context, data):
    print(f"simulating {context.simulation_dt}")
    window = 50
    df_aapl = data.history(assets=[context.aapl], fields=["close"], bar_count=window)
    df_amzn = data.history(assets=[context.amzn], fields=["close"], bar_count=window)

    aapl_close = df_aapl["close"].cast(pl.Float64).to_numpy()
    amzn_close = df_amzn["close"].cast(pl.Float64).to_numpy()

    if len(aapl_close) > 30 and len(amzn_close) > 30:
        ratio = aapl_close / amzn_close

        ma = talib.SMA(ratio, timeperiod=30)
        stddev = talib.STDDEV(ratio, timeperiod=20)

        if len(ma) > 0 and len(stddev) > 0:
            current_ratio = ratio[-1]
            ma_last = ma[-1]
            stddev_last = stddev[-1]

            threshold = stddev_last

            if current_ratio > ma_last + threshold:
                await context.order_target_percent(asset=context.aapl, target=Decimal('-0.5'), style=MarketOrder())
                await context.order_target_percent(asset=context.amzn, target=Decimal('0.5'), style=MarketOrder())
            elif current_ratio < ma_last - threshold:
                await context.order_target_percent(asset=context.aapl, target=Decimal('0.5'), style=MarketOrder())
                await context.order_target_percent(asset=context.amzn, target=Decimal('-0.5'), style=MarketOrder())

