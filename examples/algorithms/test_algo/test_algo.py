import datetime
import logging
from decimal import Decimal

import numpy as np
import talib

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm


async def initialize(context):
    pass


async def handle_data(context, data: BarData):
    asset = await context.symbol("NVDA")
    await context.order(asset=asset, amount=1, style=MarketOrder())
    df_aapl = data.history(assets=[asset], fields=["close"], bar_count=10,
                           frequency=datetime.timedelta(minutes=1)
                           )
    df_aapl = data.current(assets=[asset], fields=["close"]
                           )
    print(df_aapl.head(n=10))
    df_aapl = data.history(
        assets=[asset], fields=None, bar_count=10,
        frequency="1mo",
        data_source="limex_us_fundamental_data"
    )
    print(df_aapl.head(n=10))
