import datetime
import logging
from decimal import Decimal

import numpy as np
import talib

from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm


async def initialize(context: TradingAlgorithm):
    # Set the asset to AAPL
    context.asset = await context.symbol('AAPL')
    # Set the count for history data
    context.count = 20


async def handle_data(context: TradingAlgorithm, data: BarData):
    try:
        print(f"Handle data: {context.simulation_dt}")
        # Get the current price
        current_price = data.current(assets=[context.asset], fields=['close'])

        # Get the price history
        history = data.history(assets=[context.asset], fields=['close'], bar_count=context.count,
                               frequency=datetime.timedelta(days=1))

        # Calculate the moving average
        ma = history['close'].mean()
        # Calculate the upper and lower Bollinger Bands
        upper_bb, middle_bb, lower_bb = talib.BBANDS(history['close'], timeperiod=context.count - 1, nbdevup=2, nbdevdn=2,
                                                     matype=0)

        # Check if the current price is above the upper Bollinger Band
        if not np.isnan(upper_bb[-1]) and current_price['close'][0] > upper_bb[-1]:
            # Buy the asset
            await context.order_target_percent(asset=context.asset, target=Decimal('1'), style=MarketOrder())
        # Check if the current price is below the moving average
        elif current_price['close'][0] < ma:
            # Close the position
            await context.order_target_percent(asset=context.asset, target=Decimal('0'), style=MarketOrder())
    except Exception as e:
        logging.exception(f"Exception running algorithm for {context.simulation_dt}")