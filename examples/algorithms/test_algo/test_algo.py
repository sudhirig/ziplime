import datetime
from decimal import Decimal

from pydantic import BaseModel

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

from ziplime.trading.trading_algorithm import TradingAlgorithm
from ziplime.domain.bar_data import BarData


class EquityToTrade(BaseModel):
    target_percentage: Decimal
    symbol: str


class TestAlgoConfig(BaseAlgorithmConfig):
    equities_to_trade: list[EquityToTrade]
    currency: str


async def initialize(context: TradingAlgorithm):
    context.i = 0
    context.aapl = await context.symbol('AAPL')
    context.amzn = await context.symbol('AMZN')


async def handle_data(context: TradingAlgorithm, data: BarData):
    print(f"Handle data: {context.simulation_dt}")
    # Skip first 300 days to get full windows
    context.i += 1
    # data.history(assets=[context.aapl], fields=['price'], bar_count=100)[
    #     "price"]
    if context.i < 300:
        return

    short_mavg = \
        data.history(assets=[context.aapl], fields=['price'], bar_count=100, frequency=datetime.timedelta(minutes=1))[
            "price"].mean()
    long_mavg = \
        data.history(assets=[context.aapl], fields=['price'], bar_count=300, frequency=datetime.timedelta(minutes=1))[
            "price"].mean()
    # await context.order_target_percent(asset=context.aapl, target=-Decimal(1), style=MarketOrder())

    if short_mavg > long_mavg:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        await context.order_target_percent(asset=context.aapl, target=Decimal(1), style=MarketOrder())
    elif short_mavg < long_mavg:
        await context.order_target_percent(asset=context.aapl, target=Decimal(0), style=MarketOrder())

    # Save values for later inspection
    context.record(
        AAPL=data.current(assets=[context.aapl], fields=['price']),
        short_mavg=short_mavg,
        long_mavg=long_mavg
    )

    #
    # print(close_price_history)
    # print(close_price_current)
    #
    # submitted_order = await context.order_target_percent(asset=context.aapl, target=Decimal(0.5), style=MarketOrder())
    # submitted_order = await context.order_target_percent(asset=context.amzn, target=Decimal(0.5), style=MarketOrder())
    #
