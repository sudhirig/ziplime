from decimal import Decimal

from pydantic import BaseModel

from ziplime.assets.domain.asset_type import AssetType
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
    config: TestAlgoConfig = context.algorithm.config

    context.assets = [
        await context.symbol(equity.symbol, asset_type=AssetType.EQUITY)
        for equity in config.equities_to_trade
    ]
    context.counter = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    context.counter += 1
    print(f"Handle data for: {context.get_datetime()}")

    for asset in context.assets:
        submitted_order = await context.order(asset=asset, amount=1, style=MarketOrder())
        submitted_order = await context.order(asset=asset, amount=1, style=MarketOrder())
        # if context.counter > 1:
        #     close_price_history = data.history(
        #         assets=context.assets,
        #         fields=['close'],
        #         bar_count=10,
        #         frequency=datetime.timedelta(minutes=1)
        #     )
        #     print(close_price_history.head())
        #
        #     # asset_series = long_mavg_df.filter(long_mavg_df["sid"] == asset.sid)["close"]
        #     # print(asset_series.head(1))
        #     current_close_price = data.current(
        #         assets=context.assets,
        #         fields=['close']
        #     )
        #     print(current_close_price.head())
        #
        #     # Submit an order limit/market
        #     # submitted_order = await context.order(asset=asset, amount=10, style=LimitOrder(limit_price=1))
        #     submitted_order = await context.order_target_percent(asset=asset, target=0.1, style=MarketOrder())
        #     if submitted_order:
        #         print(f"Submitted order: {submitted_order}")
        #     else:
        #         print("New order not submitted.")
        #     print(context.portfolio)
        #     # print(asset_series)
