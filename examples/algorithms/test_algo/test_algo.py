import datetime
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
    context.aapl = await context.symbol('AAPL')
    context.amzn = await context.symbol('AMZN')

async def handle_data(context: TradingAlgorithm, data: BarData):
    close_price_history = data.history(assets=[context.aapl],fields=['close'],bar_count=10,frequency=datetime.timedelta(minutes=1))
    close_price_current = data.current(assets=[context.aapl],fields=['close'])

    print(close_price_history)
    print(close_price_current)

    submitted_order = await context.order_target_percent(asset=context.aapl, target=Decimal(0.5), style=MarketOrder())
    submitted_order = await context.order_target_percent(asset=context.amzn, target=Decimal(0.5), style=MarketOrder())

