from dataclasses import dataclass

from ziplime.trading.entities.trading_pair import TradingPair
from ziplime.trading.enums.order_type import OrderType


@dataclass
class OrderRequest:
    order_id: str
    trading_pair: TradingPair
    order_type: OrderType
