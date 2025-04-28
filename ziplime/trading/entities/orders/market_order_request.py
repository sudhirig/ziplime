from decimal import Decimal

from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType


class MarketOrderRequest(OrderRequest):

    def __init(self, order_id: str, order_side: OrderSide, quantity: Decimal):
        super().__init__(order_id=order_id, order_type=OrderType.MARKET)
        self.order_side = order_side
        self.quantity = quantity
