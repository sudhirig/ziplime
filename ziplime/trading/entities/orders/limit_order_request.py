from decimal import Decimal

from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.enums.order_side import OrderSide
from ziplime.trading.enums.order_type import OrderType


class LimitOrderRequest(OrderRequest):

    def __init(self, order_id: str, order_side: OrderSide, quantity: Decimal, limit_price: Decimal):
        super().__init__(order_id=order_id, order_type=OrderType.LIMIT)
        self.order_side = order_side
        self.quantity = quantity
        self.limit_price = limit_price
