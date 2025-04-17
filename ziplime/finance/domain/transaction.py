import datetime


from ziplime.assets.domain.db.asset import Asset
from ziplime.protocol import DataSourceType


class Transaction:

    def __init__(self, asset: Asset, amount: int, dt: datetime.datetime, price: float, order_id: str,
                 commission: float | None = None):
        self.asset = asset
        self.amount = amount
        # if amount < 1:
        #     raise Exception("Transaction magnitude must be at least 1.")

        self.dt = dt
        self.price = price
        self.order_id = order_id
        self.type = DataSourceType.TRANSACTION
        self.commission = commission

    def to_dict(self):
        return {
            "type": self.type,
            "amount": self.amount,
            "dt": self.dt,
            "price": self.price,
            "order_id": self.order_id,
            "commission": self.commission,
            "asset": self.asset,
        }
