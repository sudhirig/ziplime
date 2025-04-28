import datetime
from decimal import Decimal

from ziplime.assets.models.asset_model import AssetModel
from ziplime.protocol import DataSourceType


class Transaction:

    def __init__(self, asset: AssetModel, amount: int, dt: datetime.datetime, price: Decimal, order_id: str,
                 commission: Decimal | None = None):
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
