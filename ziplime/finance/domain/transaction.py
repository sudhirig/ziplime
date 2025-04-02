import datetime
from copy import copy

from zipline.protocol import DATASOURCE_TYPE

from ziplime.assets.domain.db.asset import Asset


class Transaction:

    def __init__(self, asset: Asset, amount: int, dt: datetime.datetime, price: float, order_id: str,
                 commission: float | None = None):
        self.asset = asset
        self.amount = amount
        if amount < 1:
            raise Exception("Transaction magnitude must be at least 1.")

        self.dt = dt
        self.price = price
        self.order_id = order_id
        self.type = DATASOURCE_TYPE.TRANSACTION
        self.commission = commission

    def __getitem__(self, name):
        return self.__dict__[name]

    def to_dict(self):
        py = copy(self.__dict__)
        del py["type"]
        del py["asset"]

        # Adding 'sid' for backwards compatibility with downstrean consumers.
        py["sid"] = self.asset

        # If you think this looks dumb, that is because it is! We once stored
        # commission here, but haven't for over a year. I don't want to change
        # the perf packet structure yet.
        # py["commission"] = None

        return py

