import pandas as pd

from ziplime.assets.domain.asset import Asset


class InnerPosition:
    """The real values of a position.

    This exists to be owned by both a
    :class:`zipline.finance.position.Position` and a
    :class:`zipline.protocol.Position` at the same time without a cycle.
    """

    def __init__(self,
                 asset: Asset,
                 amount: int=0,
                 cost_basis: float=0.0,
                 last_sale_price: float=0.0,
                 last_sale_date: pd.Timestamp | None=None):
        self.asset = asset
        self.amount = amount
        self.cost_basis = cost_basis  # per share
        self.last_sale_price = last_sale_price
        self.last_sale_date = last_sale_date

    def __repr__(self):
        return (
                '%s(asset=%r, amount=%r, cost_basis=%r,'
                ' last_sale_price=%r, last_sale_date=%r)' % (
                    type(self).__name__,
                    self.asset,
                    self.amount,
                    self.cost_basis,
                    self.last_sale_price,
                    self.last_sale_date,
                )
        )
