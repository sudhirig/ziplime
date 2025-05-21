import datetime
from dataclasses import dataclass
from decimal import Decimal

from ziplime.assets.entities.asset import Asset


@dataclass
class Position:
    asset: Asset
    amount: int
    cost_basis: Decimal  # per share
    last_sale_price: Decimal
    last_sale_date: datetime.datetime | None = None
