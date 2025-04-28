import datetime
from dataclasses import dataclass
from decimal import Decimal

from ziplime.assets.models.asset_model import AssetModel


@dataclass
class Position:
    asset: AssetModel
    amount: int
    cost_basis: Decimal  # per share
    last_sale_price: Decimal
    last_sale_date: datetime.datetime | None = None
