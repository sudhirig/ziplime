import datetime
from dataclasses import dataclass, field
from decimal import Decimal

from ziplime.assets.entities.asset import Asset
from ziplime.domain.position import Position


@dataclass
class Portfolio:
    # capital_used: float
    cash_flow: Decimal
    starting_cash: Decimal
    portfolio_value: Decimal
    pnl: Decimal
    returns: Decimal
    cash: Decimal
    positions_value: Decimal
    positions_exposure: Decimal
    positions: dict[Asset, Position] = field(default_factory=dict)
    start_date: datetime.datetime | None = None


