import datetime
from dataclasses import dataclass

from ziplime.assets.domain.db.asset import Asset
from ziplime.domain.position import Position


@dataclass
class Portfolio:
    capital_used: float
    starting_cash: float
    portfolio_value: float
    pnl: float
    returns: float
    cash: float
    positions: dict[Asset, Position]
    positions_value: float
    start_date: datetime.datetime | None = None
