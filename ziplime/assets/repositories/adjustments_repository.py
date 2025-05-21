import datetime
import polars as pl
from typing import Any, Self

from ziplime.assets.entities.asset import Asset
from ziplime.assets.models.dividend import Dividend


class AdjustmentRepository:
    def get_splits(self, assets: frozenset[Asset], dt: datetime.date): ...

    def get_stock_dividends(self, sid: int, trading_days: pl.Series) -> list[Dividend]: ...

    def to_json(self): ...

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self: ...
