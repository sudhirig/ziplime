import datetime
from typing import Any, Self

from ziplime.assets.entities.asset import Asset


class AdjustmentRepository:
    def get_splits(self, assets: list[Asset], dt: datetime.date):...

    def to_json(self): ...

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self: ...
