import datetime

import polars as pl
from abc import abstractmethod
from typing import Any, Self

from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle


class BundleStorage:

    def __init__(self):
        pass

    @abstractmethod
    async def store_bundle(self, data_bundle: DataBundle): ...

    @abstractmethod
    async def load_data_bundle(self, data_bundle: DataBundle,
                               symbols: list[str] | None = None,
                               start_date: datetime.datetime | None = None,
                               end_date: datetime.datetime | None = None,
                               frequency: datetime.timedelta | Period | None = None,
                               start_auction_delta: datetime.timedelta = None,
                               end_auction_delta: datetime.timedelta = None,
                               aggregations: list[pl.Expr] = None,
                               ) -> pl.DataFrame: ...

    @classmethod
    @abstractmethod
    async def from_json(cls, data: dict[str, Any]) -> Self: ...

    @abstractmethod
    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]: ...
