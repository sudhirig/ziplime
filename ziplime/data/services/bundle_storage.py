import polars as pl
from abc import abstractmethod
from typing import Any, Self

from ziplime.data.domain.data_bundle import DataBundle


class BundleStorage:

    def __init__(self):
        pass

    @abstractmethod
    async def store_bundle(self, data_bundle: DataBundle): ...

    @abstractmethod
    async def load_data_bundle(self, data_bundle: DataBundle) -> pl.DataFrame: ...

    @classmethod
    @abstractmethod
    async def from_json(cls, data: dict[str, Any]) -> Self: ...

    @abstractmethod
    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]: ...
