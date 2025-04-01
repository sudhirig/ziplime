import polars as pl
from abc import abstractmethod
from typing import Any, Self

from ziplime.data.domain.bundle_data import BundleData


class BundleStorage:

    def __init__(self):
        pass

    @abstractmethod
    async def store_bundle(self, bundle_data: BundleData): ...

    @abstractmethod
    async def load_bundle_data(self, bundle_data: BundleData) -> pl.DataFrame: ...

    @classmethod
    @abstractmethod
    async def from_json(cls, data: dict[str, Any]) -> Self: ...

    @abstractmethod
    async def to_json(self, bundle_data: BundleData) -> dict[str, Any]: ...
