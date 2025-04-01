from abc import abstractmethod
from typing import Any

from ziplime.data.domain.bundle_data import BundleData
from ziplime.data.services.bundle_storage import BundleStorage


class BundleRegistry:
    def __init__(self):
        pass

    async def list_bundles(self) -> list[dict[str, Any]]: ...

    async def list_bundles_by_name(self, bundle_name: str) -> list[dict[str, Any]]: ...

    async def load_bundle_metadata(self, bundle_name: str, bundle_version: str | None) -> dict[str, Any] | None: ...

    async def delete_bundle(self): ...

    @abstractmethod
    async def persist_metadata(self, bundle_data: BundleData, metadata: dict[str, Any]): ...

    @abstractmethod
    async def get_bundle_metadata(self, bundle_data: BundleData, bundle_storage: BundleStorage) -> dict[str, Any]: ...

    async def register_bundle(self, bundle_data: BundleData, bundle_storage: BundleStorage):
        metadata = await self.get_bundle_metadata(bundle_data=bundle_data, bundle_storage=bundle_storage)
        await self.persist_metadata(bundle_data=bundle_data, metadata=metadata)
