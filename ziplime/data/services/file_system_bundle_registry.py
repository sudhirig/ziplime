import datetime
import os
from pathlib import Path
from typing import Any

import aiofiles.os
import orjson
import structlog

from ziplime.data.domain.bundle_data import BundleData
from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_storage import BundleStorage


class FileSystemBundleRegistry(BundleRegistry):

    def __init__(self, base_data_path: str):
        super().__init__()
        self._base_data_path = base_data_path
        self._logger = structlog.get_logger(__name__)
        os.makedirs(self._base_data_path, exist_ok=True)

    async def get_bundle_metadata(self, bundle_data: BundleData, bundle_storage: BundleStorage) -> dict[str, Any]:
        return {
            "name": bundle_data.name,
            "version": bundle_data.version,

            "bundle_storage_class": f"{bundle_storage.__class__.__module__}.{bundle_storage.__class__.__name__}",
            "bundle_storage_data": await bundle_storage.to_json(bundle_data=bundle_data),

            "asset_repository_class": f"{bundle_data.asset_repository.__class__.__module__}.{bundle_data.asset_repository.__class__.__name__}",
            "asset_repository_data": bundle_data.asset_repository.to_json(),

            "adjustment_repository_class": f"{bundle_data.adjustment_repository.__class__.__module__}.{bundle_data.adjustment_repository.__class__.__name__}",
            "adjustment_repository_data": bundle_data.adjustment_repository.to_json(),

            "start_date": bundle_data.start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_date": bundle_data.end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "trading_calendar_name": bundle_data.trading_calendar.name,
            "frequency_seconds": bundle_data.frequency.total_seconds(),
            "timestamp": bundle_data.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        }

    async def load_bundle_metadata(self, bundle_name: str, bundle_version: str | None) -> dict[str, Any] | None:
        if bundle_version is None:
            bundles = await self.list_bundles_by_name(bundle_name=bundle_name)
            if not bundles:
                return None
            bundle_version = bundles[0]["version"]

        bundle_metadata_path = Path(self.get_bundle_registry_path(), f"{bundle_name}_{bundle_version}.json")
        async with aiofiles.open(bundle_metadata_path, mode="rb") as f:
            return orjson.loads(await f.read())

    def get_bundle_registry_path(self) -> Path:
        return Path(self._base_data_path, "bundle_registry")

    async def persist_metadata(self, bundle_data: BundleData, metadata: dict[str, Any]):
        bundle_metadata_path = Path(self.get_bundle_registry_path(), f"{bundle_data.name}_{bundle_data.version}.json")
        await aiofiles.os.makedirs(bundle_metadata_path.parent, exist_ok=True)
        async with aiofiles.open(bundle_metadata_path, mode="wb") as f:
            await f.write(orjson.dumps(metadata, option=orjson.OPT_INDENT_2))

    async def delete_bundle(self):
        pass

    async def list_bundles(self) -> list[dict[str, Any]]:
        registry_items = []
        for file in await aiofiles.os.scandir(self.get_bundle_registry_path()):
            if file.is_file():
                if not file.name.endswith(".json"):
                    continue
                registry_items.append(file.path)
        bundles = []
        for item in registry_items:
            async with aiofiles.open(item, mode="r") as f:
                bundle_metadata = orjson.loads(await f.read())
                bundles.append(bundle_metadata)
        return bundles

    async def list_bundles_by_name(self, bundle_name: str) -> list[dict[str, Any]]:
        bundles = await self.list_bundles()
        return sorted(list(filter(lambda b: b["name"] == bundle_name, bundles)), key=lambda b: b["timestamp"], reverse=True)
