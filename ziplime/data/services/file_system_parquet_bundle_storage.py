import aiofiles.os
from pathlib import Path
from typing import Any, Sequence, Literal, Self
from polars import CredentialProviderFunction
from polars._typing import ParquetCompression
import polars as pl
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.bundle_storage import BundleStorage


class FileSystemParquetBundleStorage(BundleStorage):

    def __init__(self,
                 base_data_path: str,
                 compression: ParquetCompression = "brotli",
                 compression_level: int | None = None,
                 statistics: bool | str | dict[str, bool] = True,
                 row_group_size: int | None = None,
                 data_page_size: int | None = None,
                 use_pyarrow: bool = False,
                 pyarrow_options: dict[str, Any] | None = None,
                 partition_by: str | Sequence[str] | None = None,
                 partition_chunk_size_bytes: int = 4_294_967_296,
                 storage_options: dict[str, Any] | None = None,
                 credential_provider: (
                         CredentialProviderFunction | Literal["auto"] | None
                 ) = "auto", ):
        super().__init__()
        self.base_data_path = base_data_path
        self.compression = compression
        self.compression_level = compression_level
        self.statistics = statistics
        self.row_group_size = row_group_size
        self.data_page_size = data_page_size
        self.use_pyarrow = use_pyarrow
        self.pyarrow_options = pyarrow_options
        self.partition_by = partition_by
        self.partition_chunk_size_bytes = partition_chunk_size_bytes
        self.storage_options = storage_options

    async def store_bundle(self, data_bundle: DataBundle):
        # we need here to know ehere to store bundle, and info is in bundle metadata
        bundle_path = self.get_data_bundle_path(data_bundle=data_bundle)
        await aiofiles.os.makedirs(bundle_path.parent, exist_ok=True)
        data_bundle.data.write_parquet(bundle_path, compression=self.compression,
                                       compression_level=10,
                                       statistics=self.statistics, row_group_size=self.row_group_size,
                                       data_page_size=self.data_page_size, use_pyarrow=self.use_pyarrow,
                                       pyarrow_options=self.pyarrow_options, partition_by=self.partition_by,
                                       partition_chunk_size_bytes=self.partition_chunk_size_bytes,
                                       storage_options=self.storage_options)

    async def load_data_bundle(self, data_bundle: DataBundle) -> pl.DataFrame:
        bundle_path = self.get_data_bundle_path(data_bundle=data_bundle)
        data = pl.read_parquet(source=bundle_path)
        return data


    @classmethod
    async def from_json(cls, data: dict[str, Any]) -> Self:
        return cls(base_data_path=data["base_data_path"])

    def get_data_bundle_path(self, data_bundle: DataBundle) -> Path:
        return Path(self.base_data_path, "data_bundle", data_bundle.name, data_bundle.version, f"data.parquet")

    async def to_json(self, data_bundle: DataBundle) -> dict[str, Any]:
        return {
            "base_data_path": self.base_data_path,
        }
