import polars as pl


class AssetDataSource:

    def __init__(self):
        pass

    async def get_assets(self, **kwargs) -> pl.DataFrame:
        pass

    async def get_constituents(self, index: str) -> pl.DataFrame: ...

    async def search_assets(self, query: str, **kwargs) -> pl.DataFrame: ...
