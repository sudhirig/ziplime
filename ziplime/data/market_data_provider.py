import datetime
from abc import abstractmethod
import polars as pl

from ziplime.assets.models.asset_model import AssetModel


class MarketDataProvider:

    @abstractmethod
    async def get_data_by_date(self, fields: list[str],
                               from_date: datetime.datetime,
                               to_date: datetime.datetime,
                               frequency: datetime.timedelta,
                               assets: list[AssetModel],
                               include_bounds: bool,
                               ) -> pl.DataFrame:
        pass

    @abstractmethod
    async def get_data_by_limit(self, fields: list[str],
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta,
                                assets: list[AssetModel],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        pass

    @abstractmethod
    async def get_scalar_asset_spot_value(self, asset: AssetModel, field: str, dt: datetime.datetime,
                                          frequency: datetime.timedelta):
        pass

    @abstractmethod
    async def _get_single_asset_value(self, asset: AssetModel, field: str, dt: datetime.datetime,
                                      frequency: datetime.timedelta) -> pl.DataFrame:
        pass

    @abstractmethod
    async def get_spot_value(self, assets: list[AssetModel], fields: list[str], dt: datetime.datetime,
                             frequency: datetime.timedelta):
        pass

    # @abstractmethod
    # def get_adjusted_value(
    #         self, asset: Asset, field: str, dt: datetime.datetime, perspective_dt: datetime.datetime,
    #         data_frequency: datetime.timedelta,
    #         spot_value: float = None
    # ):
    #     pass

    # @abstractmethod
    # def _get_adjustment_list(self, asset: Asset, adjustments_dict: dict[str, Any], table_name: str):
    #     pass
    #
    # @abstractmethod
    # def get_splits(self, assets: list[Asset], dt: datetime.date):
    #     pass
    #
    # @abstractmethod
    # def get_stock_dividends(self, sid: int, trading_days: pd.DatetimeIndex):
    #     pass
    #
    # @abstractmethod
    # def get_current_future_chain(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
    #     pass
    #
    # @abstractmethod
    # def _get_current_contract(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
    #     pass
    #
    # @abstractmethod
    # def get_adjustments(self, assets: list[Asset], field: str, dt: datetime.datetime,
    #                     perspective_dt: datetime.datetime):
    #     pass
