import datetime
from abc import abstractmethod
import polars as pl

from ziplime.assets.entities.asset import Asset
from ziplime.data.market_data_provider import MarketDataProvider
from ziplime.data.domain.data_bundle import DataBundle


class BundledMarketDataProvider(MarketDataProvider):

    def __init__(self, data_bundle: DataBundle, ):
        self._data_bundle = data_bundle

    async def get_data_by_date(self, fields: list[str],
                               from_date: datetime.datetime,
                               to_date: datetime.datetime,
                               frequency: datetime.timedelta,
                               assets: list[Asset],
                               include_bounds: bool,
                               ) -> pl.DataFrame:
        return await self._data_bundle.get_data_by_date(fields=fields, from_date=from_date, to_date=to_date,
                                                  frequency=frequency,
                                                  assets=assets, include_bounds=include_bounds)

    @abstractmethod
    async def get_data_by_limit(self, fields: list[str],
                                limit: int,
                                end_date: datetime.datetime,
                                frequency: datetime.timedelta,
                                assets: list[Asset],
                                include_end_date: bool,
                                ) -> pl.DataFrame:
        return await self._data_bundle.get_data_by_limit(fields=fields, limit=limit, end_date=end_date, frequency=frequency,
                                                   assets=assets, include_end_date=include_end_date)

    @abstractmethod
    async def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                          frequency: datetime.timedelta):
        pass

    @abstractmethod
    async def _get_single_asset_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                      frequency: datetime.timedelta) -> pl.DataFrame:
        pass

    @abstractmethod
    async def get_spot_value(self, assets: list[Asset], fields: list[str], dt: datetime.datetime,
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
