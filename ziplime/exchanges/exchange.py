import datetime
from abc import abstractmethod

from exchange_calendars import ExchangeCalendar

from ziplime.assets.models.asset_model import AssetModel
from ziplime.data.domain.data_bundle import DataBundle

from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel


class Exchange:

    def __init__(self, name: str, canonical_name: str, country_code: str,
                 trading_calendar: ExchangeCalendar,
                 data_bundle: DataBundle | None = None):
        self.name = name
        self.canonical_name = canonical_name
        self.country_code = country_code
        self.trading_calendar = trading_calendar
        self.data_bundle = data_bundle

    def get_start_cash_balance(self):
        pass

    def get_current_cash_balance(self):
        pass

    def subscribe_to_market_data(self, asset):
        return []

    def get_subscribed_assets(self):
        return []

    @abstractmethod
    def get_positions(self) -> dict[AssetModel, Position]:
        ...

    @abstractmethod
    def get_portfolio(self) -> Portfolio:
        ...

    @abstractmethod
    def get_account(self) -> Account:
        ...

    @abstractmethod
    def get_time_skew(self):
        ...

    @abstractmethod
    async def submit_order(self, order: Order):
        ...

    def is_alive(self):
        ...

    @abstractmethod
    def get_orders(self) -> dict[str, Order]:
        ...

    @abstractmethod
    async def get_transactions(self, orders: dict[AssetModel, dict[str, Order]]):
        ...

    @abstractmethod
    def get_orders_by_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    def get_transactions_by_order_ids(self, order_ids: list[str]):
        ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        ...

    @abstractmethod
    def get_last_traded_dt(self, asset):
        ...

    @abstractmethod
    def get_spot_value(self, assets, field, dt, data_frequency):
        ...

    @abstractmethod
    def get_realtime_bars(self, assets, frequency):
        ...

    @abstractmethod
    def get_slippage_model(self, asset: AssetModel) -> SlippageModel:
        ...

    @abstractmethod
    def get_commission_model(self, asset: AssetModel) -> CommissionModel:
        ...

    @abstractmethod
    async def get_scalar_asset_spot_value(self, asset: AssetModel, field: str, dt: datetime.datetime,
                                          frequency: datetime.timedelta): ...

    async def get_spot_values(self, assets: list[AssetModel], fields: list[str], exchange_name: str): ...
