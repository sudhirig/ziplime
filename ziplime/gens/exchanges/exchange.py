from abc import abstractmethod

from ziplime.assets.domain.db.asset import Asset

from ziplime.domain.bar_data import BarData
from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel


class Exchange:

    def __init__(self, name: str):
        self.name = name

    def subscribe_to_market_data(self, asset):
        return []

    def get_subscribed_assets(self):
        return []

    @abstractmethod
    def get_positions(self) -> dict[Asset, Position]: ...

    @abstractmethod
    def get_portfolio(self) -> Portfolio: ...

    @abstractmethod
    def get_account(self) -> Account: ...

    @abstractmethod
    def get_time_skew(self): ...

    @abstractmethod
    async def submit_order(self, order: Order): ...

    def is_alive(self): ...

    @abstractmethod
    def get_orders(self) -> dict[str, Order]: ...

    @abstractmethod
    async def get_transactions(self, orders: dict[Asset, dict[str, Order]], bar_data: BarData): ...

    @abstractmethod
    def get_orders_by_ids(self, order_ids: list[str]): ...

    @abstractmethod
    def get_transactions_by_order_ids(self, order_ids: list[str]): ...

    @abstractmethod
    def cancel_order(self, order_id: str) -> None: ...

    @abstractmethod
    def get_last_traded_dt(self, asset): ...

    @abstractmethod
    def get_spot_value(self, assets, field, dt, data_frequency): ...

    @abstractmethod
    def get_realtime_bars(self, assets, frequency): ...

    @abstractmethod
    def get_slippage_model(self, asset: Asset) -> SlippageModel: ...

    @abstractmethod
    def get_commission_model(self, asset: Asset) -> CommissionModel: ...
