from abc import abstractmethod

from zipline.assets import Asset
from zipline.finance.order import Order as ZPOrder

from ziplime.protocol import Position, Portfolio, Account


class Broker:
    @abstractmethod
    def subscribe_to_market_data(self, asset):
        pass

    @abstractmethod
    def get_subscribed_assets(self):
        pass

    @abstractmethod
    def get_positions(self) -> dict[Asset, Position]:
        pass

    @abstractmethod
    def get_portfolio(self) -> Portfolio:
        pass

    @abstractmethod
    def get_account(self) -> Account:
        pass

    @abstractmethod
    def get_time_skew(self):
        pass

    @abstractmethod
    def order(self, asset, amount, style):
        pass

    def is_alive(self):
        pass

    @abstractmethod
    def get_orders(self) -> dict[str, ZPOrder]:
        pass

    @abstractmethod
    def get_transactions(self):
        pass

    @abstractmethod
    def get_orders_by_ids(self, order_ids: list[str]):
        pass

    @abstractmethod
    def get_transactions_by_order_ids(self, order_ids: list[str]):
        pass

    @abstractmethod
    def cancel_order(self, order_param):
        pass

    @abstractmethod
    def get_last_traded_dt(self, asset):
        pass

    @abstractmethod
    def get_spot_value(self, assets, field, dt, data_frequency):
        pass

    @abstractmethod
    def get_realtime_bars(self, assets, frequency):
        pass
