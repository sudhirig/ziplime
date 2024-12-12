from abc import ABCMeta, abstractmethod


class Broker(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def subscribe_to_market_data(self, asset):
        pass

    @property
    @abstractmethod
    def subscribed_assets(self):
        pass

    @property
    @abstractmethod
    def positions(self):
        pass

    @property
    @abstractmethod
    def portfolio(self):
        pass

    @property
    @abstractmethod
    def account(self):
        pass

    @property
    @abstractmethod
    def time_skew(self):
        pass

    @abstractmethod
    def order(self, asset, amount, style):
        pass

    def is_alive(self):
        pass

    @property
    @abstractmethod
    def orders(self):
        pass

    @property
    @abstractmethod
    def transactions(self):
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
