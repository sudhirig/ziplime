import datetime
from abc import abstractmethod

from lime_trader.models.market import Period



class AbstractFundamentalDataProvider:

    @abstractmethod
    def get_fundamental_data(self,
                             symbols: list[str],
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             period: Period,
                             fundamental_data_list: set[str]
                             ):
        pass
