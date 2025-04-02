import datetime
from abc import abstractmethod



class AbstractFundamentalDataProvider:

    @abstractmethod
    def get_fundamental_data(self,
                             symbols: list[str],
                             date_from: datetime.datetime,
                             date_to: datetime.datetime,
                             frequency: datetime.timedelta,
                             fundamental_data_list: set[str]
                             ):
        pass
