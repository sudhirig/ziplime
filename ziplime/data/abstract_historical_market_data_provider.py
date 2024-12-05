import datetime
from abc import abstractmethod
from lime_trader.models.market import Period


class AbstractHistoricalMarketDataProvider:

    @abstractmethod
    def get_historical_data_table(self, symbols: list[str],
                                  period: Period,
                                  date_from: datetime.datetime,
                                  date_to: datetime.datetime,
                                  show_progress: bool,
                                  ):
        pass
