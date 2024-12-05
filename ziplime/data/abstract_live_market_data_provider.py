import datetime
from abc import abstractmethod
from lime_trader.models.market import Period


class AbstractLiveMarketDataProvider:

    @abstractmethod
    def fetch_live_data_table(
            self,
            symbols: list[str],
            period: Period,
            date_from: datetime.datetime,
            date_to: datetime.datetime,
            show_progress: bool):
        pass
