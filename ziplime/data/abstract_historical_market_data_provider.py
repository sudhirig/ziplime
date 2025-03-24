import datetime
from abc import abstractmethod

from exchange_calendars import ExchangeCalendar
from lime_trader.models.market import Period


class AbstractHistoricalMarketDataProvider:

    @abstractmethod
    def get_historical_data_table(self, symbols: list[str],
                                  frequency: datetime.timedelta,
                                  date_from: datetime.datetime,
                                  date_to: datetime.datetime,
                                  show_progress: bool,
                                  exchange_calendar: ExchangeCalendar,
                                  ):
        pass
