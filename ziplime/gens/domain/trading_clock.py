import datetime
from abc import abstractmethod

from exchange_calendars import ExchangeCalendar


class TradingClock:
    trading_calendar: ExchangeCalendar
    emission_rate: datetime.timedelta

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError("__iter__ method must be implemented for trading clock.")
