import datetime
from exchange_calendars import ExchangeCalendar

from ziplime.domain.data_frequency import DataFrequency


class SimulationParameters:
    def __init__(
            self,
            start_session: datetime.datetime,
            end_session: datetime.datetime,
            trading_calendar: ExchangeCalendar,
            capital_base: float,
            emission_rate: DataFrequency,
            # data_frequency: str = "daily",
            arena: str = "backtest",
    ):

        if start_session >= end_session:
            raise ValueError("Period start falls after period end.")
        if start_session >= trading_calendar.last_session:
            raise ValueError("Period start falls after the last known trading day.")
        if end_session <= trading_calendar.first_session:
            raise ValueError("Period end falls before the first known trading day.")

        # chop off any minutes or hours on the given start and end dates,
        # as we only support session labels here (and we represent session
        # labels as midnight UTC).
        self.start_session = start_session.date()
        self.end_session = end_session.date()
        self.capital_base = capital_base

        self.emission_rate = emission_rate
        # self.data_frequency = data_frequency

        # copied to algorithm's environment for runtime access
        self.arena = arena

        self.trading_calendar = trading_calendar

        if not trading_calendar.is_session(self.start_session):
            # if the start date is not a valid session in this calendar,
            # push it forward to the first valid session
            self.start_session = trading_calendar.minute_to_session(
                self.start_session
            ).tz_localize(self.trading_calendar.tz)

        if not trading_calendar.is_session(self.end_session):
            # if the end date is not a valid session in this calendar,
            # pull it backward to the last valid session before the given
            # end date.
            self._end_session = trading_calendar.minute_to_session(
                self.end_session, direction="previous"
            ).tz_localize(self.trading_calendar.tz)

        self.first_open = trading_calendar.session_first_minute(
            self.start_session
        ).tz_convert(self.trading_calendar.tz)
        self.last_close = trading_calendar.session_close(
            self.end_session
        ).tz_convert(self.trading_calendar.tz)


        self.sessions = self.trading_calendar.sessions_in_range(
            self.start_session, self.end_session
        )
