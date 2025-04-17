import datetime
import time
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.gens.domain.simulation_event import SimulationEvent
from ziplime.gens.domain.trading_clock import TradingClock


class RealtimeClock(TradingClock):
    def __init__(self,
                 sessions: pl.Series,
                 market_opens: pl.Series,
                 market_closes: pl.Series,
                 before_trading_start_minutes: pl.Series,
                 timezone: ZoneInfo,
                 emission_rate: datetime.timedelta,
                 timedelta_diff_from_current_time: datetime.timedelta = None,
                 ):
        self.sessions = sessions
        self.market_opens = market_opens
        self.market_closes = market_closes
        self.before_trading_start_minutes = before_trading_start_minutes
        self.before_trading_start_bar_yielded = len(self.before_trading_start_minutes) * [False]
        self.market_closes_yielded = len(self.market_closes) * [False]
        self.market_opens_yielded = len(self.market_opens) * [False]

        self.timezone = timezone
        self.emission_rate = emission_rate
        if timedelta_diff_from_current_time is None:
            self.timedelta_diff_from_current_time = datetime.timedelta(seconds=0)
        else:
            self.timedelta_diff_from_current_time = timedelta_diff_from_current_time
        self.minutes_by_session = self.calc_minutes_by_session()

    def calc_minutes_by_session(self):
        minutes_by_session_n = {}
        if self.emission_rate < datetime.timedelta(days=1):
            for session_idx, session in enumerate(self.sessions):
                minutes = pl.datetime_range(self.market_opens[session_idx], self.market_closes[session_idx],
                                            interval=self.emission_rate,
                                            eager=True)
                minutes_by_session_n[session] = minutes
        else:
            minutes_by_session_n = {session: pl.Series([self.market_closes[session_idx]]) for
                                    session_idx, session in enumerate(self.sessions)}
        return minutes_by_session_n

    def _sleep_and_increase_time(self, sleep_seconds: int) -> datetime.datetime:
        time.sleep(sleep_seconds)
        now = datetime.datetime.now(tz=self.timezone) + self.timedelta_diff_from_current_time
        return now

    def __iter__(self):
        current_time = datetime.datetime.now(tz=self.timezone) + self.timedelta_diff_from_current_time
        last_bar_emit: datetime.datetime | None =  None
        while current_time <= self.market_closes[-1]:
            current_session_index = self.sessions.index_of(current_time.date())
            if current_session_index is None:
                # happens if current time is not in trading hours, so we need to wait till market hours
                current_time = self._sleep_and_increase_time(sleep_seconds=1)
                continue
            if current_time >= self.before_trading_start_minutes[current_session_index] and not \
                    self.before_trading_start_bar_yielded[current_session_index]:
                self.before_trading_start_bar_yielded[current_session_index] = True
                yield current_time, SimulationEvent.BEFORE_TRADING_START_BAR
            elif current_time < self.market_opens[current_session_index]:
                current_time = self._sleep_and_increase_time(sleep_seconds=1)
            elif self.market_opens[current_session_index] <= current_time <= self.market_closes[current_session_index] and not self.market_opens_yielded[current_session_index]:
                self.market_opens_yielded[current_session_index] = True
                yield current_time.date(), SimulationEvent.SESSION_START
            elif self.market_opens[current_session_index] <= current_time <= self.market_closes[current_session_index]:
                if last_bar_emit is None or current_time - last_bar_emit >= self.emission_rate:
                    last_bar_emit = current_time
                    yield current_time, SimulationEvent.BAR
                    current_time = datetime.datetime.now(tz=self.timezone) + self.timedelta_diff_from_current_time
                    yield current_time, SimulationEvent.EMISSION_RATE_END
                else:
                    current_time = self._sleep_and_increase_time(sleep_seconds=1)
            elif current_time > self.market_closes[current_session_index]:
                if self.market_closes_yielded[current_session_index]:
                    current_time = self._sleep_and_increase_time(sleep_seconds=1)
                else:
                    last_bar_emit = current_time
                    yield current_time.date(), SimulationEvent.SESSION_END
            else:
                # We should never end up in this branch
                raise RuntimeError("Invalid state in RealtimeClock")
