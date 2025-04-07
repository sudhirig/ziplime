import datetime
from zoneinfo import ZoneInfo

import polars as pl

from ziplime.gens.domain.simulation_event import SimulationEvent
from ziplime.gens.domain.trading_clock import TradingClock


class SimulationClock(TradingClock):
    def __init__(self,
                 sessions: pl.Series,
                 market_opens: pl.Series,
                 market_closes: pl.Series,
                 before_trading_start_minutes: pl.Series,
                 timezone: ZoneInfo,
                 emission_rate: datetime.timedelta):
        self.sessions = sessions
        self.market_opens = market_opens
        self.market_closes = market_closes
        self.before_trading_start_minutes = before_trading_start_minutes
        self.timezone = timezone
        self.emission_rate = emission_rate
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

    def __iter__(self):
        for idx, session in enumerate(self.sessions):
            yield session, SimulationEvent.SESSION_START

            bts_minute = self.before_trading_start_minutes[idx]
            regular_minutes = self.minutes_by_session[session]

            yield bts_minute, SimulationEvent.BEFORE_TRADING_START_BAR
            for minute in regular_minutes:
                yield minute, SimulationEvent.BAR
                yield minute, SimulationEvent.EMISSION_RATE_END

            yield regular_minutes[-1], SimulationEvent.SESSION_END
