#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
from zoneinfo import ZoneInfo

import polars as pl

BAR = 0
SESSION_START = 1
SESSION_END = 2
EMISSION_RATE_END = 3
BEFORE_TRADING_START_BAR = 4


class MinuteSimulationClock:
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
            yield session, SESSION_START

            bts_minute = self.before_trading_start_minutes[idx]
            regular_minutes = self.minutes_by_session[session]

            yield bts_minute, BEFORE_TRADING_START_BAR
            for minute in regular_minutes:
                yield minute, BAR
                yield minute, EMISSION_RATE_END

            yield regular_minutes[-1], SESSION_END
