from time import sleep
from typing import Callable

import pandas as pd

from zipline.gens.sim_engine import (
    BAR,
    SESSION_START,
    SESSION_END,
    MINUTE_END,
    BEFORE_TRADING_START_BAR
)


class RealtimeClock:
    """Realtime clock for live trading.

    This class is a drop-in replacement for
    :class:`zipline.gens.sim_engine.MinuteSimulationClock`.
    The key difference between the two is that the RealtimeClock's event
    emission is synchronized to the (broker's) wall time clock, while
    MinuteSimulationClock yields a new event on every iteration (regardless of
    wall clock).

    The :param:`time_skew` parameter represents the time difference between
    the Broker and the live trading machine's clock.
    """

    def __init__(self,
                 sessions,
                 execution_opens,
                 execution_closes,
                 before_trading_start_minutes,
                 minute_emission: bool,
                 is_broker_alive: Callable,
                 frequency: str,
                 time_skew: pd.Timedelta = pd.Timedelta("0s"),
                 ):
        self.sessions = sessions
        self.execution_opens = execution_opens
        self.execution_closes = execution_closes
        self.before_trading_start_minutes = before_trading_start_minutes
        self.minute_emission = minute_emission
        self.time_skew = time_skew
        self.is_broker_alive = is_broker_alive
        self._last_emit = None
        self._before_trading_start_bar_yielded = [False for i in range(len(self.execution_opens))]
        self._frequency = frequency

    def __iter__(self):
        # self.execution_closes.iloc[0] = self.execution_closes.iloc[0] +pd.Timedelta(minutes=160)

        if self._frequency == "daily":
            delta = pd.Timedelta(days=1)
        else:
            delta = pd.Timedelta(minutes=1)
        yield self.sessions[0], SESSION_START

        # current = pd.to_datetime('now', utc=True) - pd.Timedelta(days=3)
        # current_times = []
        # for i in range(200):
        #     next_time = current + pd.Timedelta(hours=4)
        #     current = next_time
        #     current_times.append(next_time)
        # i = 0
        # current_session_index = 0
        # last_session_index = 0
        # while self.is_broker_alive() and i <96:
        while self.is_broker_alive():
            # current_time = current_times[i]
            current_time = pd.to_datetime('now', utc=True)
            server_time = (current_time + self.time_skew).floor('1 min')
            current_session_index = self.sessions.searchsorted(pd.Timestamp(current_time.date()))
            if current_session_index is None or current_session_index == len(self.sessions):
                # we have got to an end
                return
            if server_time >= self.before_trading_start_minutes[current_session_index] and not \
                    self._before_trading_start_bar_yielded[current_session_index]:
                # this just calls initialize, so we can emit bar event immediately
                # self._last_emit = server_time
                self._before_trading_start_bar_yielded[current_session_index] = True
                yield server_time, BEFORE_TRADING_START_BAR
            elif server_time < self.execution_opens.iloc[current_session_index]:
                sleep(1)
            elif self.execution_opens.iloc[current_session_index] <= server_time < self.execution_closes.iloc[
                current_session_index]:
                if self._last_emit is None or server_time - self._last_emit >= delta:
                    self._last_emit = server_time
                    yield server_time, BAR
                    if self.minute_emission:
                        yield server_time, MINUTE_END
                else:
                    sleep(1)
            elif server_time == self.execution_closes.iloc[current_session_index]:
                self._last_emit = server_time
                yield server_time, BAR
                if self.minute_emission:
                    yield server_time, MINUTE_END
                yield server_time, SESSION_END
            elif server_time > self.execution_closes.iloc[current_session_index]:
                # Return with no yield if the algo is started in after hours
                # just continue
                sleep(1)
            else:
                # We should never end up in this branch
                raise RuntimeError("Invalid state in RealtimeClock")
            # i += 1
