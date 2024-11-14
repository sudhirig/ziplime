import datetime

import pandas as pd
from exchange_calendars import get_calendar


def normalize_daily_start_end_session(
        calendar_name: str, start_session: datetime.datetime,
        end_session: datetime.datetime) -> tuple[pd.Timestamp, pd.Timestamp]:
    cal = get_calendar(calendar_name, start=start_session)
    if start_session < cal.first_session:  # eg. Starts on 1 Jan will be realigned to the first trading day of the year
        start_session = cal.first_session
        if start_session.weekday() == 6:  # Don't start on Sundays, this helps with futures testing...
            start_session = cal.next_close(start_session).floor(freq="D")
    if not (cal.is_session(end_session)):
        end_session = cal.previous_close(end_session).floor(freq="D")

    start_session = pd.Timestamp(ts_input=start_session, tzinfo=None).tz_localize(None)
    end_session = pd.Timestamp(ts_input=end_session, tzinfo=None).tz_localize(None)
    return start_session, end_session
