import datetime
import inspect

import pandas as pd
from pandas import DatetimeIndex
from zipline.utils.calendar_utils import wrap_with_signature
from exchange_calendars import get_calendar as ec_get_calendar  # get_calendar,


def normalize_daily_start_end_session(
        calendar_name: str, start_session: datetime.datetime,
        end_session: datetime.datetime) -> tuple[pd.Timestamp, pd.Timestamp]:
    cal = ec_get_calendar(calendar_name, start=start_session)
    if start_session < cal.first_session:  # eg. Starts on 1 Jan will be realigned to the first trading day of the year
        start_session = cal.first_session
        if start_session.weekday() == 6:  # Don't start on Sundays, this helps with futures testing...
            start_session = cal.next_close(start_session).floor(freq="D")
    if not (cal.is_session(end_session)):
        end_session = cal.previous_close(end_session).floor(freq="D")

    start_session = pd.Timestamp(ts_input=start_session, tzinfo=None).tz_localize(None)
    end_session = pd.Timestamp(ts_input=end_session, tzinfo=None).tz_localize(None)
    return start_session, end_session


def days_at_time(days, t, tz, day_offset=0):
    """
    Create an index of days at time ``t``, interpreted in timezone ``tz``.

    The returned index is localized to UTC.

    Parameters
    ----------
    days : DatetimeIndex
        An index of dates (represented as midnight).
    t : datetime.time
        The time to apply as an offset to each day in ``days``.
    tz : pytz.timezone
        The timezone to use to interpret ``t``.
    day_offset : int
        The number of days we want to offset @days by

    Example
    -------
    In the example below, the times switch from 13:45 to 12:45 UTC because
    March 13th is the daylight savings transition for US/Eastern.  All the
    times are still 8:45 when interpreted in US/Eastern.

    >>> import pandas as pd; import datetime; import pprint
    >>> dts = pd.date_range('2016-03-12', '2016-03-14')
    >>> dts_at_845 = days_at_time(dts, datetime.time(8, 45), 'US/Eastern')
    >>> pprint.pprint([str(dt) for dt in dts_at_845])
    ['2016-03-12 13:45:00+00:00',
     '2016-03-13 12:45:00+00:00',
     '2016-03-14 12:45:00+00:00']
    """
    if len(days) == 0:
        return days

    # Offset days without tz to avoid timezone issues.
    days = DatetimeIndex(days).tz_localize(None)
    delta = pd.Timedelta(
        days=day_offset,
        hours=t.hour,
        minutes=t.minute,
        seconds=t.second,
    )
    return (days + delta).tz_localize(tz).tz_convert('UTC')

@wrap_with_signature(inspect.signature(ec_get_calendar))
def get_calendar(*args, **kwargs):
    if args[0] in ["us_futures", "CMES", "XNYS", "NYSE"]:
        return ec_get_calendar(*args, side="right", start=pd.Timestamp("1990-01-01"))
    return ec_get_calendar(*args, side="right")



def add_tz_info(d: pd.Timestamp, tzinfo:datetime.tzinfo) -> pd.Timestamp:
    if d.tzinfo is None:
        return d.replace(tzinfo=tzinfo)
    return d