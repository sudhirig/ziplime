import datetime

import pandas as pd


# def normalize_daily_start_end_session(
#         calendar_name: str, start_session: datetime.datetime,
#         end_session: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
#     cal = ec_get_calendar(calendar_name, start=start_session)
#     if start_session < cal.first_session:  # eg. Starts on 1 Jan will be realigned to the first trading day of the year
#         start_session = cal.first_session
#         if start_session.weekday() == 6:  # Don't start on Sundays, this helps with futures testing...
#             start_session = cal.next_close(start_session).floor(freq="D")
#     if not (cal.is_session(end_session)):
#         end_session = cal.previous_close(end_session).floor(freq="D")
#
#     start_session = pd.Timestamp(ts_input=start_session, tzinfo=None).tz_localize(None)
#     end_session = pd.Timestamp(ts_input=end_session, tzinfo=None).tz_localize(None)
#     return start_session, end_session


# @wrap_with_signature(inspect.signature(ec_get_calendar))
# def get_calendar(*args, **kwargs):
#     if args[0] in ["us_futures", "CMES", "XNYS", "NYSE"]:
#         return ec_get_calendar(*args, side="right", start=pd.Timestamp("1990-01-01"))
#     return ec_get_calendar(*args, side="right")
#


def add_tz_info(d: pd.Timestamp, tzinfo: datetime.tzinfo) -> pd.Timestamp:
    if d.tzinfo is None:
        return d.replace(tzinfo=tzinfo)
    return d


import inspect

import pandas as pd
from exchange_calendars import get_calendar as ec_get_calendar  # get_calendar,

from exchange_calendars.utils.pandas_utils import days_at_time  # noqa: reexport


# https://stackoverflow.com/questions/56753846/python-wrapping-function-with-signature
def wrap_with_signature(signature):
    def wrapper(func):
        func.__signature__ = signature
        return func

    return wrapper


@wrap_with_signature(inspect.signature(ec_get_calendar))
def get_calendar(*args, **kwargs):
    if args[0] in ["us_futures", "CMES", "XNYS", "NYSE"]:
        return ec_get_calendar(*args, side="right", start=pd.Timestamp("1990-01-01"))
    return ec_get_calendar(*args, side="right")

# get_calendar = compose(partial(get_calendar, side="right"), "XNYS")
# NOTE Sessions are now timezone-naive (previously UTC).
# Schedule columns now have timezone set as UTC
# (whilst the times have always been defined in terms of UTC,
# previously the dtype was timezone-naive).
