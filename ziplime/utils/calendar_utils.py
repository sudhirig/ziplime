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
