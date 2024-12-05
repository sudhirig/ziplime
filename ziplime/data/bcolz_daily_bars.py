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
import warnings
from functools import partial

from exchange_calendars import ExchangeCalendar
from zipline.data._equities import _read_bcolz_data, _compute_row_slices

from ziplime.constants.default_columns import DEFAULT_COLUMNS
from ziplime.domain.column_specification import ColumnSpecification

with warnings.catch_warnings():  # noqa
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    from bcolz import carray, ctable
    import numpy as np

import logging

import pandas as pd

from zipline.utils.input_validation import expect_element

logger = logging.getLogger("UsEquityPricing")

OHLC = frozenset(["open", "high", "low", "close"])
# US_EQUITY_PRICING_BCOLZ_COLUMNS = (
#     "open",
#     "high",
#     "low",
#     "close",
#     "volume",
#     "day",
#     "id",
# )
#
UINT32_MAX = np.iinfo(np.uint32).max


def check_uint32_safe(value, colname):
    if value >= UINT32_MAX:
        raise ValueError("Value %s from column '%s' is too large" % (value, colname))


@expect_element(invalid_data_behavior={"warn", "raise", "ignore"})
def winsorise_uint32(df, invalid_data_behavior, column, *columns):
    """Drops any record where a value would not fit into a uint32.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to winsorise.
    invalid_data_behavior : {'warn', 'raise', 'ignore'}
        What to do when data is outside the bounds of a uint32.
    *columns : iterable[str]
        The names of the columns to check.

    Returns
    -------
    truncated : pd.DataFrame
        ``df`` with values that do not fit into a uint32 zeroed out.
    """
    columns = list((column,) + columns)
    mask = df[columns] > UINT32_MAX

    if invalid_data_behavior != "ignore":
        mask |= df[columns].isnull()
    else:
        # we are not going to generate a warning or error for this so just use
        # nan_to_num
        df[columns] = np.nan_to_num(df[columns])

    mv = mask.values
    if mv.any():
        if invalid_data_behavior == "raise":
            raise ValueError(
                "%d values out of bounds for uint32: %r"
                % (
                    mv.sum(),
                    df[mask.any(axis=1)],
                ),
            )
        if invalid_data_behavior == "warn":
            warnings.warn(
                "Ignoring %d values because they are out of bounds for"
                " uint32:\n %r"
                % (
                    mv.sum(),
                    df[mask.any(axis=1)],
                ),
                stacklevel=3,  # one extra frame for `expect_element`
            )

    df[mask] = 0
    return df


class ZiplimeBcolzDailyBarWriter:
    """Class capable of writing daily OHLCV data to disk in a format that can
    be read efficiently by BcolzDailyOHLCVReader.

    Parameters
    ----------
    filename : str
        The location at which we should write our output.
    calendar : zipline.utils.calendar.trading_calendar
        Calendar to use to compute asset calendar offsets.
    start_session: pd.Timestamp
        Midnight UTC session label.
    end_session: pd.Timestamp
        Midnight UTC session label.

    See Also
    --------
    zipline.data.bcolz_daily_bars.BcolzDailyBarReader
    """

    # _csv_dtypes = {
    #     "open": float64_dtype,
    #     "high": float64_dtype,
    #     "low": float64_dtype,
    #     "close": float64_dtype,
    #     "volume": float64_dtype,
    # }

    def __init__(self, filename: str, calendar: ExchangeCalendar, start_session: pd.Timestamp,
                 end_session: pd.Timestamp, cols: list[ColumnSpecification] = DEFAULT_COLUMNS):

        self.cols = cols
        self._filename = filename
        start_session = start_session.tz_localize(None)
        end_session = end_session.tz_localize(None)

        if start_session != end_session:
            if not calendar.is_session(start_session):
                raise ValueError("Start session %s is invalid!" % start_session)
            if not calendar.is_session(end_session):
                raise ValueError("End session %s is invalid!" % end_session)

        self._start_session = start_session
        self._end_session = end_session

        self._calendar = calendar
