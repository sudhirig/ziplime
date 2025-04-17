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

from itertools import chain

import pandas as pd
from numpy import (
    int64,
    uint32,
    zeros,
)
from pandas import Timestamp

from ziplime.lib.adjustment import Float64Multiply

from ziplime.utils.pandas_utils import timedelta_to_integral_seconds
from ziplime.utils.sqlite_utils import SQLITE_MAX_VARIABLE_NUMBER

_SID_QUERY_TEMPLATE = """
SELECT DISTINCT sid FROM {0}
WHERE effective_date >= ? AND effective_date <= ?
"""
SID_QUERIES = {
    tablename: _SID_QUERY_TEMPLATE.format(tablename)
    for tablename in ('splits', 'dividends', 'mergers')
}

ADJ_QUERY_TEMPLATE = """
SELECT sid, ratio, effective_date
FROM {0}
WHERE sid IN ({1}) AND effective_date >= {2} AND effective_date <= {3}
"""

EPOCH = Timestamp(0, tz='UTC')


def _get_sids_from_table(db,
                         tablename: str,
                         start_date: int,
                         end_date: int) -> set:
    """Get the unique sids for all adjustments between start_date and end_date
    from table `tablename`.

    Parameters
    ----------
    db : sqlite3.connection
    tablename : str
    start_date : int (seconds since epoch)
    end_date : int (seconds since epoch)

    Returns
    -------
    sids : set
        Set of sets
    """

    cursor = db.execute(
        SID_QUERIES[tablename],
        (start_date, end_date),
    )
    out = set()
    for result in cursor.fetchall():
        out.add(result[0])
    return out


def _get_split_sids(db, start_date: int, end_date: int) -> set:
    return _get_sids_from_table(db, 'splits', start_date, end_date)


def _get_merger_sids(db, start_date: int, end_date: int) -> set:
    return _get_sids_from_table(db, 'mergers', start_date, end_date)


def _get_dividend_sids(db, start_date: int, end_date: int) -> set:
    return _get_sids_from_table(db, 'dividends', start_date, end_date)


def _adjustments(adjustments_db,
                 split_sids: set,
                 merger_sids: set,
                 dividends_sids: set,
                 start_date: int,
                 end_date: int,
                 assets: pd.Index):
    c = adjustments_db.cursor()

    splits_to_query = [str(a) for a in assets if a in split_sids]
    splits_results = []
    while splits_to_query:
        query_len = min(len(splits_to_query), SQLITE_MAX_VARIABLE_NUMBER)
        query_assets = splits_to_query[:query_len]
        t = [str(a) for a in query_assets]
        statement = ADJ_QUERY_TEMPLATE.format(
            'splits',
            ",".join(['?' for _ in query_assets]),
            start_date,
            end_date,
        )
        c.execute(statement, t)
        splits_to_query = splits_to_query[query_len:]
        splits_results.extend(c.fetchall())

    mergers_to_query = [str(a) for a in assets if a in merger_sids]
    mergers_results = []
    while mergers_to_query:
        query_len = min(len(mergers_to_query), SQLITE_MAX_VARIABLE_NUMBER)
        query_assets = mergers_to_query[:query_len]
        t = [str(a) for a in query_assets]
        statement = ADJ_QUERY_TEMPLATE.format(
            'mergers',
            ",".join(['?' for _ in query_assets]),
            start_date,
            end_date,
        )
        c.execute(statement, t)
        mergers_to_query = mergers_to_query[query_len:]
        mergers_results.extend(c.fetchall())

    dividends_to_query = [str(a) for a in assets if a in dividends_sids]
    dividends_results = []
    while dividends_to_query:
        query_len = min(len(dividends_to_query), SQLITE_MAX_VARIABLE_NUMBER)
        query_assets = dividends_to_query[:query_len]
        t = [str(a) for a in query_assets]
        statement = ADJ_QUERY_TEMPLATE.format(
            'dividends',
            ",".join(['?' for _ in query_assets]),
            start_date,
            end_date,
        )
        c.execute(statement, t)
        dividends_to_query = dividends_to_query[query_len:]
        dividends_results.extend(c.fetchall())

    return splits_results, mergers_results, dividends_results


def load_adjustments_from_sqlite(adjustments_db,
                                 dates: pd.DatetimeIndex,
                                 assets: pd.Index,
                                 should_include_splits: bool,
                                 should_include_mergers: bool,
                                 should_include_dividends: bool,
                                 adjustment_type: str):
    """Load a dictionary of Adjustment objects from adjustments_db.

    Parameters
    ----------
    adjustments_db : sqlite3.Connection
        Connection to a sqlite3 table in the format written by
        SQLiteAdjustmentWriter.
    dates : pd.DatetimeIndex
        Dates for which adjustments are needed.
    assets : pd.Int64Index
        Assets for which adjustments are needed.
    should_include_splits : bool
        Whether split adjustments should be included.
    should_include_mergers : bool
        Whether merger adjustments should be included.
    should_include_dividends : bool
        Whether dividend adjustments should be included.
    adjustment_type : str
        Whether price adjustments, volume adjustments, or both, should be
        included in the output.

    Returns
    -------
    adjustments : dict[str -> dict[int -> Adjustment]]
        A dictionary containing price and/or volume adjustment mappings from
        index to adjustment objects to apply at that index.
    """

    if not (adjustment_type == 'price' or
            adjustment_type == 'volume' or
            adjustment_type == 'all'):
        raise ValueError(
            "%s is not a valid adjustment type.\n"
            "Valid adjustment types are 'price', 'volume', and 'all'.\n" % (
                adjustment_type,
            )
        )

    should_include_price_adjustments = bool(
        adjustment_type == 'all' or adjustment_type == 'price'
    )
    should_include_volume_adjustments = bool(
        adjustment_type == 'all' or adjustment_type == 'volume'
    )

    if not should_include_price_adjustments:
        should_include_mergers = False
        should_include_dividends = False

    start_date = timedelta_to_integral_seconds(dates[0] - EPOCH)
    end_date = timedelta_to_integral_seconds(dates[-1] - EPOCH)

    if should_include_splits:
        split_sids = _get_split_sids(
            adjustments_db,
            start_date,
            end_date,
        )
    else:
        split_sids = set()

    if should_include_mergers:
        merger_sids = _get_merger_sids(
            adjustments_db,
            start_date,
            end_date,
        )
    else:
        merger_sids = set()

    if should_include_dividends:
        dividend_sids = _get_dividend_sids(
            adjustments_db,
            start_date,
            end_date,
        )
    else:
        dividend_sids = set()

    splits, mergers, dividends = _adjustments(
        adjustments_db,
        split_sids,
        merger_sids,
        dividend_sids,
        start_date,
        end_date,
        assets,
    )

    price_adjustments = {}
    volume_adjustments = {}
    result = {}
    asset_ixs = {}  # Cache sid lookups here.
    date_ixs = {}

    _dates_seconds = \
        dates.values.astype('datetime64[s]').view(int64)

    # Pre-populate date index cache.
    for i, dt in enumerate(_dates_seconds):
        date_ixs[dt] = i

    # splits affect prices and volumes, volumes is the inverse
    for sid, ratio, eff_date in splits:
        if eff_date < start_date:
            continue

        date_loc = _lookup_dt(date_ixs, eff_date, _dates_seconds)

        if sid not in asset_ixs:
            asset_ixs[sid] = assets.get_loc(sid)
        asset_ix = asset_ixs[sid]

        if should_include_price_adjustments:
            price_adj = Float64Multiply(0, date_loc, asset_ix, asset_ix, ratio)
            price_adjustments.setdefault(date_loc, []).append(price_adj)

        if should_include_volume_adjustments:
            volume_adj = Float64Multiply(
                0, date_loc, asset_ix, asset_ix, 1.0 / ratio
            )
            volume_adjustments.setdefault(date_loc, []).append(volume_adj)

    # mergers and dividends affect prices only
    for sid, ratio, eff_date in chain(mergers, dividends):
        if eff_date < start_date:
            continue

        date_loc = _lookup_dt(date_ixs, eff_date, _dates_seconds)

        if sid not in asset_ixs:
            asset_ixs[sid] = assets.get_loc(sid)
        asset_ix = asset_ixs[sid]

        price_adj = Float64Multiply(0, date_loc, asset_ix, asset_ix, ratio)
        price_adjustments.setdefault(date_loc, []).append(price_adj)

    if should_include_price_adjustments:
        result['price'] = price_adjustments
    if should_include_volume_adjustments:
        result['volume'] = volume_adjustments

    return result


def _lookup_dt(dt_cache: dict,
               dt: int,
               fallback):
    if dt not in dt_cache:
        dt_cache[dt] = fallback.searchsorted(dt, side='right')
    return dt_cache[dt]
