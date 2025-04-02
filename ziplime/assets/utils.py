from collections import namedtuple

import logging
import pandas as pd
import sqlalchemy as sa
from toolz import (
    concatv,
    curry,
    groupby,
    sliding_window,
    valmap,
)

import numpy as np

from .domain.continuous_future import ContinuousFuture


def make_asset_array(size, asset):
    out = np.empty([size], dtype=object)
    out.fill(asset)
    return out


log = logging.getLogger("assets.py")

# A set of fields that need to be converted to timestamps in UTC
_asset_timestamp_fields = frozenset(
    {
        "start_date",
        "end_date",
        "first_traded",
        "notice_date",
        "expiration_date",
        "auto_close_date",
    }
)

OwnershipPeriod = namedtuple("OwnershipPeriod", "start end sid value")


def merge_ownership_periods(mappings):
    """Given a dict of mappings where the values are lists of
    OwnershipPeriod objects, returns a dict with the same structure with
    new OwnershipPeriod objects adjusted so that the periods have no
    gaps.

    Orders the periods chronologically, and pushes forward the end date
    of each period to match the start date of the following period. The
    end date of the last period pushed forward to the max Timestamp.
    """
    return valmap(
        lambda v: tuple(
            OwnershipPeriod(
                a.start,
                b.start,
                a.sid,
                a.value,
            )
            for a, b in sliding_window(
                2,
                concatv(
                    sorted(v),
                    # concat with a fake ownership object to make the last
                    # end date be max timestamp
                    [
                        OwnershipPeriod(
                            pd.Timestamp.max,
                            None,
                            None,
                            None,
                        )
                    ],
                ),
            )
        ),
        mappings,
    )


def _build_ownership_map_from_rows(rows, key_from_row, value_from_row):
    mappings = {}
    for row in rows:
        mappings.setdefault(
            key_from_row(row),
            [],
        ).append(
            OwnershipPeriod(
                # TODO FIX TZ MESS
                # pd.Timestamp(row.start_date, unit="ns", tz="utc"),
                # pd.Timestamp(row.end_date, unit="ns", tz="utc"),
                pd.Timestamp(row.start_date, unit="ns", tz='UTC').to_pydatetime(),
                pd.Timestamp(row.end_date, unit="ns", tz='UTC').to_pydatetime(),
                row.sid,
                value_from_row(row),
            ),
        )

    return merge_ownership_periods(mappings)


def build_ownership_map(conn, table, key_from_row, value_from_row):
    """Builds a dict mapping to lists of OwnershipPeriods, from a db table."""
    return _build_ownership_map_from_rows(
        conn.execute(sa.select(table.c)).fetchall(),
        key_from_row,
        value_from_row,
    )


def build_grouped_ownership_map(conn, table, key_from_row, value_from_row, group_key):
    """Builds a dict mapping group keys to maps of keys to lists of
    OwnershipPeriods, from a db table.
    """

    grouped_rows = groupby(
        group_key,
        conn.execute(sa.select(table.c)).fetchall(),
    )
    return {
        key: _build_ownership_map_from_rows(
            rows,
            key_from_row,
            value_from_row,
        )
        for key, rows in grouped_rows.items()
    }


@curry
def _filter_kwargs(names, dict_):
    """Filter out kwargs from a dictionary.

    Parameters
    ----------
    names : set[str]
        The names to select from ``dict_``.
    dict_ : dict[str, any]
        The dictionary to select from.

    Returns
    -------
    kwargs : dict[str, any]
        ``dict_`` where the keys intersect with ``names`` and the values are
        not None.
    """
    return {k: v for k, v in dict_.items() if k in names and v is not None}


asset_args = frozenset({
    'sid',
    'symbol',
    'asset_name',
    'start_date',
    'end_date',
    'first_traded',
    'auto_close_date',
    'tick_size',
    'multiplier',
    'exchange_info',
})
_filter_future_kwargs = _filter_kwargs(asset_args)
_filter_equity_kwargs = _filter_kwargs(asset_args)


def _convert_asset_timestamp_fields(dict_):
    """Takes in a dict of Asset init args and converts dates to pd.Timestamps"""
    for key in _asset_timestamp_fields & dict_.keys():
        # TODO FIX TZ MESS
        # value = pd.Timestamp(dict_[key], tz="UTC")
        value = pd.Timestamp(dict_[key], tz=None).to_pydatetime().date()
        dict_[key] = None if pd.isnull(value) else value
    return dict_


SID_TYPE_IDS = {
    # Asset would be 0,
    ContinuousFuture: 1,
}

CONTINUOUS_FUTURE_ROLL_STYLE_IDS = {
    "calendar": 0,
    "volume": 1,
}

CONTINUOUS_FUTURE_ADJUSTMENT_STYLE_IDS = {
    None: 0,
    "div": 1,
    "add": 2,
}


def _encode_continuous_future_sid(root_symbol, offset, roll_style, adjustment_style):
    # Generate a unique int identifier
    values = (
        SID_TYPE_IDS[ContinuousFuture],
        offset,
        *[ord(x) for x in root_symbol.upper()],
        CONTINUOUS_FUTURE_ROLL_STYLE_IDS[roll_style],
        CONTINUOUS_FUTURE_ADJUSTMENT_STYLE_IDS[adjustment_style],
    )
    return int("".join([str(x) for x in values]))


Lifetimes = namedtuple("Lifetimes", "sid start end")

