import warnings
from dataclasses import asdict

import numpy
from exchange_calendars import ExchangeCalendar
from numpy import zeros
from zipline.data._equities import _compute_row_slices

from ziplime.data.abstract_data_bundle import AbstractDataBundle
from ziplime.domain.column_specification import ColumnSpecification

with warnings.catch_warnings():  # noqa
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    from bcolz import carray, ctable
    import numpy as np

import logging

import pandas as pd

from zipline.data.bar_reader import NoDataAfterDate, NoDataBeforeDate, NoDataOnDate
from zipline.utils.calendar_utils import get_calendar
from zipline.utils.cli import maybe_show_progress
from zipline.utils.functional import apply
from zipline.utils.input_validation import expect_element
from zipline.utils.memoize import lazyval
from zipline.utils.numpy_utils import iNaT

logger = logging.getLogger("UsEquityPricing")


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


class BcolzDataBundle(AbstractDataBundle):
    """Class capable of writing daily OHLCV data to disk in a format that can
    be read efficiently by BcolzDailyOHLCVReader.

    Parameters
    ----------
    root_directory : str
        The location at which we should write our output.
    """

    def __init__(self, root_directory: str):
        super().__init__()
        self._root_directory = root_directory
        # Cache of fully read np.array for the carrays in the daily bar table.
        # raw_array does not use the same cache, but it could.
        # Need to test keeping the entire array in memory for the course of a
        # process first.
        self._spot_cols = {}

    @lazyval
    def _table(self) -> ctable:
        return ctable(rootdir=self._root_directory, mode="r")

    @lazyval
    def sessions(self):
        if "calendar" in self._table.attrs.attrs:
            # backwards compatibility with old formats, will remove
            return pd.DatetimeIndex(self._table.attrs["calendar"])
        else:
            cal = get_calendar(self._table.attrs["calendar_name"])
            start_session_ns = self._table.attrs["start_session_ns"]

            start_session = pd.Timestamp(start_session_ns)

            end_session_ns = self._table.attrs["end_session_ns"]
            end_session = pd.Timestamp(end_session_ns)

            sessions = cal.sessions_in_range(start_session, end_session)

            return sessions

    @lazyval
    def _first_rows(self):
        return {
            int(asset_id): start_index
            for asset_id, start_index in self._table.attrs["first_row"].items()
        }

    @lazyval
    def _last_rows(self):
        return {
            int(asset_id): end_index
            for asset_id, end_index in self._table.attrs["last_row"].items()
        }

    @lazyval
    def _calendar_offsets(self):
        return {
            int(id_): offset
            for id_, offset in self._table.attrs["calendar_offset"].items()
        }

    @lazyval
    def first_trading_day(self):
        try:
            return pd.Timestamp(self._table.attrs["first_trading_day"], unit="s")
        except KeyError:
            return None

    @lazyval
    def trading_calendar(self):
        if "calendar_name" in self._table.attrs.attrs:
            return get_calendar(self._table.attrs["calendar_name"])
        else:
            return None

    @property
    def last_available_dt(self):
        return self.sessions[-1]

    def _compute_slices(self, start_idx, end_idx, assets):
        """Compute the raw row indices to load for each asset on a query for the
        given dates after applying a shift.

        Parameters
        ----------
        start_idx : int
            Index of first date for which we want data.
        end_idx : int
            Index of last date for which we want data.
        assets : pandas.Int64Index
            Assets for which we want to compute row indices

        Returns
        -------
        A 3-tuple of (first_rows, last_rows, offsets):
        first_rows : np.array[intp]
            Array with length == len(assets) containing the index of the first
            row to load for each asset in `assets`.
        last_rows : np.array[intp]
            Array with length == len(assets) containing the index of the last
            row to load for each asset in `assets`.
        offset : np.array[intp]
            Array with length == (len(asset) containing the index in a buffer
            of length `dates` corresponding to the first row of each asset.

            The value of offset[i] will be 0 if asset[i] existed at the start
            of a query.  Otherwise, offset[i] will be equal to the number of
            entries in `dates` for which the asset did not yet exist.
        """
        # The core implementation of the logic here is implemented in Cython
        # for efficiency.
        return _compute_row_slices(
            self._first_rows,
            self._last_rows,
            self._calendar_offsets,
            start_idx,
            end_idx,
            assets,
        )

    @lazyval
    def _column_specification(self):
        cols = [
            ColumnSpecification(
                name=col["name"],
                original_type=col["original_type"],
                write_type=col["write_type"],
                scaled_type=col["scaled_type"],
                scale_factor=col["scale_factor"]
            )
            for col in self._table.attrs["column_specification"]
        ]
        return {
            col.name: col
            for col in cols
        }

    def load_raw_arrays(self, columns, start_date, end_date, assets):
        start_idx = self._load_raw_arrays_date_to_index(start_date)
        end_idx = self._load_raw_arrays_date_to_index(end_date)

        first_rows, last_rows, offsets = self._compute_slices(
            start_idx,
            end_idx,
            assets,
        )
        read_all = True
        return self._read_bcolz_data(
            self._table,
            (end_idx - start_idx + 1, len(assets)),
            list(columns),
            first_rows,
            last_rows,
            offsets,
            read_all,
        )
    def load_raw_arrays_full_range(self, columns, start_date, end_date, assets, frequency):
        # start_idx = self._load_raw_arrays_date_to_index(start_date)
        # end_idx = self._load_raw_arrays_date_to_index(end_date)
        dr = pd.date_range(pd.to_datetime(self._table.attrs["first_trading_day"], unit="s"), end_date, freq=frequency)
        start_idx = dr.get_loc(start_date)
        end_idx = dr.get_loc(end_date)
        first_rows, last_rows, offsets = self._compute_slices(
            start_idx,
            end_idx,
            assets,
        )
        read_all = True
        return self._read_bcolz_data(
            self._table,
            (end_idx - start_idx + 1, len(assets)),
            list(columns),
            first_rows,
            last_rows,
            offsets,
            read_all,
        )

    def _load_raw_arrays_date_to_index(self, date):
        try:
            # TODO get_loc is deprecated but get_indexer doesnt raise and error
            return self.sessions.get_loc(date)
        except KeyError as exc:
            raise NoDataOnDate(date) from exc

    def _spot_col(self, colname):
        """Get the colname from daily_bar_table and read all of it into memory,
        caching the result.

        Parameters
        ----------
        colname : string
            A name of a OHLCV carray in the daily_bar_table

        Returns
        -------
        array (uint32)
            Full read array of the carray in the daily_bar_table with the
            given colname.
        """
        try:
            col = self._spot_cols[colname]
        except KeyError:
            col = self._spot_cols[colname] = self._table[colname]
        return col

    def get_last_traded_dt(self, asset, day):
        volumes = self._spot_col("volume")

        search_day = day

        while True:
            try:
                ix = self.sid_day_index(asset, search_day)
            except NoDataBeforeDate:
                return pd.NaT
            except NoDataAfterDate:
                prev_day_ix = self.sessions.get_loc(search_day) - 1
                if prev_day_ix > -1:
                    search_day = self.sessions[prev_day_ix]
                continue
            except NoDataOnDate:
                return pd.NaT
            if volumes[ix] != 0:
                return search_day
            prev_day_ix = self.sessions.get_loc(search_day) - 1
            if prev_day_ix > -1:
                search_day = self.sessions[prev_day_ix]
            else:
                return pd.NaT

    def sid_day_index(self, sid, day):
        """

        Parameters
        ----------
        sid : int
            The asset identifier.
        day : datetime64-like
            Midnight of the day for which data is requested.

        Returns
        -------
        int
            Index into the data tape for the given sid and day.
            Raises a NoDataOnDate exception if the given day and sid is before
            or after the date range of the equity.
        """
        try:
            day_loc = self.sessions.get_loc(day)
        except Exception as exc:
            raise NoDataOnDate(
                "day={0} is outside of calendar={1}".format(day, self.sessions)
            ) from exc
        offset = day_loc - self._calendar_offsets[sid]
        if offset < 0:
            raise NoDataBeforeDate(
                "No data on or before day={0} for sid={1}".format(day, sid)
            )
        ix = self._first_rows[sid] + offset
        if ix > self._last_rows[sid]:
            raise NoDataAfterDate(
                "No data on or after day={0} for sid={1}".format(day, sid)
            )
        return ix

    def get_value(self, sid, dt, field):
        """

        Parameters
        ----------
        sid : int
            The asset identifier.
        day : datetime64-like
            Midnight of the day for which data is requested.
        colname : string
            The price field. e.g. ('open', 'high', 'low', 'close', 'volume')

        Returns
        -------
        float
            The spot price for colname of the given sid on the given day.
            Raises a NoDataOnDate exception if the given day and sid is before
            or after the date range of the equity.
            Returns -1 if the day is within the date range, but the price is
            0.
        """
        ix = self.sid_day_index(sid, dt)
        price = self._spot_col(field)[ix]
        if field != "volume":
            if price == 0:
                return np.nan
            else:
                return price * 0.001
        else:
            return price

    def currency_codes(self, sids):
        # XXX: This is pretty inefficient. This reader doesn't really support
        # country codes, so we always either return USD or None if we don't
        # know about the sid at all.
        first_rows = self._first_rows
        out = []
        for sid in sids:
            if sid in first_rows:
                out.append("USD")
            else:
                out.append(None)
        return np.array(out, dtype=object)

    def _read_bcolz_data(self, table, shape, columns, first_rows, last_rows, offsets, read_all):
        """
        Load raw bcolz data for the given columns and indices.

        Parameters
        ----------
        table : bcolz.ctable
            The table from which to read.
        shape : tuple (length 2)
            The shape of the expected output arrays.
        columns : list[str]
            List of column names to read.

        first_rows : ndarray[intp]
        last_rows : ndarray[intp]
        offsets : ndarray[intp
            Arrays in the format returned by _compute_row_slices.
        read_all : bool
            Whether to read_all sid data at once, or to read a silce from the
            carray for each sid.

        Returns
        -------
        results : list of ndarray
            A 2D array of shape `shape` for each column in `columns`.
        """
        results = []
        ndays = shape[0]
        nassets = shape[1]
        if not nassets == len(first_rows) == len(last_rows) == len(offsets):
            raise ValueError("Incompatible index arrays.")
        for column_name in columns:
            col = self._column_specification[column_name]
            outbuf = zeros(shape=shape, dtype=col.write_type)
            if read_all:
                raw_data = table[column_name][:]

                for asset in range(nassets):
                    first_row = first_rows[asset]
                    last_row = last_rows[asset]
                    offset = offsets[asset]
                    if first_row <= last_row:
                        outbuf[offset:offset + (last_row + 1 - first_row), asset] = \
                            raw_data[first_row:last_row + 1]
                    else:
                        continue
            else:
                carray = table[column_name]

                for asset in range(nassets):
                    first_row = first_rows[asset]
                    last_row = last_rows[asset]
                    offset = offsets[asset]
                    out_start = offset
                    out_end = (last_row - first_row) + offset + 1
                    if first_row <= last_row:
                        outbuf[offset:offset + (last_row + 1 - first_row), asset] = \
                            carray[first_row:last_row + 1]
                    else:
                        continue

            # if column_name in {'open', 'high', 'low', 'close'}:
            if col.write_type != col.original_type:
                where_nan = (outbuf == 0)
                outbuf_as_float = outbuf.astype(col.original_type) * 1 / col.scale_factor
                outbuf_as_float[where_nan] = numpy.nan
                results.append(outbuf_as_float)
            else:
                results.append(outbuf)
        return results

    ############## WRITE #########################
    @property
    def progress_bar_message(self):
        return "Merging daily equity files:"

    def progress_bar_item_show_func(self, value):
        return value if value is None else str(value[0])

    def write(
            self, data, calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
            cols: list[ColumnSpecification],
            validate_sessions: bool,
            assets=None, show_progress=False, invalid_data_behavior="warn",
            **kwargs
    ):
        """

        Parameters
        ----------
        data : iterable[tuple[int, pandas.DataFrame or bcolz.ctable]]
            The data chunks to write. Each chunk should be a tuple of sid
            and the data for that asset.
        assets : set[int], optional
            The assets that should be in ``data``. If this is provided
            we will check ``data`` against the assets and provide better
            progress information.
        show_progress : bool, optional
            Whether or not to show a progress bar while writing.
        invalid_data_behavior : {'warn', 'raise', 'ignore'}, optional
            What to do when data is encountered that is outside the range of
            a uint32.

        Returns
        -------
        table : bcolz.ctable
            The newly-written table.
        """
        ctx = maybe_show_progress(
            ((sid, self.to_ctable(df, invalid_data_behavior, cols)) for sid, df in data),
            show_progress=show_progress,
            item_show_func=self.progress_bar_item_show_func,
            label=self.progress_bar_message,
            length=len(assets) if assets is not None else None,
        )
        with ctx as it:
            return self._write_internal(iterator=it, assets=assets, calendar=calendar,
                                        start_session=start_session, end_session=end_session,
                                        cols=cols, show_progress=show_progress,
                                        validate_sessions=validate_sessions
                                        )

    def _write_internal(self, iterator, assets,
                        calendar: ExchangeCalendar, start_session: pd.Timestamp, end_session: pd.Timestamp,
                        show_progress,
                        cols: list[ColumnSpecification],
                        validate_sessions: bool
                        ):
        """Internal implementation of write.

        `iterator` should be an iterator yielding pairs of (asset, ctable).
        """
        total_rows = 0
        first_row = {}
        last_row = {}
        calendar_offset = {}

        # Maps column name -> output carray.
        columns = {
            k.name: carray(np.array([], dtype=np.dtype(k.write_type)))
            for k in cols  # US_EQUITY_PRICING_BCOLZ_COLUMNS
        }

        earliest_date = None
        sessions = calendar.sessions_in_range(
            start=start_session, end=end_session
        )

        if assets is not None:

            @apply
            def iterator(iterator=iterator, assets=set(assets)):
                for asset_id, table in iterator:
                    if asset_id not in assets:
                        raise ValueError("unknown asset id %r" % asset_id)
                    yield asset_id, table

        for asset_id, table in iterator:
            nrows = len(table)
            for column_name in columns:
                if column_name == "id":
                    # We know what the content of this column is, so don't
                    # bother reading it.
                    columns["id"].append(
                        np.full((nrows,), asset_id, dtype="uint32"),
                    )
                    continue

                columns[column_name].append(table[column_name])

            if earliest_date is None:
                earliest_date = table["day"][0]
            else:
                earliest_date = min(earliest_date, table["day"][0])

            # Bcolz doesn't support ints as keys in `attrs`, so convert
            # assets to strings for use as attr keys.
            asset_key = str(asset_id)

            # Calculate the index into the array of the first and last row
            # for this asset. This allows us to efficiently load single
            # assets when querying the data back out of the table.
            first_row[asset_key] = total_rows
            last_row[asset_key] = total_rows + nrows - 1
            total_rows += nrows

            asset_first_day = pd.Timestamp(table["day"][0], unit="s").normalize()
            asset_last_day = pd.Timestamp(table["day"][-1], unit="s").normalize()

            asset_sessions = sessions[
                sessions.slice_indexer(asset_first_day, asset_last_day)
            ]
            if validate_sessions and len(table) != len(asset_sessions):
                missing_sessions = asset_sessions.difference(
                    pd.to_datetime(np.array(table["day"]), unit="s")
                ).tolist()

                extra_sessions = (
                    pd.to_datetime(np.array(table["day"]), unit="s")
                    .difference(asset_sessions)
                    .tolist()
                )
                raise AssertionError(
                    f"Got {len(table)} rows for daily bars table with "
                    f"first day={asset_first_day.date()}, last "
                    f"day={asset_last_day.date()}, expected {len(asset_sessions)} rows.\n"
                    f"Missing sessions: {missing_sessions}\nExtra sessions: {extra_sessions}"
                )

            # Calculate the number of trading days between the first date
            # in the stored data and the first date of **this** asset. This
            # offset used for output alignment by the reader.
            calendar_offset[asset_key] = sessions.get_loc(asset_first_day)

        # This writes the table to disk.
        full_table = ctable(
            columns=[columns[col.name] for col in cols],
            names=[col.name for col in cols],
            rootdir=self._root_directory,
            mode="w",
        )

        full_table.attrs["first_trading_day"] = (
            earliest_date if earliest_date is not None else iNaT
        )

        full_table.attrs["first_row"] = first_row
        full_table.attrs["last_row"] = last_row
        full_table.attrs["calendar_offset"] = calendar_offset
        full_table.attrs["calendar_name"] = calendar.name
        full_table.attrs["start_session_ns"] = start_session.value
        full_table.attrs["end_session_ns"] = end_session.value
        full_table.attrs["column_specification"] = [
            asdict(col) for col in cols
        ]
        full_table.flush()
        return full_table

    @expect_element(invalid_data_behavior={"warn", "raise", "ignore"})
    def to_ctable(self, raw_data, invalid_data_behavior, cols):
        if isinstance(raw_data, ctable):
            # we already have a ctable so do nothing
            return raw_data
        data = raw_data.copy()
        for col in cols:
            if col.write_type != col.original_type:
                data[col.name] = (raw_data[col.name] * col.scale_factor).round().astype(col.scaled_type).astype(
                    col.write_type)
        dates = data.index.values.astype("datetime64[s]")
        check_uint32_safe(dates.max().view(np.int64), "day")
        data["day"] = dates.astype("uint32")
        return ctable.fromdataframe(data)
