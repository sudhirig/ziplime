import datetime
from dataclasses import dataclass
from functools import reduce
from operator import mul
from typing import Any

import pandas as pd
import polars as pl
from exchange_calendars import ExchangeCalendar

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.domain.db.asset import Asset
from ziplime.assets.domain.db.equity import Equity
from ziplime.assets.repositories.adjustments_repository import AdjustmentRepository
from ziplime.assets.repositories.asset_repository import AssetRepository
from ziplime.data.services.bundle_data_source import BundleDataSource


@dataclass
class BundleData:
    name: str
    version: str

    start_date: datetime.date
    end_date: datetime.date
    trading_calendar: ExchangeCalendar
    frequency: datetime.timedelta
    timestamp: datetime.datetime
    asset_repository: AssetRepository
    adjustment_repository: AdjustmentRepository

    data: pl.DataFrame

    missing_bundle_data_source: BundleDataSource | None = None

    def get_dataframe(self) -> pl.DataFrame:
        df = self.data
        return df

    def get_data_by_date(self, fields: list[str],
                         from_date: datetime.datetime,
                         to_date: datetime.datetime,
                         frequency: datetime.timedelta,
                         assets: list[Asset],
                         include_bounds: bool,
                         ) -> pl.DataFrame:

        cols = list(set(fields + ["date", "sid"]))
        if include_bounds:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= to_date,
                pl.col("date") >= from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).all().sort(by="date")
        else:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < to_date,
                pl.col("date") > from_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).all().sort(by="date")
        if self.frequency < frequency:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
            return df
        return df_raw

    def get_missing_data_by_limit(self, fields: list[str],
                                  limit: int,
                                  end_date: datetime.datetime,
                                  frequency: datetime.timedelta,
                                  assets: list[Asset],
                                  include_end_date: bool,
                                  ) -> pl.DataFrame:

        return self.missing_bundle_data_source.get_data_sync(
            symbols=[asset.get_symbol_by_exchange(None) for asset in assets], frequency=frequency,
            date_from=end_date - frequency * limit,
            date_to=end_date)

    def get_data_by_limit(self, fields: list[str],
                          limit: int,
                          end_date: datetime.datetime,
                          frequency: datetime.timedelta,
                          assets: list[Asset],
                          include_end_date: bool,
                          ) -> pl.DataFrame:

        total_bar_count = limit
        if end_date > self.end_date:
            return self.get_missing_data_by_limit(frequency=frequency, assets=assets, fields=fields,
                                                  limit=limit, include_end_date=include_end_date,
                                                  end_date=end_date
                                                  )  # pl.DataFrame() # we have missing data
        if self.frequency < frequency:
            multiplier = int(frequency / self.frequency)
            total_bar_count = limit * multiplier

        cols = list(set(fields + ["date", "sid"]))
        if include_end_date:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") <= end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])
            ).group_by(pl.col("sid")).tail(total_bar_count).sort(by="date")
        else:
            df_raw = self.get_dataframe().select(pl.col(col) for col in cols).filter(
                pl.col("date") < end_date,
                pl.col("sid").is_in([asset.sid for asset in assets])).group_by(pl.col("sid")).tail(
                total_bar_count).sort(by="date")
        if self.frequency < frequency:
            df = df_raw.group_by_dynamic(
                index_column="date", every=frequency, by="sid").agg(pl.col(field).last() for field in fields)
            return df
        return df_raw

    def get_scalar_asset_spot_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                    frequency: datetime.timedelta):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset
            The asset or assets whose data is desired. This cannot be
            an arbitrary AssetConvertible.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        return self.get_spot_value(
            assets=[asset],
            fields=[field],
            dt=dt,
            frequency=frequency,
        )
        # return self._get_single_asset_value(
        #     asset=asset,
        #     field=field,
        #     dt=dt,
        #     frequency=frequency,
        # )

    def _get_single_asset_value(self, asset: Asset, field: str, dt: datetime.datetime,
                                frequency: datetime.timedelta) -> pl.DataFrame:

        return self.get_spot_value(
            assets=[asset],
            fields=[field],
            dt=dt,
            frequency=frequency,
        )
        # if field not in self._fields:
        #     raise KeyError("Invalid column: " + str(field))
        #
        # if (
        #         dt < asset.start_date
        #         or (
        #         data_frequency == "daily" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
        #     asset.end_date, tzinfo=datetime.timezone.utc))
        #         or (
        #         data_frequency == "minute" and add_tz_info(session_label, tzinfo=datetime.timezone.utc) > add_tz_info(
        #     asset.end_date, tzinfo=datetime.timezone.utc))
        # ):
        #     if field == "volume":
        #         return 0
        #     elif field == "contract":
        #         return None
        #     elif field != "last_traded":
        #         return np.nan
        #
        # if data_frequency == "daily":
        #     if field == "contract":
        #         return self._get_current_contract(continuous_future=asset, dt=session_label)
        #     else:
        #         return self._get_daily_spot_value(
        #             asset=asset,
        #             column=field,
        #             dt=session_label,
        #         )
        # else:
        #     if field == "last_traded":
        #         return self.get_last_traded_dt(asset, dt, "minute")
        #     elif field == "price":
        #         return self._get_minute_spot_value(
        #             asset=asset,
        #             column="close",
        #             dt=dt,
        #             ffill=True,
        #         )
        #     elif field == "contract":
        #         return self._get_current_contract(continuous_future=asset, dt=dt)
        #     else:
        #         return self._get_minute_spot_value(asset=asset, column=field, dt=dt)

    def get_spot_value(self, assets: list[Asset], fields: list[str], dt: datetime.datetime,
                       frequency: datetime.timedelta):
        """Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset, ContinuousFuture, or iterable of same.
            The asset or assets whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        df_raw = self.get_data_by_limit(
            fields=fields,
            limit=1,
            end_date=dt,
            frequency=frequency,
            assets=assets,
            include_end_date=True,
        )
        return df_raw

    def get_adjusted_value(
            self, asset: Asset, field: str, dt: datetime.datetime, perspective_dt: datetime.datetime,
            data_frequency: datetime.timedelta,
            spot_value: float = None
    ):
        """Returns a scalar value representing the value
        of the desired asset's field at the given dt with adjustments applied.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or datetime.datetime
            The value of the given ``field`` for ``asset`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open',
            'high', 'low', 'close', or 'price', the value will be a float. If
            the ``field`` is 'volume' the value will be a int. If the ``field``
            is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            spot_value = self.get_spot_value(assets=[asset], fields=[field], dt=dt, data_frequency=data_frequency)

        if isinstance(asset, Equity):  # TODO: fix this, not valid way to check if it is equity
            ratio = self.get_adjustments(assets=[asset], field=field, dt=dt, perspective_dt=perspective_dt)[0]
            spot_value *= ratio

        return spot_value

    def _get_adjustment_list(self, asset: Asset, adjustments_dict: dict[str, Any], table_name: str):
        """Internal method that returns a list of adjustments for the given sid.

        Parameters
        ----------
        asset : Asset
            The asset for which to return adjustments.

        adjustments_dict: dict
            A dictionary of sid -> list that is used as a cache.

        table_name: string
            The table that contains this data in the adjustments db.

        Returns
        -------
        adjustments: list
            A list of [multiplier, datetime.datetime], earliest first

        """
        if self.adjustment_repository is None:
            return []

        sid = asset.sid

        try:
            adjustments = adjustments_dict[sid]
        except KeyError:
            adjustments = adjustments_dict[
                sid
            ] = self.adjustment_repository.get_adjustments_for_sid(table_name, sid)

        return adjustments

    def get_splits(self, assets: list[Asset], dt: datetime.date):
        """Returns any splits for the given sids and the given dt.

        Parameters
        ----------
        assets : container
            Assets for which we want splits.
        dt : datetime.datetime
            The date for which we are checking for splits. Note: this is
            expected to be midnight UTC.

        Returns
        -------
        splits : list[(asset, float)]
            List of splits, where each split is a (asset, ratio) tuple.
        """
        if self.adjustment_repository is None or not assets:
            return []

        # convert dt to # of seconds since epoch, because that's what we use
        # in the adjustments db
        # seconds = int(dt.value / 1e9)

        splits = self.adjustment_repository.conn.execute(
            "SELECT sid, ratio FROM SPLITS WHERE effective_date = ?", (dt,)
        ).fetchall()

        splits = [split for split in splits if split[0] in assets]
        splits = [
            (self.asset_repository.retrieve_asset(split[0]), split[1]) for split in splits
        ]

        return splits

    def get_stock_dividends(self, sid: int, trading_days: pd.DatetimeIndex):
        """Returns all the stock dividends for a specific sid that occur
        in the given trading range.

        Parameters
        ----------
        sid: int
            The asset whose stock dividends should be returned.

        trading_days: pd.DatetimeIndex
            The trading range.

        Returns
        -------
        list: A list of objects with all relevant attributes populated.
        All timestamp fields are converted to datetime.datetime.
        """

        if self._adjustment_reader is None:
            return []

        if len(trading_days) == 0:
            return []

        start_dt = trading_days[0]
        end_dt = trading_days[-1]

        dividends = self._adjustment_reader.conn.execute(
            "SELECT declared_date, ex_date, pay_date, payment_sid, ratio, "
            "record_date, sid FROM stock_dividend_payouts "
            "WHERE sid = ? AND ex_date > ? AND pay_date < ?",
            (
                int(sid),
                start_dt,
                end_dt,
            ),
        ).fetchall()

        dividend_info = []
        for dividend_tuple in dividends:
            dividend_info.append(
                {
                    "declared_date": pd.Timestamp(dividend_tuple[0], unit="s").to_pydatetime(),
                    "ex_date": pd.Timestamp(dividend_tuple[1], unit="s").to_pydatetime(),
                    "pay_date": pd.Timestamp(dividend_tuple[2], unit="s").to_pydatetime(),
                    "payment_sid": dividend_tuple[3],
                    "ratio": dividend_tuple[4],
                    "record_date": pd.Timestamp(dividend_tuple[5], unit="s").to_pydatetime(),
                    "sid": dividend_tuple[6],
                }
            )

        return dividend_info

    def get_current_future_chain(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        """Retrieves the future chain for the contract at the given `dt` according
        the `continuous_future` specification.

        Returns
        -------

        future_chain : list[Future]
            A list of active futures, where the first index is the current
            contract specified by the continuous future definition, the second
            is the next upcoming contract and so on.
        """
        rf = self._roll_finders[continuous_future.roll_style]
        session = self.trading_calendar.minute_to_session(dt)
        contract_center = rf.get_contract_center(
            continuous_future.root_symbol, session, continuous_future.offset
        )
        oc = self.asset_repository.get_ordered_contracts(continuous_future.root_symbol)
        chain = oc.active_chain(contract_center, session.value)
        return self.asset_repository.retrieve_all(sids=chain)

    def _get_current_contract(self, continuous_future: ContinuousFuture, dt: datetime.datetime):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_sid = rf.get_contract_center(
            continuous_future.root_symbol, dt, continuous_future.offset
        )
        if contract_sid is None:
            return None
        return self.asset_repository.retrieve_asset(sid=contract_sid)

    def get_adjustments(self, assets: list[Asset], field: str, dt: datetime.datetime,
                        perspective_dt: datetime.datetime):
        """Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of assets

        Parameters
        ----------
        assets : list of type Asset, or Asset
            The asset, or assets whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the asset.
        dt : datetime.datetime
            The timestamp for the desired value.
        perspective_dt : datetime.datetime
            The timestamp from which the data is being viewed back from.

        Returns
        -------
        adjustments : list[Adjustment]
            The adjustments to that field.
        """
        adjustment_ratios_per_asset = []

        def split_adj_factor(x):
            return x if field != "volume" else 1.0 / x

        for asset in assets:
            adjustments_for_asset = []
            split_adjustments = self._get_adjustment_list(
                asset, self._splits_dict, "SPLITS"
            )
            for adj_dt, adj in split_adjustments:
                if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                    adjustments_for_asset.append(split_adj_factor(adj))
                elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                    break

            if field != "volume":
                merger_adjustments = self._get_adjustment_list(
                    asset, self._mergers_dict, "MERGERS"
                )
                for adj_dt, adj in merger_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt > perspective_dt:
                        break

                dividend_adjustments = self._get_adjustment_list(
                    asset,
                    self._dividends_dict,
                    "DIVIDENDS",
                )
                for adj_dt, adj in dividend_adjustments:
                    if dt < adj_dt.tz_localize(dt.tzinfo) <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt.tz_localize(dt.tzinfo) > perspective_dt:
                        break

            ratio = reduce(mul, adjustments_for_asset, 1.0)
            adjustment_ratios_per_asset.append(ratio)

        return adjustment_ratios_per_asset
