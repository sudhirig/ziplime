import datetime
import logging
import pandas as pd

from exchange_calendars import ExchangeCalendar
from zipline.assets import AssetDBWriter
from ziplime.data.adjustments import SQLiteAdjustmentWriter
from zipline.utils.cache import dataframe_cache
from zipline.utils.calendar_utils import register_calendar_alias

from ziplime.data.abstract_fundamendal_data_provider import AbstractFundamentalDataProvider
from ziplime.data.abstract_historical_market_data_provider import AbstractHistoricalMarketDataProvider
from ziplime.data.bundles import register
import numpy as np

from ziplime.data.storages.polars_data_bundle import PolarsDataBundle
from ziplime.domain.column_specification import ColumnSpecification
from ziplime.utils.calendar_utils import normalize_daily_start_end_session

logger = logging.getLogger(__name__)


def gen_asset_metadata(data: pd.DataFrame, show_progress: bool):
    if show_progress:
        logger.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": ["min", "max"]})
    data.reset_index(inplace=True)
    start_date = data.date[np.min.__name__].iloc[0].replace(minute=0, second=0, microsecond=0, hour=0)
    end_date = data.date[np.max.__name__].iloc[0].replace(minute=0, second=0, microsecond=0, hour=0)
    data["start_date"] = start_date
    data["end_date"] = end_date
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "LIME"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data


def parse_pricing_and_vol(data: pd.DataFrame, sessions: pd.IndexSlice, symbol_map: pd.Series):
    for asset_id, symbol in symbol_map.items():
        asset_data = (
            data.xs(symbol, level=1).infer_objects(copy=False).fillna(0.0)
        )
        yield asset_id, asset_data


def create_equities_bundle(
        bundle_name: str,
        frequency: datetime.timedelta,
        fundamental_data_list: set[str],
        symbol_list: list[str] = None,
):
    def ingest(
            historical_market_data_provider: AbstractHistoricalMarketDataProvider,
            fundamental_data_provider: AbstractFundamentalDataProvider,
            asset_db_writer: AssetDBWriter,
            data_bundle_writer: PolarsDataBundle,
            fundamental_data_writer: PolarsDataBundle,
            adjustment_writer: SQLiteAdjustmentWriter,
            calendar: ExchangeCalendar,
            start_session: pd.Timestamp,
            end_session: pd.Timestamp,
            cache: dataframe_cache,
            show_progress: bool,
            market_data_fields: list[ColumnSpecification],
            fundamental_data_fields: list[ColumnSpecification],
            output_dir: str,
            **kwargs
    ):
        date_from = start_session.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
        date_to = end_session.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
        logger.info(f"Ingesting equities bundle {bundle_name} for period {start_session} - {end_session}")
        historical_data = historical_market_data_provider.get_historical_data_table(
            symbols=symbol_list,
            frequency=frequency,
            date_from=date_from,
            date_to=date_to,
            show_progress=show_progress,
            exchange_calendar=calendar)

        if len(historical_data) == 0:
            logger.warning(
                f"Data source {type(historical_market_data_provider)} didn't return any data for symbols {symbol_list}")
            return

        asset_metadata = gen_asset_metadata(data=historical_data[["symbol", "date"]], show_progress=show_progress)

        historical_data.set_index(["date", "symbol"], inplace=True)

        # historical_data = pd.concat([historical_data, fundamental_data], ignore_index=False, axis=1)
        # final_df = final_df[final_df.date.notnull()]

        exchanges = pd.DataFrame(
            data=[["LIME", "LIME", "US"]],
            columns=["exchange", "canonical_name", "country_code"],
        )

        asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)

        symbol_map = asset_metadata.symbol
        sessions = calendar.sessions_in_range(start=start_session, end=end_session)

        fundamental_data = fundamental_data_provider.get_fundamental_data(symbols=symbol_list,
                                                                          frequency=frequency,
                                                                          date_from=date_from, date_to=date_to,
                                                                          fundamental_data_list=fundamental_data_list)

        fundamental_data_writer.write(
            data=parse_pricing_and_vol(data=fundamental_data, sessions=sessions, symbol_map=symbol_map),
            show_progress=show_progress,
            calendar=calendar,
            start_session=start_session,
            end_session=end_session,
            cols=fundamental_data_fields,
            validate_sessions=False,
            frequency=frequency
        )

        data_bundle_writer.write(
            data=parse_pricing_and_vol(data=historical_data, sessions=sessions, symbol_map=symbol_map),
            show_progress=show_progress,
            calendar=calendar,
            start_session=start_session,
            end_session=end_session,
            cols=market_data_fields,
            validate_sessions=False,
            frequency=frequency,
        )

        # Write empty splits and divs - they are not present in API
        divs_splits = {
            "divs": pd.DataFrame(
                columns=[
                    "sid",
                    "amount",
                    "ex_date",
                    "record_date",
                    "declared_date",
                    "pay_date",
                ]
            ),
            "splits": pd.DataFrame(columns=["sid", "ratio", "effective_date"]),
        }
        adjustment_writer.write(
            splits=divs_splits["splits"], dividends=divs_splits["divs"]
        )
        logger.info(
            f"Ingesting equities bundle {bundle_name} for period {start_session} - {end_session} "
            f"and symbols {symbol_list} Completed"
        )

    return ingest


def register_lime_equities_bundle(
        bundle_name: str,
        start_session: datetime.datetime | None,
        end_session: datetime.datetime | None,
        symbol_list: list[str],
        frequency: datetime.timedelta,
        calendar_name: str,
        fundamental_data_list: set[str],
):
    if start_session and end_session:
        start_session, end_session = normalize_daily_start_end_session(
            calendar_name=calendar_name, start_session=start_session, end_session=end_session
        )
    register(
        name=bundle_name,
        f=create_equities_bundle(
            bundle_name=bundle_name,
            symbol_list=symbol_list,
            fundamental_data_list=fundamental_data_list,
            frequency=frequency
        ),
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
    )


register_calendar_alias("LIME", "NYSE")
