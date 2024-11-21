import datetime
from os import environ, _Environ
import logging
import pandas as pd

from exchange_calendars import ExchangeCalendar
from lime_trader.models.market import Period
from zipline.assets import AssetDBWriter
from zipline.data.adjustments import SQLiteAdjustmentWriter
from zipline.data.bcolz_daily_bars import BcolzDailyBarWriter
from zipline.data.bcolz_minute_bars import BcolzMinuteBarWriter
from zipline.utils.cache import dataframe_cache
from zipline.utils.calendar_utils import register_calendar_alias

from ziplime.data.bundles import core as bundles, register
import numpy as np

from ziplime.data.lime_data_provider import LimeDataProvider
from ziplime.utils.calendar_utils import normalize_daily_start_end_session

logger = logging.getLogger(__name__)


def gen_asset_metadata(data: pd.DataFrame, show_progress: bool):
    if show_progress:
        logger.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": ["min", "max"]})
    data.reset_index(inplace=True)
    data["start_date"] = data.date[np.min.__name__]
    data["end_date"] = data.date[np.max.__name__]
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "LIME"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data


def parse_pricing_and_vol(data: pd.DataFrame, sessions: pd.IndexSlice, symbol_map: pd.Series):
    for asset_id, symbol in symbol_map.items():
        asset_data = (
            data.xs(symbol, level=1).reindex(sessions.tz_localize(None)).infer_objects(copy=False).fillna(0.0)
        )
        yield asset_id, asset_data


def create_lime_equities_bundle(
        bundle_name: str,
        period: Period,
        fundamental_data_list: list[str],
        symbol_list: list[str] = None,
):
    def ingest(
            environ: _Environ,
            asset_db_writer: AssetDBWriter,
            minute_bar_writer: BcolzMinuteBarWriter,
            daily_bar_writer: BcolzDailyBarWriter,
            adjustment_writer: SQLiteAdjustmentWriter,
            calendar: ExchangeCalendar,
            start_session: pd.Timestamp,
            end_session: pd.Timestamp,
            cache: dataframe_cache,
            show_progress: bool,
            output_dir: str,
            # period: Period,
            # symbols: list[str],
            **kwargs
    ):
        limex_api_key = environ.get("LIMEX_API_KEY", None)
        lime_sdk_credentials_file = environ.get("LIME_SDK_CREDENTIALS_FILE", None)
        if limex_api_key is None:
            raise ValueError(
                "Please set LIMEX_API_KEY environment variable and retry."
            )
        if lime_sdk_credentials_file is None:
            raise ValueError(
                "Please set LIME_SDK_CREDENTIALS_FILE environment variable and retry."
            )
        logger.info(f"Ingesting equities bundle {bundle_name} for period {start_session} - {end_session}")

        data_provider = LimeDataProvider(limex_api_key=limex_api_key,
                                         lime_sdk_credentials_file=lime_sdk_credentials_file)
        asset_metadata_inserted = False
        for raw_data in data_provider.fetch_data_table(
                symbols=symbol_list,
                period=period,
                date_from=start_session.to_pydatetime().replace(tzinfo=datetime.timezone.utc),
                date_to=end_session.to_pydatetime().replace(tzinfo=datetime.timezone.utc),
                show_progress=show_progress,
                fundamental_data_list=fundamental_data_list
        ):
            if len(raw_data) == 0:
                continue
            asset_metadata = gen_asset_metadata(data=raw_data[["symbol", "date"]], show_progress=show_progress)

            if not asset_metadata_inserted:
                exchanges = pd.DataFrame(
                    data=[["LIME", "LIME", "US"]],
                    columns=["exchange", "canonical_name", "country_code"],
                )
                asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)
                asset_metadata_inserted = True
            symbol_map = asset_metadata.symbol


            raw_data.set_index(["date", "symbol"], inplace=True)
            if period == Period.DAY:
                sessions = calendar.sessions_in_range(start=start_session, end=end_session)
                daily_bar_writer.write(
                    data=parse_pricing_and_vol(data=raw_data, sessions=sessions, symbol_map=symbol_map),
                    show_progress=show_progress,
                )
            elif period == Period.MINUTE:
                sessions = calendar.sessions_minutes(start=start_session, end=end_session)
                minute_bar_writer.write(
                    data=parse_pricing_and_vol(data=raw_data, sessions=sessions, symbol_map=symbol_map),
                    show_progress=show_progress,
                )
            else:
                raise Exception("Unsupported period.")

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
        start_session: datetime.datetime,
        end_session: datetime.datetime,
        symbol_list: list[str],
        period: Period,
        calendar_name: str,
        fundamental_data_list: list[str],
):
    start_session, end_session = normalize_daily_start_end_session(
        calendar_name=calendar_name, start_session=start_session, end_session=end_session
    )
    register(
        name=bundle_name,
        f=create_lime_equities_bundle(
            bundle_name=bundle_name,
            symbol_list=symbol_list,
            fundamental_data_list=fundamental_data_list,
            period=period
        ),
        start_session=start_session,
        end_session=end_session,
        calendar_name=calendar_name,
    )


register_calendar_alias("LIME", "NYSE")


# zipline.data.bcolz_daily_bars.BcolzDailyBarWriter = CustomClass