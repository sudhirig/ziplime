import datetime
from typing import Any

import polars as pl
import structlog
from exchange_calendars import ExchangeCalendar, get_calendar

from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.period import Period
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.data.services.bundle_registry import BundleRegistry
from ziplime.data.services.bundle_storage import BundleStorage
from ziplime.utils.class_utils import load_class


class BundleService:

    def __init__(self, bundle_registry: BundleRegistry):
        self._bundle_registry = bundle_registry
        self._logger = structlog.get_logger(__name__)

    async def list_bundles(self) -> list[dict[str, Any]]:
        return await self._bundle_registry.list_bundles()

    async def ingest_custom_data_bundle(self, name: str,
                                        bundle_version: str,
                                        date_start: datetime.datetime,
                                        date_end: datetime.datetime,
                                        trading_calendar: ExchangeCalendar,
                                        symbols: list[str],
                                        data_bundle_source: DataBundleSource,
                                        frequency: datetime.timedelta | Period,
                                        data_frequency_use_window_end: bool,
                                        bundle_storage: BundleStorage,
                                        asset_service: AssetService,
                                        ):

        """Ingest data for a given bundle.        """
        self._logger.info(f"Ingesting custom bundle: name={name}, date_start={date_start}, date_end={date_end}, "
                          f"symbols={symbols}, frequency={frequency}")
        if date_start < trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date start must be after first session of trading calendar. "
                f"First session is {trading_calendar.first_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date start is {date_start}")

        if date_end > trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date end must be before last session of trading calendar. "
                f"Last session is {trading_calendar.last_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date end is {date_end}")

        data = await data_bundle_source.get_data(
            symbols=symbols,
            frequency=frequency,
            date_from=date_start,
            date_to=date_end
        )

        if data.is_empty():
            self._logger.warning(
                f"No data for symbols={symbols}, frequency={frequency}, date_start={date_start},"
                f"date_end={date_end} found. Skipping ingestion."
            )
            return

        # repair data
        all_bars = [
            s for s in pl.from_pandas(
                trading_calendar.sessions_minutes(start=date_start.replace(tzinfo=None),
                                                  end=date_end.replace(tzinfo=None)).tz_convert(trading_calendar.tz)
            ) if s >= date_start and s <= date_end
        ]

        required_sessions = pl.DataFrame({"date": all_bars}).group_by_dynamic(
            index_column="date", every=frequency
        ).agg()
        if data_frequency_use_window_end:
            if (
                    (type(frequency) is datetime.timedelta and frequency >=datetime.timedelta(days=1)) or
                    (type(frequency) is str and frequency in ["1d", "1w", "1mo", "1q", "1y"])
            ):
                last_row = required_sessions.tail(1).with_columns(pl.col("date").dt.offset_by(frequency) - pl.duration(days=1))
                required_sessions = required_sessions.with_columns(
                    pl.col("date") - pl.duration(days=1)
                )[1:]

                required_sessions = pl.concat([required_sessions, last_row])
        required_columns = [
            "date"
        ]
        missing = [c for c in required_columns if c not in data.columns]

        if missing:
            raise ValueError(f"Ingested data is missing required columns: {missing}. Cannot ingest bundle.")
        if "symbol" not in data.columns and "sid" not in data.columns:
            raise ValueError(f"When ingesting custom bundle you must supply either a symbol or a sid column.")


        sid_id = "sid" in data.columns
        symbol_id = "symbol" in data.columns

        asset_identifiers = list(data["sid"].unique()) if sid_id else list(data["symbol"].unique())

        if sid_id:
            data = await self._backfill_symbol_data(data=data, asset_service=asset_service,
                                                    required_sessions=required_sessions)
        else:
            data = await self._backfill_sid_data(data=data, asset_service=asset_service,
                                                 required_sessions=required_sessions)

        data_bundle = DataBundle(name=name,
                                 start_date=date_start,
                                 end_date=date_end,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency,
                                 data=data,
                                 timestamp=datetime.datetime.now(tz=trading_calendar.tz),
                                 version=bundle_version
                                 )

        await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage)
        await bundle_storage.store_bundle(data_bundle=data_bundle)

        self._logger.info(f"Finished ingesting custom bundle_name={name}, bundle_version={bundle_version}")

    async def _backfill_sid_data(self, data: pl.DataFrame, asset_service: AssetService, required_sessions: pl.Series):

        unique_symbols = list(data["symbol"].unique())
        symbol_to_sid = {a.get_symbol_by_exchange(exchange_name=None): a.sid for a in
                         await asset_service.get_equities_by_symbols(unique_symbols)}
        data = data.with_columns(
            pl.lit(0).alias("sid"),
        )

        for symbol in unique_symbols:
            symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
            missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))

            if len(missing_sessions) > 0:
                self._logger.warning(
                    f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
                new_rows_df = pl.DataFrame(
                    {"date": missing_sessions, "symbol": symbol},
                    schema_overrides={"date": data.schema["date"]}
                )
                # Concatenate with the original DataFrame
                data = pl.concat([data, new_rows_df], how="diagonal")

            data = data.with_columns(
                pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64).alias("sid")
            ).sort(["sid", "date"])
        return data

    async def _backfill_symbol_data(self):
        pass

    async def ingest_market_data_bundle(self, name: str,
                                        bundle_version: str,
                                        date_start: datetime.datetime,
                                        date_end: datetime.datetime,
                                        trading_calendar: ExchangeCalendar,
                                        symbols: list[str],
                                        data_bundle_source: DataBundleSource,
                                        frequency: datetime.timedelta,
                                        bundle_storage: BundleStorage,
                                        asset_service: AssetService,
                                        forward_fill_missing_ohlcv_data: bool
                                        ):

        """Ingest data for a given bundle.        """
        self._logger.info(f"Ingesting market data bundle: name={name}, date_start={date_start}, date_end={date_end}, "
                          f"symbols={symbols}, frequency={frequency}")
        if date_start < trading_calendar.first_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date start must be after first session of trading calendar. "
                f"First session is {trading_calendar.first_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date start is {date_start}")

        if date_end > trading_calendar.last_session.replace(tzinfo=trading_calendar.tz):
            raise ValueError(
                f"Date end must be before last session of trading calendar. "
                f"Last session is {trading_calendar.last_session.replace(tzinfo=trading_calendar.tz)} "
                f"and date end is {date_end}")

        data = await data_bundle_source.get_data(
            symbols=symbols,
            frequency=frequency,
            date_from=date_start,
            date_to=date_end
        )

        if data.is_empty():
            self._logger.warning(
                "No data for symbols={symbols}, frequency={frequency}, date_from={date_from}, date_end={date_end} found. Skipping ingestion.")
            return

        required_columns = [
            "date", "symbol", "exchange", "exchange_country", "open", "high", "low", "close", "volume"
        ]
        missing = [c for c in required_columns if c not in data.columns]

        if missing:
            raise ValueError(f"Ingested data is missing required columns: {missing}. Cannot ingest bundle.")

        data = data.with_columns(
            pl.lit(0).alias("sid"),
            pl.lit(False).alias("backfilled")
        )
        # repair data
        all_bars = [
            s for s in pl.from_pandas(
                trading_calendar.sessions_minutes(start=date_start.replace(tzinfo=None),
                                                  end=date_end.replace(tzinfo=None)).tz_convert(trading_calendar.tz)
            ) if s >= date_start and s <= date_end
        ]
        required_sessions = pl.DataFrame({"date": all_bars, "close": 0.00}).group_by_dynamic(
            index_column="date", every=frequency
        ).df

        equities_by_exchange = data.select(
            "symbol", "exchange", "exchange_country"
        ).group_by("exchange", "exchange_country").agg(pl.col("symbol").unique())
        for row in equities_by_exchange.iter_rows(named=True):
            exchange_name = row["exchange"]
            exchange_country = row["exchange_country"]
            symbols = row["symbol"]
            equities = await asset_service.get_equities_by_symbols_and_exchange(symbols=symbols,
                                                                                exchange_name=exchange_name)
            symbol_to_sid = {e.get_symbol_by_exchange(exchange_name=exchange_name): e.sid for e in equities}

            for symbol in symbols:
                symbol_data = data.filter(symbol=symbol).with_columns(pl.col("date"))
                missing_sessions = sorted(set(required_sessions["date"]) - set(symbol_data["date"]))
                if len(missing_sessions) > 0:
                    self._logger.warning(
                        f"Data for symbol {symbol} is missing on ticks ({len(missing_sessions)}): {[missing_session.isoformat() for missing_session in missing_sessions]}")
                    new_rows_df = pl.DataFrame({"date": missing_sessions, "symbol": symbol, "exchange": exchange_name,
                                                "exchange_country": exchange_country},
                                               schema_overrides={"date": data.schema["date"]})

                    # Concatenate with the original DataFrame
                    data = pl.concat([data, new_rows_df], how="diagonal")

            data = data.with_columns(
                pl.col("symbol").replace(symbol_to_sid).cast(pl.Int64).alias("sid")
            ).sort(["sid", "date"])
        if forward_fill_missing_ohlcv_data:
            data = data.with_columns(pl.col("close", "price").fill_null(strategy="forward"))
            data = data.with_columns(pl.col("high", "low", "open").fill_null(pl.col("price")))
            data = data.with_columns(pl.col("volume").fill_null(pl.lit(0.0)))

        data_bundle = DataBundle(name=name,
                                 start_date=date_start,
                                 end_date=date_end,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency,
                                 data=data,
                                 timestamp=datetime.datetime.now(tz=trading_calendar.tz),
                                 version=bundle_version
                                 )
        await self._bundle_registry.register_bundle(data_bundle=data_bundle, bundle_storage=bundle_storage)
        await bundle_storage.store_bundle(data_bundle=data_bundle)

        self._logger.info(f"Finished ingesting market data bundle_name={name}, bundle_version={bundle_version}")

    async def load_bundle(self, bundle_name: str, bundle_version: str | None) -> DataBundle:
        bundle_metadata = await self._bundle_registry.load_bundle_metadata(bundle_name=bundle_name,
                                                                           bundle_version=bundle_version)

        bundle_storage_class: BundleStorage = load_class(
            module_name='.'.join(bundle_metadata["bundle_storage_class"].split(".")[:-1]),
            class_name=bundle_metadata["bundle_storage_class"].split(".")[-1])
        # asset_repository_class: AssetRepository = load_class(
        #     module_name='.'.join(bundle_metadata["asset_repository_class"].split(".")[:-1]),
        #     class_name=bundle_metadata["asset_repository_class"].split(".")[-1])
        # adjustment_repository_class: AdjustmentRepository = load_class(
        #     module_name='.'.join(bundle_metadata["adjustment_repository_class"].split(".")[:-1]),
        #     class_name=bundle_metadata["adjustment_repository_class"].split(".")[-1])

        bundle_storage = await bundle_storage_class.from_json(bundle_metadata["bundle_storage_data"])

        # asset_repository = asset_repository_class.from_json(bundle_metadata["asset_repository_data"])
        # adjustment_repository = adjustment_repository_class.from_json(bundle_metadata["adjustment_repository_data"])
        start_date = datetime.datetime.strptime(bundle_metadata["start_date"], "%Y-%m-%dT%H:%M:%SZ")
        trading_calendar = get_calendar(bundle_metadata["trading_calendar_name"],
                                        start=start_date - datetime.timedelta(days=30))
        start_date = start_date.replace(tzinfo=trading_calendar.tz)
        end_date = datetime.datetime.strptime(bundle_metadata["end_date"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=trading_calendar.tz)
        frequency_timedelta = datetime.timedelta(seconds=int(bundle_metadata["frequency_seconds"])) if bundle_metadata["frequency_seconds"] is not None else None
        frequency_text = bundle_metadata.get("frequency_text", None)
        timestamp = datetime.datetime.strptime(bundle_metadata["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=trading_calendar.tz)
        data_bundle = DataBundle(name=bundle_name,
                                 start_date=start_date,
                                 end_date=end_date,
                                 trading_calendar=trading_calendar,
                                 frequency=frequency_timedelta or frequency_text,
                                 timestamp=timestamp,
                                 version=bundle_metadata["version"]
                                 )
        data = await bundle_storage.load_data_bundle(data_bundle=data_bundle)
        data_bundle.data = data
        return data_bundle
        # data_portal = DataPortal(
        #     data_bundle=data_bundle,
        #     historical_data_reader=data_bundle.historical_data_reader,
        #     fundamental_data_reader=data_bundle.fundamental_data_reader,
        #     future_minute_reader=data_bundle.historical_data_reader,
        #     future_daily_reader=data_bundle.historical_data_reader,
        # )
        # return data_portal

    async def clean(self, bundle_name: str, before: datetime.datetime = None, after: datetime.datetime = None,
                    keep_last: bool = None):
        """Clean up data that was created with ``ingest`` or
        ``$ python -m ziplime ingest``

        Parameters
        ----------
        name : str
            The name of the bundle to remove data for.
        before : datetime, optional
            Remove data ingested before this date.
            This argument is mutually exclusive with: keep_last
        after : datetime, optional
            Remove data ingested after this date.
            This argument is mutually exclusive with: keep_last
        keep_last : int, optional
            Remove all but the last ``keep_last`` ingestions.
            This argument is mutually exclusive with:
              before
              after

        Returns
        -------
        cleaned : set[str]
            The names of the runs that were removed.

        Raises
        ------
        BadClean
            Raised when ``before`` and or ``after`` are passed with
            ``keep_last``. This is a subclass of ``ValueError``.
        """

        for bundle in await self._bundle_registry.list_bundles():
            self._delete_bundle(bundle)

        # try:
        #     all_runs = sorted(
        #         filter(
        #             complement(pth.hidden),
        #             os.listdir(pth.data_path([name])),
        #         ),
        #         key=from_bundle_ingest_dirname,
        #     )
        # except OSError as e:
        #     if e.errno != errno.ENOENT:
        #         raise
        #     raise UnknownBundle(name)
        #
        # if before is after is keep_last is None:
        #     raise BadClean(before, after, keep_last)
        # if (before is not None or after is not None) and keep_last is not None:
        #     raise BadClean(before, after, keep_last)
        #
        # if keep_last is None:
        #
        #     def should_clean(name):
        #         dt = from_bundle_ingest_dirname(name)
        #         return (before is not None and dt < before) or (
        #                 after is not None and dt > after
        #         )
        #
        # elif keep_last >= 0:
        #     last_n_dts = set(take(keep_last, reversed(all_runs)))
        #
        #     def should_clean(name):
        #         return name not in last_n_dts
        #
        # else:
        #     raise BadClean(before, after, keep_last)
        #
        # cleaned = set()
        # for run in all_runs:
        #     if should_clean(run):
        #         log.info("Cleaning %s.", run)
        #         path = pth.data_path([name, run])
        #         shutil.rmtree(path)
        #         cleaned.add(path)
        #
        # return cleaned
