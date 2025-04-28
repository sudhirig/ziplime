import datetime
from ziplime.core.ingest_data import ingest_data

if __name__ == "__main__":
    ingest_data(start_date=datetime.datetime(year=2024, month=10, day=5, tzinfo=datetime.timezone.utc),
                end_date=datetime.datetime(year=2024, month=10, day=12, tzinfo=datetime.timezone.utc),
                symbols=["AAPL", "AMZN", "NVDA"],
                trading_calendar="NYSE",
                bundle_name="limex_us_polars_minute",
                data_frequency=datetime.timedelta(minutes=1))
