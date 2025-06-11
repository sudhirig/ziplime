import datetime
from ziplime.core.ingest_data import ingest_data

if __name__ == "__main__":
    ingest_data(start_date=datetime.datetime(year=2022, month=1, day=1, tzinfo=datetime.timezone.utc),
                end_date=datetime.datetime(year=2025, month=6, day=2, tzinfo=datetime.timezone.utc),
                symbols=["VOO", "AMZN", "NVDA", "AAPL"],
                         # "V", "MA", "XOM", "COST", "ORCL", ""BRK.B",
                         # "PG", "HD", "JNJ", "UNH"],
                trading_calendar="NYSE",
                bundle_name="limex_us_polars_minute",
                data_frequency=datetime.timedelta(minutes=1))
