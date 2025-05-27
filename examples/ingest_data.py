import datetime
from ziplime.core.ingest_data import ingest_data

if __name__ == "__main__":
    ingest_data(start_date=datetime.datetime(year=2022, month=8, day=1, tzinfo=datetime.timezone.utc),
                end_date=datetime.datetime(year=2024, month=12, day=31, tzinfo=datetime.timezone.utc),
                symbols=["AAPL", "AMZN", "NVDA", "MSFT", "GOOGL", "GOOG",  "AVGO", "WMT"],
                         # "V", "MA", "XOM", "COST", "ORCL", ""BRK.B",
                         # "PG", "HD", "JNJ", "UNH"],
                trading_calendar="NYSE",
                bundle_name="limex_us_polars_minute",
                data_frequency=datetime.timedelta(minutes=1))
