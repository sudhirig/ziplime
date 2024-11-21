from typing import Any

import pandas as pd
from zipline._protocol import BarData


def get_fundamental_data(bar_data: BarData, assets, fields, bar_count, frequency, fillna: Any=None):
    if frequency != '1q':
        raise Exception("Currently only frequency of 1 quarter is supported for fundamental data")
    frequency = '1d'

    max_days_per_quarter = 92
    end_date = pd.to_datetime(bar_data.current_session)
    start_date = end_date - pd.Timedelta(days=max_days_per_quarter * bar_count)

    total_days = max_days_per_quarter * bar_count
    bar_data_history = bar_data.history(
        assets, fields, total_days, frequency
    )
    quarters = pd.date_range(start=start_date, end=end_date, freq='Q')
    filtered = pd.DataFrame(bar_data_history[bar_data_history != 0][-bar_count:], index=quarters)

    if fillna is not None:
        filtered[filtered.isnull()] = fillna

    result_series = pd.Series(data=filtered[filtered.columns[0]], index=filtered.index)
    return result_series
