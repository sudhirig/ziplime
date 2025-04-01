import os

import pandas as pd


def last_modified_time(path: str) -> pd.Timestamp:
    """
    Get the last modified time of path as a Timestamp.
    """
    return pd.Timestamp(os.path.getmtime(path), unit='s', tz='UTC')
