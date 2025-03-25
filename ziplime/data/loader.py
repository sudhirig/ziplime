import logging
import os

import pandas as pd

from ..utils.paths import (
    cache_root,
    data_root,
)

logger = logging.getLogger(__name__)

ONE_HOUR = pd.Timedelta(hours=1)


def last_modified_time(path: str) -> pd.Timestamp:
    """
    Get the last modified time of path as a Timestamp.
    """
    return pd.Timestamp(os.path.getmtime(path), unit='s', tz='UTC')


# def get_data_filepath(name: str) -> str:
#     """
#     Returns a handle to data file.
#
#     Creates containing directory, if needed.
#     """
#     dr = data_root()
#
#     if not os.path.exists(dr):
#         os.makedirs(dr)
#
#     return os.path.join(dr, name)
#
#
# def get_cache_filepath(name: str) -> str:
#     cr = cache_root()
#     if not os.path.exists(cr):
#         os.makedirs(cr)
#
#     return os.path.join(cr, name)
#
#
# def get_benchmark_filename(symbol: str) -> str:
#     return "%s_benchmark.csv" % symbol
#
#
