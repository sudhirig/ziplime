from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.domain.column_specification import ColumnSpecification

OHLCV_COLUMNS_POLARS = [
    ColumnSpecification(name="close", write_type="float64", original_type='float64', scale_factor=1,
                        scaled_type="float64"),
    ColumnSpecification(name="open", write_type="float64", original_type='float64', scale_factor=1,
                        scaled_type="float64"),
    ColumnSpecification(name="high", write_type="float64", original_type='float64', scale_factor=1,
                        scaled_type="float64"),
    ColumnSpecification(name="low", write_type="float64", original_type='float64', scale_factor=1,
                        scaled_type="float64"),
    ColumnSpecification(name="volume", write_type="float64", original_type='float64', scale_factor=1,
                        scaled_type="float64")
]

DEFAULT_COLUMNS_POLARS = OHLCV_COLUMNS_POLARS + FUNDAMENTAL_DATA_COLUMNS
