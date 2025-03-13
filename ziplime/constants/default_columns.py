from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.domain.column_specification import ColumnSpecification

OHLCV_COLUMNS = [
    ColumnSpecification(name="close", write_type="uint32", original_type='float64', scale_factor=1000,
                        scaled_type="int"),
    ColumnSpecification(name="open", write_type="uint32", original_type='float64', scale_factor=1000,
                        scaled_type="int"),
    ColumnSpecification(name="high", write_type="uint32", original_type='float64', scale_factor=1000,
                        scaled_type="int"),
    ColumnSpecification(name="low", write_type="uint32", original_type='float64', scale_factor=1000,
                        scaled_type="int"),
    ColumnSpecification(name="volume", write_type="uint32", original_type='int', scale_factor=1,
                        scaled_type="int")
]
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

DEFAULT_COLUMNS = OHLCV_COLUMNS + FUNDAMENTAL_DATA_COLUMNS
DEFAULT_COLUMNS_POLARS = OHLCV_COLUMNS_POLARS + FUNDAMENTAL_DATA_COLUMNS
