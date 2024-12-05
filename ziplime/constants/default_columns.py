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

DEFAULT_COLUMNS = OHLCV_COLUMNS + FUNDAMENTAL_DATA_COLUMNS
