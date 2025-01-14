from dataclasses import dataclass


@dataclass
class ColumnSpecification:
    """Describes how historical data value is stored"""
    name: str
    original_type: str
    write_type: str
    # used only for floats/Decimals
    scaled_type: str
    scale_factor: int
