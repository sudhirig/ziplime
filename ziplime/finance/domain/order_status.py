import enum


class OrderStatus(enum.Enum):
    OPEN = "open"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    HELD = "held"
