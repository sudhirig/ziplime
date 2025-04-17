import enum


class SimulationEvent(enum.Enum):
    BAR = "bar"
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    EMISSION_RATE_END = "emission_rate_end"
    BEFORE_TRADING_START_BAR = "before_trading_start_bar"
