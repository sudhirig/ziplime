import enum


class AssetType(enum.Enum):
    EQUITY = "equity"
    FUTURES_CONTRACT = "futures_contract"
    OPTIONS_CONTRACT = "options_contract"
    CURRENCY = "currency"
