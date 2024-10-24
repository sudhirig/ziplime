import enum

from ziplime.domain.column_specification import ColumnSpecification


class FundamentalDataValueType(enum.Enum):
    VALUE = "value"
    TTM = "ttm"
    ADD_VALUE = "add_value"


class FundamentalData(enum.Enum):
    TOTAL_SHARE_HOLDER_EQUITY_VALUE = "total_share_holder_equity"
    # TOTAL_LIABILITIES = "total_liabilities"
    # TOTAL_ASSETS = "total_assets"
    # SHARES_OUTSTANDING = "shares_outstanding"
    ROI = "roi"
    ROE = "roe"
    REVENUE = "revenue"
    RETURN_ON_TANGIBLE_EQUITY = "return_on_tangible_equity"
    QUICK_RATIO = "quick_ratio"
    PRICE_SALES = "price_sales"
    PRICE_FCF = "price_fcf"
    PE_RATIO = "pe_ratio"
    PRICE_BOOK = "price_book"
    OPERATING_MARGIN = "operating_margin"

    # OPERATING_INCOME = "operating_income"

    def property(self, value_type: FundamentalDataValueType):
        return self.value + "_" + value_type.value


FUNDAMENTAL_DATA_COLUMNS = [
    ColumnSpecification(name=f"{fd.value}_{fdt.value}", write_type="uint32", original_type='float')
    for fd in FundamentalData for fdt in FundamentalDataValueType
]
