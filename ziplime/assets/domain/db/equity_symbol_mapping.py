import datetime

from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import IntegerPK, AssetFK, StringIndexed, ExchangeFK, AssetRouterFK, EquityFK
from ziplime.db.base_model import BaseModel


class EquitySymbolMapping(BaseModel):
    __tablename__ = "equity_symbol_mappings"

    id: Mapped[IntegerPK]
    sid: Mapped[EquityFK]
    symbol: Mapped[str]
    company_symbol: Mapped[StringIndexed]
    share_class_symbol: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    exchange: Mapped[ExchangeFK]
