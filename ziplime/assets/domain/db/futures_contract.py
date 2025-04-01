import datetime

from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import IntegerPK, StringUnique, FuturesRootSymbolFK, ExchangeFK
from ziplime.db.base_model import BaseModel


class FuturesContract(BaseModel):
    __tablename__ = "futures_contracts"

    sid: Mapped[IntegerPK]
    asset_name: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    first_traded: Mapped[datetime.date]
    auto_close_date: Mapped[datetime.date]
    exchange: Mapped[ExchangeFK]


    symbol: Mapped[StringUnique]
    root_symbol: Mapped[FuturesRootSymbolFK]
    notice_date: Mapped[datetime.date]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
