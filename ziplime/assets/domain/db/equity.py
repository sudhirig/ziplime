import datetime

from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import IntegerPK, ExchangeFK
from ziplime.db.base_model import BaseModel


class Equity(BaseModel):
    __tablename__ = "equities"

    sid: Mapped[IntegerPK]
    asset_name: Mapped[str]
    start_date: Mapped[datetime.date]
    end_date: Mapped[datetime.date]
    first_traded: Mapped[datetime.date]
    auto_close_date: Mapped[datetime.date]
    exchange: Mapped[ExchangeFK]
