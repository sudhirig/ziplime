import datetime

from sqlalchemy.orm import Mapped

from ziplime.assets.domain.db.asset import Asset
from ziplime.db.annotated_types import StringUnique, FuturesRootSymbolFK, ExchangeFK


class FuturesContract(Asset):
    __tablename__ = "futures_contracts"

    symbol: Mapped[StringUnique]
    root_symbol: Mapped[FuturesRootSymbolFK]
    notice_date: Mapped[datetime.date]
    expiration_date: Mapped[datetime.date]
    multiplier: Mapped[float]
    tick_size: Mapped[float]
    exchange: Mapped[ExchangeFK]
