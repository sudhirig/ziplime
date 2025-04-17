from sqlalchemy.orm import Mapped

from ziplime.assets.domain.db.asset import Asset


class Currency(Asset):
    __tablename__ = "currencies"
    symbol: Mapped[str]
