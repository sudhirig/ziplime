from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import UuidPK, ExchangeFK, AssetRouterFK
from ziplime.db.base_model import BaseModel


class TradingPair(BaseModel):
    id: Mapped[UuidPK]

    base_asset_sid: Mapped[AssetRouterFK]
    quote_asset_sid: Mapped[AssetRouterFK]

    exchange: Mapped[ExchangeFK]

