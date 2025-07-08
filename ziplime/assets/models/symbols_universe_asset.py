import datetime
from abc import abstractmethod

from sqlalchemy.orm import Mapped

from ziplime.core.db.annotated_types import AssetRouterFKPK, Decimal8
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseAssetModel(BaseModel):
    __tablename__ = "symbols_universe_assets"

    asset_sid: Mapped[AssetRouterFKPK]
    # asset_router = relationship(AssetRouter)
    name: Mapped[str]
    ratio: Mapped[Decimal8]
