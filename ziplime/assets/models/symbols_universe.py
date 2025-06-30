import datetime
from abc import abstractmethod

from sqlalchemy.orm import Mapped, relationship, declared_attr

from ziplime.assets.entities.asset import Asset
from ziplime.core.db.annotated_types import AssetRouterFKPK
from ziplime.core.db.base_model import BaseModel


class SymbolsUniverseModel(BaseModel):
    __tablename__ = "symbols_universe"

    sid: Mapped[AssetRouterFKPK]
    # asset_router = relationship(AssetRouter)
    name: Mapped[str]
    universe_type: Mapped[str]

    def get_assets(self, ) -> list[Asset]:
        return []
