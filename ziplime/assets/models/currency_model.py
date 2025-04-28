from sqlalchemy.orm import Mapped

from ziplime.assets.models.asset_model import AssetModel


class CurrencyModel(AssetModel):
    __tablename__ = "currencies"
