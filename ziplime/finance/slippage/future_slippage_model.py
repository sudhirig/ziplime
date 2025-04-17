from ziplime.finance.shared import AllowedAssetMarker

from ziplime.assets.domain.db.futures_contract import FuturesContract
from ziplime.finance.slippage.slippage_model import SlippageModel


class FutureSlippageModel(SlippageModel, metaclass=AllowedAssetMarker):
    """Base class for slippage models which only support futures."""

    allowed_asset_types = (FuturesContract,)
