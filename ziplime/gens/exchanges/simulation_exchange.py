import uuid

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.db.asset import Asset
from zipline.finance.order import Order as ZPOrder

from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import EquityCommissionModel, FutureCommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.gens.exchanges.exchange import Exchange


class SimulationExchange(Exchange):

    def __init__(self, equity_slippage: SlippageModel,
                 future_slippage: SlippageModel,
                 equity_commission: EquityCommissionModel,
                 future_commission: FutureCommissionModel,
                 ):
        self.slippage_models = {
            AssetType.EQUITY.value: equity_slippage,
            AssetType.FUTURES_CONTRACT.value: future_slippage,
        }
        self.commission_models = {
            AssetType.EQUITY.value: equity_commission,
            AssetType.FUTURES_CONTRACT.value: future_commission,
        }

    def get_commission_model(self, asset: Asset):
        return self.commission_models[asset.asset_router.asset_type]

    def get_slippage_model(self, asset: Asset):
        return self.slippage_models[asset.asset_router.asset_type]

    def submit_order(self, order: Order):
        order.id = uuid.uuid4().hex

    def get_positions(self) -> dict[Asset, Position]:
        pass

    def get_portfolio(self) -> Portfolio:
        pass

    def get_account(self) -> Account:
        pass

    def get_time_skew(self):
        pass

    def order(self, asset, amount, style):
        pass

    def is_alive(self):
        pass

    def get_orders(self) -> dict[str, ZPOrder]:
        pass

    def get_transactions(self):
        pass

    def get_orders_by_ids(self, order_ids: list[str]):
        pass

    def get_transactions_by_order_ids(self, order_ids: list[str]):
        pass

    def cancel_order(self, order_param):
        pass

    def get_last_traded_dt(self, asset):
        pass

    def get_spot_value(self, assets, field, dt, data_frequency):
        pass

    def get_realtime_bars(self, assets, frequency):
        pass
