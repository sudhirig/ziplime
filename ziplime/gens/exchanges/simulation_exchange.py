import uuid

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.domain.db.asset import Asset

from ziplime.domain.bar_data import BarData
from ziplime.domain.position import Position
from ziplime.domain.portfolio import Portfolio
from ziplime.domain.account import Account
from ziplime.finance.commission import EquityCommissionModel, FutureCommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.slippage.slippage_model import SlippageModel
from ziplime.gens.exchanges.exchange import Exchange


class SimulationExchange(Exchange):

    def __init__(self, name: str,
                 equity_slippage: SlippageModel,
                 future_slippage: SlippageModel,
                 equity_commission: EquityCommissionModel,
                 future_commission: FutureCommissionModel,
                 ):
        super().__init__(name)
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

    async def submit_order(self, order: Order):
        order.id = uuid.uuid4().hex
        return order

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

    def get_orders(self) -> dict[str, Order]:
        pass

    async def get_transactions(self, orders: dict[Asset, dict[str, Order]], bar_data: BarData):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: ziplime._protocol.BarData

        Notes
        -----
        This method book-keeps the blotter's open_orders dictionary, so that
         it is accurate by the time we're done processing open orders.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        commissions_list: List
            commissions_list: list of commissions resulting from filling the
            open orders.  A commission is an object with "asset" and "cost"
            parameters.

        closed_orders: List
            closed_orders: list of all the orders that have filled.
        """

        closed_orders = []
        transactions = []
        commissions = []

        for asset, asset_orders in orders.items():
            slippage = self.get_slippage_model(asset=asset)

            for order, txn in slippage.simulate(data=bar_data, assets=[asset],
                                                orders_for_asset=asset_orders.values()):
                commission = self.get_commission_model(asset=asset)
                additional_commission = commission.calculate(order, txn)

                if additional_commission > 0:
                    commissions.append(
                        {
                            "asset": order.asset,
                            "order": order,
                            "cost": additional_commission,
                        }
                    )

                order.filled += txn.amount
                order.commission += additional_commission

                order.dt = txn.dt

                transactions.append(txn)

                if not order.open:
                    closed_orders.append(order)

        return transactions, commissions, closed_orders

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
