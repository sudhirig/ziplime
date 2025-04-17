from collections import defaultdict, OrderedDict
from copy import copy

import structlog

from ziplime.assets.domain.db.asset import Asset

from .blotter import Blotter

from ziplime.domain.bar_data import BarData
from ziplime.finance.domain.order import Order
from ...gens.exchanges.exchange import Exchange


class InMemoryBlotter(Blotter):
    def __init__(
            self,
            exchange: Exchange,
            cancel_policy,
    ):
        super().__init__(cancel_policy=cancel_policy)
        self._logger = structlog.get_logger(__name__)
        self.exchange = exchange
        # these orders are aggregated by asset
        self.open_orders = defaultdict(dict)
        # keep a dict of orders by their own id
        self.orders = {}
        # holding orders that have come in since the last event.
        self.new_orders = OrderedDict()

        self.max_shares = int(1e11)

    def save_order(self, order: Order):
        """Place an order.

        Parameters
        ----------
        asset : ziplime.assets.Asset
            The asset that this order is for.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : ziplime.finance.execution.ExecutionStyle
            The execution style for the order.
        order_id : str, optional
            The unique identifier for this order.

        Returns
        -------
        order_id : str or None
            The unique identifier for this order, or None if no order was
            placed.

        Notes
        -----
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(asset, amount)
        Limit order:     order(asset, amount, style=LimitOrder(limit_price))
        Stop order:      order(asset, amount, style=StopOrder(stop_price))
        StopLimit order: order(asset, amount, style=StopLimitOrder(limit_price,
        stop_price))
        """
        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount
        self.open_orders[order.asset][order.id] = order
        self.orders[order.id] = order
        return order.id

    def order_cancelled(self, order: Order) -> None:
        asset_orders = self.open_orders[order.asset]
        asset_orders.pop(order.id, None)

    def order_rejected(self, order: Order) -> None:
        asset_orders = self.open_orders[order.asset]
        asset_orders.pop(order.id, None)

    def get_order_by_id(self, order_id: str) -> Order | None:
        return self.orders.get(order_id, None)

    def get_open_orders_by_asset(self, asset: Asset) -> dict[str, Order] | None:
        return self.open_orders.get(asset, None)

    def get_open_orders(self) -> dict[Asset, dict[str, Order]]:
        return self.open_orders

    def cancel_all_orders_for_asset(self, asset: Asset, relay_status: bool = True):
        """
        Cancel all open orders for a given asset.
        """
        self.open_orders.pop(asset, None)

    # End of day cancel for daily frequency
    def execute_daily_cancel_policy(self, event):
        if self.cancel_policy.should_cancel(event):
            warn = self.cancel_policy.warn_on_cancel
            for asset in copy(self.open_orders):
                orders = self.open_orders[asset]
                if len(orders) > 1:
                    order = orders[0]
                    self.cancel(order.id, relay_status=True)
                    if warn:
                        if order.filled > 0:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} has been partially filled. "
                                f"{order.filled} shares were successfully "
                                f"purchased. {order.amount - order.filled} shares were not "
                                f"filled by the end of day and "
                                f"were canceled."
                            )
                        elif order.filled < 0:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} has been partially filled. "
                                f"{-1 * order.filled} shares were successfully "
                                f"sold. {-1 * (order.amount - order.filled)} shares were not "
                                f"filled by the end of day and "
                                f"were canceled."
                            )
                        else:
                            self._logger.warn(
                                f"Your order for {order.amount} shares of "
                                f"{order.asset.symbol} failed to fill by the end of day "
                                f"and was canceled."
                            )

    def execute_cancel_policy(self, event):
        if self.cancel_policy.should_cancel(event):
            warn = self.cancel_policy.warn_on_cancel
            for asset in copy(self.open_orders):
                self.cancel_all_orders_for_asset(asset, warn, relay_status=False)

    def order_held(self, order: Order) -> None:
        pass

    def process_splits(self, splits):
        """
        Processes a list of splits by modifying any open orders as needed.

        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (asset, ratio).

        Returns
        -------
        None
        """
        for asset, ratio in splits:
            if asset not in self.open_orders:
                continue

            orders_to_modify = self.open_orders[asset]
            for order in orders_to_modify:
                order.handle_split(ratio)

    def get_transactions(self, bar_data: BarData):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: ziplime.protocol.BarData

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

        if self.open_orders:
            for asset, asset_orders in self.open_orders.items():
                slippage = self.exchange.get_slippage_model(asset=asset)

                for order, txn in slippage.simulate(data=bar_data, assets=[asset],
                                                    orders_for_asset=asset_orders.values()):
                    commission = self.exchange.get_commission_model(asset=asset)
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

    def prune_orders(self, closed_orders):
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """
        # remove all closed orders from our open_orders dict
        for order in closed_orders:
            asset = order.asset
            asset_orders = self.open_orders[asset]
            asset_orders.pop(order, None)

        # now clear out the assets from our open_orders dict that have
        # zero open orders
        for asset in list(self.open_orders.keys()):
            if len(self.open_orders[asset]) == 0:
                self.open_orders.pop(asset, None)
