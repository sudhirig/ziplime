import logging

from zipline.finance.blotter import Blotter

from zipline.assets import Asset
from zipline.finance.order import Order as ZPOrder
from ziplime.gens.brokers.broker import Broker


class BlotterLive(Blotter):

    def __init__(self, data_frequency: str, broker: Broker):
        super().__init__()
        self.broker = broker
        self._processed_closed_orders = []
        self._processed_transactions = []
        self.data_frequency = data_frequency
        self.new_orders = []
        self._logger = logging.getLogger(__name__)

    def __repr__(self):
        return f"""{self.__class__.__name__}(
        open_orders={self.open_orders},
        orders={self.get_orders()},
        new_orders={self.new_orders}
        )"""

    @property
    def orders(self) -> dict[str, ZPOrder]:
        return self.broker.get_orders()


    def get_orders(self) -> dict[str, ZPOrder]:
        return self.broker.get_orders()

    @property
    def open_orders(self):
        assets = set([
            order.asset
            for order in self.get_orders().values()
            if order.open
        ])
        return {
            asset: [
                order for order in self.get_orders().values()
                if order.asset == asset and order.open
            ]
            for asset in assets
        }

    def order(self, asset: Asset, amount: int, style: str, order_id: str | None = None):
        assert order_id is None
        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return None
        order = self.broker.order(asset, amount, style)
        self.new_orders.append(order)
        return order.id

    def cancel(self, order_id, relay_status=True):
        return self.broker.cancel_order(order_id)

    def execute_cancel_policy(self, event):
        # Cancellation is handled at the broker
        pass

    def reject(self, order_id, reason=''):
        self._logger.warning(f"Unexpected reject request for {order_id}: '{reason}'")

    def hold(self, order_id, reason=''):
        self._logger.warning(f"Unexpected hold request for {order_id}: '{reason}'")

    def get_transactions(self, bar_data):
        # All returned values from this function are delta between
        # the previous and actual call.
        def _list_delta(lst_a, lst_b):
            return [elem for elem in lst_a if elem not in set(lst_b)]

        all_orders = self.get_orders()
        try:
            all_transactions = list(self.broker.get_transactions().values())
        except NotImplementedError as e:
            # we cannot get all previous orders from broker, use just tracked orders
            all_transactions = list(self.broker.get_transactions_by_order_ids(order_ids=list(all_orders.keys())).values())
        new_transactions = _list_delta(all_transactions, self._processed_transactions)

        self._processed_transactions = all_transactions

        new_commissions = [{'asset': tx.asset,
                            'cost': tx.commission,
                            'order': all_orders[tx.order_id]}
                           for tx in new_transactions]

        all_closed_orders = [order for order in all_orders.values() if not order.open]
        new_closed_orders = _list_delta(all_closed_orders, self._processed_closed_orders)
        self._processed_closed_orders = all_closed_orders

        return new_transactions, new_commissions, new_closed_orders

    def prune_orders(self, closed_orders):
        # Orders are handled at the broker
        pass

    def process_splits(self, splits):
        # Splits are handled at the broker
        pass

    def cancel_all_orders_for_asset(self, asset, warn=False, relay_status=True):
        pass
