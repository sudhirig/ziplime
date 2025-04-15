from abc import ABC, abstractmethod
from ziplime.finance.cancel_policy import NeverCancel

from ziplime.assets.domain.db.asset import Asset
from ziplime.finance.commission import CommissionModel
from ziplime.finance.domain.order import Order
from ziplime.finance.domain.transaction import Transaction
from ziplime.domain.bar_data import BarData


class Blotter(ABC):
    def __init__(self, cancel_policy=None):
        self.cancel_policy = cancel_policy if cancel_policy else NeverCancel()
        self.current_dt = None

    def set_date(self, dt):
        self.current_dt = dt

    @abstractmethod
    def save_order(self, order: Order) -> None: ...

    @abstractmethod
    def order_rejected(self, order: Order) -> None: ...

    @abstractmethod
    def order_held(self, order: Order) -> None: ...

    @abstractmethod
    def order_cancelled(self, order: Order) -> None: ...

    @abstractmethod
    def get_order_by_id(self, order_id: str) -> Order | None: ...

    @abstractmethod
    def get_open_orders_by_asset(self, asset: Asset) -> dict[str, Order] | None: ...

    @abstractmethod
    def get_open_orders(self) -> dict[Asset, dict[str, Order]]: ...

    @abstractmethod
    def cancel_all_orders_for_asset(self, asset: Asset, relay_status: bool = True) -> None: ...

    def batch_order(self, order_arg_lists):
        """Place a batch of orders.

        Parameters
        ----------
        order_arg_lists : iterable[tuple]
            Tuples of args that `order` expects.

        Returns
        -------
        order_ids : list[str or None]
            The unique identifier (or None) for each of the orders placed
            (or not placed).

        Notes
        -----
        This is required for `Blotter` subclasses to be able to place a batch
        of orders, instead of being passed the order requests one at a time.
        """

        return [self.order(*order_args) for order_args in order_arg_lists]

    @abstractmethod
    def execute_cancel_policy(self, event):
        raise NotImplementedError("execute_cancel_policy")

    @abstractmethod
    def process_splits(self, splits: list[tuple[Asset, float]]) -> None:
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

        raise NotImplementedError("process_splits")

    @abstractmethod
    def get_transactions(self, bar_data: BarData) -> tuple[list[Transaction], list[CommissionModel], list[Order]]:
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

        raise NotImplementedError("get_transactions")

    @abstractmethod
    def prune_orders(self, closed_orders) -> None:
        """
        Removes all given orders from the blotter's open_orders list.

        Parameters
        ----------
        closed_orders: iterable of orders that are closed.

        Returns
        -------
        None
        """

        raise NotImplementedError("prune_orders")
