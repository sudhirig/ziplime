from abc import ABC, abstractmethod
from zipline.finance.cancel_policy import NeverCancel
from zipline.finance.execution import ExecutionStyle

from ziplime.assets.domain.db.asset import Asset
from ziplime.finance.domain.transaction import Transaction
from ziplime.domain.bar_data import BarData


class Blotter(ABC):
    def __init__(self, cancel_policy=None):
        self.cancel_policy = cancel_policy if cancel_policy else NeverCancel()
        self.current_dt = None

    def set_date(self, dt):
        self.current_dt = dt

    @abstractmethod
    def order(self, asset: Asset, amount: int, style: ExecutionStyle, order_id: str | None = None) -> None:
        """Place an order.

        Parameters
        ----------
        asset : zipline.assets.Asset
            The asset that this order is for.
        amount : int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.
        style : zipline.finance.execution.ExecutionStyle
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
        amount > 0 : Buy/Cover
        amount < 0 : Sell/Short
        Market order : order(asset, amount)
        Limit order : order(asset, amount, style=LimitOrder(limit_price))
        Stop order : order(asset, amount, style=StopOrder(stop_price))
        StopLimit order : order(asset, amount,
        style=StopLimitOrder(limit_price, stop_price))
        """

        raise NotImplementedError("order")

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
    def cancel(self, order_id: str, relay_status: bool = True):
        """Cancel a single order

        Parameters
        ----------
        order_id : int
            The id of the order

        relay_status : bool
            Whether or not to record the status of the order
        """
        raise NotImplementedError("cancel")

    @abstractmethod
    def cancel_all_orders_for_asset(self, asset: Asset, warn: bool = False, relay_status: bool = True) -> None:
        """
        Cancel all open orders for a given asset.
        """

        raise NotImplementedError("cancel_all_orders_for_asset")

    @abstractmethod
    def execute_cancel_policy(self, event):
        raise NotImplementedError("execute_cancel_policy")

    @abstractmethod
    def reject(self, order_id: str, reason: str = "") -> None:
        """
        Mark the given order as 'rejected', which is functionally similar to
        cancelled. The distinction is that rejections are involuntary (and
        usually include a message from a broker indicating why the order was
        rejected) while cancels are typically user-driven.
        """

        raise NotImplementedError("reject")

    @abstractmethod
    def hold(self, order_id: str, reason: str = "") -> None:
        """
        Mark the order with order_id as 'held'. Held is functionally similar
        to 'open'. When a fill (full or partial) arrives, the status
        will automatically change back to open/filled as necessary.
        """

        raise NotImplementedError("hold")

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
    def get_transactions(self, bar_data: BarData) -> list[Transaction]:
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: zipline._protocol.BarData

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
