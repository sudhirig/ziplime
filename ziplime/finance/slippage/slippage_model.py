import math
from abc import abstractmethod
from pandas import isnull

from ziplime.assets.domain.db.asset import Asset
from ziplime.assets.domain.db.equity import Equity
from ziplime.assets.domain.db.futures_contract import FuturesContract
from ziplime.domain.bar_data import BarData
from ziplime.errors import LiquidityExceeded
from ziplime.finance.shared import FinancialModelMeta
from ziplime.finance.domain.transaction import Transaction

SELL = 1 << 0
BUY = 1 << 1
STOP = 1 << 2
LIMIT = 1 << 3

SQRT_252 = math.sqrt(252)

DEFAULT_EQUITY_VOLUME_SLIPPAGE_BAR_LIMIT = 0.025
DEFAULT_FUTURE_VOLUME_SLIPPAGE_BAR_LIMIT = 0.05


class SlippageModel(metaclass=FinancialModelMeta):
    """Abstract base class for slippage models.

    Slippage models are responsible for the rates and prices at which orders
    fill during a simulation.

    To implement a new slippage model, create a subclass of
    :class:`~ziplime.finance.slippage.SlippageModel` and implement
    :meth:`process_order`.

    Methods
    -------
    process_order(data, order)

    Attributes
    ----------
    volume_for_bar : int
        Number of shares that have already been filled for the
        currently-filling asset in the current minute. This attribute is
        maintained automatically by the base class. It can be used by
        subclasses to keep track of the total amount filled if there are
        multiple open orders for a single asset.

    Notes
    -----
    Subclasses that define their own constructors should call
    ``super(<subclass name>, self).__init__()`` before performing other
    initialization.
    """

    # Asset types that are compatible with the given model.
    allowed_asset_types = (Equity, FuturesContract)

    def __init__(self):
        self._volume_for_bar = 0

    @property
    def volume_for_bar(self):
        return self._volume_for_bar

    @abstractmethod
    def process_order(self, data, order):
        """Compute the number of shares and price to fill for ``order`` in the
        current minute.

        Parameters
        ----------
        data : ziplime.protocol.BarData
            The data for the given bar.
        order : ziplime.finance.order.Order
            The order to simulate.

        Returns
        -------
        execution_price : float
            The price of the fill.
        execution_volume : int
            The number of shares that should be filled. Must be between ``0``
            and ``order.amount - order.filled``. If the amount filled is less
            than the amount remaining, ``order`` will remain open and will be
            passed again to this method in the next minute.

        Raises
        ------
        ziplime.finance.slippage.LiquidityExceeded
            May be raised if no more orders should be processed for the current
            asset during the current bar.

        Notes
        -----
        Before this method is called, :attr:`volume_for_bar` will be set to the
        number of shares that have already been filled for ``order.asset`` in
        the current minute.

        :meth:`process_order` is not called by the base class on bars for which
        there was no historical volume.
        """
        raise NotImplementedError("process_order")

    def simulate(self, data: BarData, assets: list[Asset], orders_for_asset):
        self._volume_for_bar = 0
        volume = data.current(assets=assets, fields=["volume"])["volume"][0]

        if volume == 0:
            return

        # can use the close price, since we verified there's volume in this
        # bar.
        price = data.current(assets=assets, fields=["close"])["close"][0]

        # BEGIN
        #
        # Remove this block after fixing data to ensure volume always has
        # corresponding price.
        if isnull(price):
            return
        # END
        dt = data.current_dt

        for order in orders_for_asset:
            if order.open_amount == 0:
                continue

            order.check_triggers(price, dt)
            if not order.triggered:
                continue

            txn = None

            try:
                execution_price, execution_volume = self.process_order(data=data, order=order)

                if execution_price is not None:
                    # txn = create_transaction(
                    #     order,
                    #     data.current_dt,
                    #     execution_price,
                    #     execution_volume,
                    # )

                    txn = Transaction(
                        asset=order.asset,
                        amount=int(execution_volume),
                        dt=data.current_dt,
                        price=execution_price, order_id=order.id
                    )


            except LiquidityExceeded:
                break

            if txn:
                self._volume_for_bar += abs(txn.amount)
                yield order, txn

    def asdict(self):
        return self.__dict__
