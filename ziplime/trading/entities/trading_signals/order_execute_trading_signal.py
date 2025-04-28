from ziplime.core.entities.enums.task_status import TaskStatus
from ziplime.exchanges.exchange import Exchange
from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_signals.trading_signal import TradingSignal
from ziplime.trading.trading_algorithm import TradingAlgorithm


class OrderExecuteTradingSignal(TradingSignal):

    # order request should be replaced with OrderExecuteTradingSignalConfig ->
    def __init__(self, algorithm: TradingAlgorithm, order: OrderRequest, exchange: Exchange):
        super().__init__(algorithm=algorithm, exchange=exchange)
        self.order = order

    def get_orders_to_execute(self) -> list[OrderRequest]:
        return [self.order]

    async def on_refresh_interval(self):
        """
        This method is responsible for controlling the task based on the status of the executor.

        :return: None
        """
        if self.status == TaskStatus.RUNNING:
            return await self.algorithm.submit_order(connector_name, trading_pair, amount, order_type, price, position_action)
        # getting price
        # self.algorithm.exchanges["xx"].get_price("")
