from ziplime.exchanges.exchange import Exchange
from ziplime.trading.entities.orders.order_request import OrderRequest
from ziplime.trading.entities.trading_signals.order_execute_trading_signal import OrderExecuteTradingSignal
from ziplime.trading.trading_algorithm import TradingAlgorithm


class TradingSignalExecutor:
    def __init__(self):
        self.running_trading_signals = []
        self.completed_trading_signals = []

    async def create_order_execute_trading_signal(self,
                                                  algorithm: TrteaadingAlgorithm,
                                                  order: OrderRequest, exchange: Exchange):
        trading_signal = OrderExecuteTradingSignal(order=order, exchange=exchange, algorithm=algorithm)
        trading_signal.start()
        self.running_trading_signals.append(trading_signal)
