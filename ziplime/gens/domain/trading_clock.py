from abc import abstractmethod


class TradingClock:

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError("__iter__ method must be implemented for trading clock.")
