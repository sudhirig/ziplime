from ziplime.finance.controls.account_control import AccountControl


class MaxLeverage(AccountControl):
    """AccountControl representing a limit on the maximum leverage allowed
    by the algorithm.
    """

    def __init__(self, max_leverage):
        """max_leverage is the gross leverage in decimal form. For example,
        2, limits an algorithm to trading at most double the account value.
        """
        super(MaxLeverage, self).__init__(max_leverage=max_leverage)
        self.max_leverage = max_leverage

        if max_leverage is None:
            raise ValueError("Must supply max_leverage")

        if max_leverage < 0:
            raise ValueError("max_leverage must be positive")

    def validate(self, _portfolio, _account, _algo_datetime, _algo_current_data):
        """Fail if the leverage is greater than the allowed leverage."""
        if _account.leverage > self.max_leverage:
            self.fail()
