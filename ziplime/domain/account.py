class Account:
    """
    The account object tracks information about the trading account. The
    values are updated as the algorithm runs and its keys remain unchanged.
    If connected to a exchange, one can update these values with the trading
    account values as reported by the exchange.
    """

    def __init__(self):
        self.settled_cash = 0.0
        self.accrued_interest = 0.0
        self.buying_power = float('inf')
        self.equity_with_loan = 0.0
        self.total_positions_value = 0.0
        self.total_positions_exposure = 0.0
        self.regt_equity = 0.0
        self.regt_margin = float('inf')
        self.initial_margin_requirement = 0.0
        self.maintenance_margin_requirement = 0.0
        self.available_funds = 0.0
        self.excess_liquidity = 0.0
        self.cushion = 0.0
        self.day_trades_remaining = float('inf')
        self.leverage = 0.0
        self.net_leverage = 0.0
        self.net_liquidation = 0.0
