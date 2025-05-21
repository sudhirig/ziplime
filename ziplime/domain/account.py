from dataclasses import dataclass
from decimal import Decimal


@dataclass
class Account:
    """
    The account object tracks information about the trading account. The
    values are updated as the algorithm runs and its keys remain unchanged.
    If connected to a exchange, one can update these values with the trading
    account values as reported by the exchange.
    """

    settled_cash: Decimal
    accrued_interest: Decimal
    buying_power: Decimal
    equity_with_loan: Decimal
    total_positions_value: Decimal
    total_positions_exposure: Decimal
    regt_equity: Decimal
    regt_margin: Decimal
    initial_margin_requirement: Decimal
    maintenance_margin_requirement: Decimal
    available_funds: Decimal
    excess_liquidity: Decimal
    cushion: Decimal
    day_trades_remaining: Decimal
    leverage: Decimal
    net_leverage: Decimal
    net_liquidation: Decimal
