from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from ziplime.assets.entities.asset import Asset


@dataclass(frozen=True)
class FuturesContract(Asset):
    root_symbol: str
    # mapping exchange code -> symbol
    symbol_mapping: dict[str, str]
    notice_date: datetime.date
    expiration_date: datetime.date
    multiplier: Decimal
    tick_size: Decimal

    def get_symbol_by_exchange(self, exchange_name: str) -> str | None:
        return self.symbol_mapping.get(exchange_name, None)
