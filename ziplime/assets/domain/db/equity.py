from functools import lru_cache

from sqlalchemy.orm import Mapped, relationship

from ziplime.assets.domain.db.asset import Asset
from ziplime.assets.domain.db.equity_symbol_mapping import EquitySymbolMapping


class Equity(Asset):
    __tablename__ = "equities"

    equity_symbol_mappings: Mapped[list[EquitySymbolMapping]] = relationship("EquitySymbolMapping")

    @lru_cache
    def get_symbol_by_exchange(self, exchange_name: str | None) -> str | None:
        if exchange_name is None:
            symbol_mapping = next(iter(self.equity_symbol_mappings), None)
        else:
            symbol_mapping = next(filter(lambda x: x.exchange == exchange_name, self.equity_symbol_mappings), None)
        if symbol_mapping:
            return symbol_mapping.symbol
        return None
