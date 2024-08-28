import dataclasses

from lime_trader.models.market import QuoteHistory


@dataclasses.dataclass
class LimeQuote:
    quote_history: QuoteHistory
    symbol: str
