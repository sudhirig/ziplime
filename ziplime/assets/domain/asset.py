import pandas as pd

from zipline.utils.calendar_utils import get_calendar


# Users don't construct instances of this object, and embedding the signature
# in the docstring seems to confuse Sphinx, so disable it for now.
# @cython.embedsignature(False)
class Asset:
    """
    Base class for entities that can be owned by a trading algorithm.

    Attributes
    ----------
    sid : int
        Persistent unique identifier assigned to the asset.
    symbol : str
        Most recent ticker under which the asset traded. This field can change
        without warning if the asset changes tickers. Use ``sid`` if you need a
        persistent identifier.
    asset_name : str
        Full name of the asset.
    exchange : str
        Canonical short name of the exchange on which the asset trades (e.g.,
        'NYSE').
    exchange_full : str
        Full name of the exchange on which the asset trades (e.g., 'NEW YORK
        STOCK EXCHANGE').
    exchange_info : zipline.assets.ExchangeInfo
        Information about the exchange this asset is listed on.
    country_code : str
        Two character code indicating the country in which the asset trades.
    start_date : pd.Timestamp
        Date on which the asset first traded.
    end_date : pd.Timestamp
        Last date on which the asset traded. On Quantopian, this value is set
        to the current (real time) date for assets that are still trading.
    tick_size : float
        Minimum amount that the price can change for this asset.
    auto_close_date : pd.Timestamp
        Date on which positions in this asset will be automatically liquidated
        to cash during a simulation. By default, this is three days after
        ``end_date``.
    """


    def __init__(self,
                 sid, # sid is required
                 exchange_info, # exchange is required
                 symbol="",
                 asset_name="",
                 start_date=None,
                 end_date=None,
                 first_traded=None,
                 auto_close_date=None,
                 tick_size=0.01,
                 multiplier=1.0):

        self.sid = sid
        self.symbol = symbol
        self.asset_name = asset_name
        self.exchange_info = exchange_info
        self.start_date = start_date
        self.end_date = end_date
        self.first_traded = first_traded
        self.auto_close_date = auto_close_date
        self.tick_size = tick_size
        self.price_multiplier = multiplier

    @property
    def exchange(self):
        return self.exchange_info.canonical_name

    @property
    def exchange_full(self):
        return self.exchange_info.name

    @property
    def country_code(self):
        return self.exchange_info.country_code

    def __int__(self):
        return self.sid

    def __index__(self):
        return self.sid

    def __hash__(self):
        return self.sid

    def __repr__(self):
        if self.symbol:
            return '%s(%d [%s])' % (type(self).__name__, self.sid, self.symbol)
        else:
            return '%s(%d)' % (type(self).__name__, self.sid)

    def __reduce__(self):
        """
        Function used by pickle to determine how to serialize/deserialize this
        class.  Should return a tuple whose first element is self.__class__,
        and whose second element is a tuple of all the attributes that should
        be serialized/deserialized during pickling.
        """
        return (self.__class__, (self.sid,
                                 self.exchange_info,
                                 self.symbol,
                                 self.asset_name,
                                 self.start_date,
                                 self.end_date,
                                 self.first_traded,
                                 self.auto_close_date,
                                 self.tick_size,
                                 self.price_multiplier))

    def to_dict(self):
        """Convert to a python dict containing all attributes of the asset.

        This is often useful for debugging.

        Returns
        -------
        as_dict : dict
        """
        return {
            'sid': self.sid,
            'symbol': self.symbol,
            'asset_name': self.asset_name,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'first_traded': self.first_traded,
            'auto_close_date': self.auto_close_date,
            'exchange': self.exchange,
            'exchange_full': self.exchange_full,
            'tick_size': self.tick_size,
            'multiplier': self.price_multiplier,
            'exchange_info': self.exchange_info,
        }

    @classmethod
    def from_dict(cls, dict_):
        """
        Build an Asset instance from a dict.
        """
        return cls(**{k: v for k, v in dict_.items() if k in cls._kwargnames})

    def is_alive_for_session(self, session_label):
        """
        Returns whether the asset is alive at the given dt.

        Parameters
        ----------
        session_label: pd.Timestamp
            The desired session label to check. (midnight UTC)

        Returns
        -------
        boolean: whether the asset is alive at the given dt.
        """
        ref_start = self.start_date.value
        ref_end = self.end_date.value

        return ref_start <= session_label.value <= ref_end

    def is_exchange_open(self, dt_minute):
        """
        Parameters
        ----------
        dt_minute: pd.Timestamp (UTC, tz-aware)
            The minute to check.

        Returns
        -------
        boolean: whether the asset's exchange is open at the given minute.
        """
        calendar = get_calendar(self.exchange)
        return calendar.is_open_on_minute(dt_minute)

