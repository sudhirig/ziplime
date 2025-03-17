from ziplime.assets.domain.asset import Asset


class Future(Asset):
    """Asset subclass representing ownership of a futures contract.
    """

    def __init__(self,
                 sid: int,  # sid is required
                 exchange_info,  # exchange is required
                 symbol: str = "",
                 root_symbol: str = "",
                 asset_name: str = "",
                 start_date=None,
                 end_date=None,
                 notice_date=None,
                 expiration_date=None,
                 auto_close_date=None,
                 first_traded=None,
                 tick_size: float = 0.001,
                 multiplier: float = 1.0):

        super().__init__(
            sid,
            exchange_info,
            symbol=symbol,
            asset_name=asset_name,
            start_date=start_date,
            end_date=end_date,
            first_traded=first_traded,
            auto_close_date=auto_close_date,
            tick_size=tick_size,
            multiplier=multiplier
        )
        self.root_symbol = root_symbol
        self.notice_date = notice_date
        self.expiration_date = expiration_date

        if auto_close_date is None:
            if notice_date is None:
                self.auto_close_date = expiration_date
            elif expiration_date is None:
                self.auto_close_date = notice_date
            else:
                self.auto_close_date = min(notice_date, expiration_date)

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
                                 self.root_symbol,
                                 self.asset_name,
                                 self.start_date,
                                 self.end_date,
                                 self.notice_date,
                                 self.expiration_date,
                                 self.auto_close_date,
                                 self.first_traded,
                                 self.tick_size,
                                 self.price_multiplier))

    def to_dict(self):
        """
        Convert to a python dict.
        """
        super_dict = super(Future, self).to_dict()
        super_dict['root_symbol'] = self.root_symbol
        super_dict['notice_date'] = self.notice_date
        super_dict['expiration_date'] = self.expiration_date
        return super_dict
