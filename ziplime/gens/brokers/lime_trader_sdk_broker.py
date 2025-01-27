import datetime
import logging
from decimal import Decimal

from zipline.errors import SymbolNotFound

from ziplime.protocol import Portfolio as ZpPortfolio, Position as ZpPosition, Account as ZpAccount
from lime_trader import LimeClient
from lime_trader.models.accounts import AccountDetails
from lime_trader.models.market import Period
from lime_trader.models.trading import Order, OrderSide, OrderDetails, OrderStatus, OrderType, TimeInForce
from zipline.assets import Asset
from zipline.finance.order import (Order as ZPOrder,
                                   ORDER_STATUS as ZP_ORDER_STATUS)
from zipline.finance.execution import (MarketOrder,
                                       LimitOrder,
                                       StopOrder,
                                       StopLimitOrder, ExecutionStyle)
from ziplime.finance.transaction import Transaction
from zipline.api import symbol as symbol_lookup
import pandas as pd
import numpy as np
import uuid


from ziplime.gens.brokers.broker import Broker


class LimeTraderSdkBroker(Broker):
    def __init__(self, lime_sdk_credentials_file: str):
        self._lime_sdk_credentials_file = lime_sdk_credentials_file
        self._logger = logging.getLogger(__name__)
        self._lime_sdk_client = LimeClient.from_file(lime_sdk_credentials_file, logger=self._logger)

        self._tracked_orders = {}

    def subscribe_to_market_data(self, asset):
        '''Do nothing to comply the interface'''
        pass

    def subscribed_assets(self):
        '''Do nothing to comply the interface'''
        return []

    def get_positions(self) -> dict[Asset, ZpPosition]:
        z_positions = {}
        positions = self._lime_sdk_client.account.get_positions(account_number=self._get_account_number(),
                                                                date=None, strategy=None)
        quotes = {
            quote.symbol: quote
            for quote in
            self._lime_sdk_client.market.get_current_quotes(symbols=[pos.symbol for pos in positions])
        }

        for pos in positions:
            try:
                asset = symbol_lookup(pos.symbol)
            except SymbolNotFound:
                continue
            try:
                quote = quotes[pos.symbol]
                z_position = ZpPosition(asset=asset,
                                        cost_basis=float(pos.average_open_price),
                                        last_sale_date=quote.date,
                                        last_sale_price=float(quote.last) if quote.last is not None else None,
                                        amount=int(pos.quantity),
                                        )
                z_positions[asset] = z_position

            except Exception as e:
                self._logger.exception(f"Exception fetching position for symbol: {pos.symbol}")
                continue
        return z_positions

    def get_portfolio(self) -> ZpPortfolio:
        account = self.get_account_balance(account_number=self._get_account_number())
        z_portfolio = ZpPortfolio(portfolio_value=float(account.account_value_total),
                                  positions=self.get_positions(),
                                  positions_value=float(account.position_market_value),
                                  cash=float(account.cash),
                                  start_date=None,
                                  returns=0.0,
                                  starting_cash=0.0,
                                  capital_used=0.0,
                                  pnl=0.0
                                  )
        return z_portfolio

    def get_account(self) -> ZpAccount:
        account = self.get_account_balance(account_number=self._get_account_number())
        z_account = ZpAccount()
        z_account.buying_power = float(account.cash)
        z_account.total_position_value = float(account.position_market_value)
        return z_account

    def get_account_balance(self, account_number: str) -> AccountDetails:
        acc = next(filter(lambda x: x.account_number == account_number, self._lime_sdk_client.account.get_balances()),
                   None)
        if acc is None:
            raise Exception(f"Invalid account number {account_number}. Not found.")
        return acc

    def get_time_skew(self) -> pd.Timedelta:
        return pd.Timedelta('0 sec')  # TODO: use clock API

    def is_alive(self) -> bool:
        try:
            self._lime_sdk_client.account.get_balances()
            return True
        except Exception as _:
            return False

    def _order2zp(self, order: OrderDetails) -> ZPOrder | None:

        try:
            asset = symbol_lookup(order.symbol)
        except SymbolNotFound:
            return None

        zp_order = ZPOrder(
            id=order.client_order_id,
            asset=asset,
            amount=int(order.quantity) if order.order_side == OrderSide.BUY else -int(order.quantity),
            stop=float(order.stop_price) if order.stop_price is not None else None,  # No stop price support
            limit=float(order.price) if order.price is not None else None,
            filled=int(order.executed_quantity),
            dt=None,
            commission=0.0,
        )

        zp_order.status = ZP_ORDER_STATUS.OPEN
        if order.order_status == OrderStatus.CANCELED:
            zp_order.status = ZP_ORDER_STATUS.CANCELLED
        elif order.order_status == OrderStatus.REJECTED:
            zp_order.status = ZP_ORDER_STATUS.REJECTED
        elif order.order_status == OrderStatus.SUSPENDED:
            zp_order.status = ZP_ORDER_STATUS.SUSPENDED
        # elif order.order_status == OrderStatus.REPLACED:
        #     zp_order.status = ZP_ORDER_STATUS.REPLACED
        # elif order.order_status == OrderStatus.PENDING_CANCEL:
        #     zp_order.status = ZP_ORDER_STATUS.PENDING_CANCEL
        # elif order.order_status == OrderStatus.DONE_FOR_DAY:
        #     zp_order.status = ZP_ORDER_STATUS.DONE_FOR_DAY
        elif order.order_status == OrderStatus.NEW:
            zp_order.status = ZP_ORDER_STATUS.OPEN
        elif order.order_status == OrderStatus.PENDING_NEW:
            zp_order.status = ZP_ORDER_STATUS.OPEN
        elif order.order_status == OrderStatus.PARTIALLY_FILLED:
            zp_order.status = ZP_ORDER_STATUS.OPEN
            zp_order.filled = int(order.executed_quantity)
        elif order.order_status == OrderStatus.FILLED:
            zp_order.status = ZP_ORDER_STATUS.FILLED
            zp_order.filled = int(order.executed_quantity)
        return zp_order

    def _new_order_id(self) -> str:
        return uuid.uuid4().hex

    def order(self, asset: Asset, amount: int, style: ExecutionStyle):
        symbol = asset.symbol
        qty = amount if amount > 0 else -amount
        side = OrderSide.BUY if amount > 0 else OrderSide.SELL

        if isinstance(style, MarketOrder):
            order_type = OrderType.MARKET
        elif isinstance(style, LimitOrder):
            order_type = OrderType.LIMIT
        else:
            raise Exception(f"Unsupported order type: {style}.")

        limit_price = style.get_limit_price(side == OrderSide.BUY) or None
        stop_price = style.get_stop_price(side == OrderSide.BUY) or None

        zp_order_id = self._new_order_id()
        dt = pd.to_datetime('now', utc=True)
        zp_order = ZPOrder(
            dt=dt,
            asset=asset,
            amount=amount,
            stop=stop_price,
            limit=limit_price,
            id=zp_order_id,
        )
        order = Order(
            symbol=symbol,
            quantity=Decimal(qty),
            side=side,
            order_type=order_type,
            time_in_force=TimeInForce.DAY,
            price=limit_price,
            client_order_id=zp_order.id,
            account_number=self._get_account_number()
        )
        validated_order = self._lime_sdk_client.trading.validate_order(order=order)
        submitted_order = self._lime_sdk_client.trading.place_order(order=order)
        order_details = self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=zp_order.id)

        zp_order = self._order2zp(order=order_details)

        self._tracked_orders[order_details.client_order_id] = order_details
        return zp_order

    def _get_account_number(self) -> str:
        # TODO: add this as param
        return self._lime_sdk_client.account.get_balances()[0].account_number

    def get_orders(self) -> dict[str, ZPOrder]:
        # return {}
        current_active_orders = self._lime_sdk_client.trading.get_active_orders(
            account_number=self._get_account_number())

        for order in current_active_orders:
            self._tracked_orders[order.client_order_id] = order

        result = {}
        for o in self._tracked_orders.values():
            zipline_order = self._order2zp(order=o)
            if zipline_order is None:
                continue
            result[o.client_order_id] = zipline_order

        return result

    def get_orders_by_ids(self, order_ids: list[str]) -> list[OrderDetails]:
        result = []
        for order_id in order_ids:
            order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=order_id)
            result.append(order)
        return result

    def get_transactions(self):
        raise NotImplementedError("Use get_transactions_by_order_ids method.")

    def get_transactions_by_order_ids(self, order_ids: list[str]):
        results = {}

        for order_id in order_ids:
            order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(client_order_id=order_id)
            # self._lime_sdk_client.account.iterate_trades(account_number=self._get_account_number(),
            #                                          date=
            #
            #                                          ):
            if order.executed_timestamp is None:
                continue
            try:
                asset = symbol_lookup(order.symbol)
            except SymbolNotFound:
                continue
            tx = Transaction(
                asset=asset,
                amount=int(order.executed_quantity),
                dt=order.executed_timestamp,
                price=float(order.price),
                order_id=order.client_order_id,
                commission=0.0,
            )
            results[order.client_order_id] = tx
        return results

    def cancel_order(self, zp_order_id: str) -> None:
        try:
            order = self._lime_sdk_client.trading.get_order_details_by_client_order_id(order_id=zp_order_id)
            self._lime_sdk_client.trading.cancel_order(order_id=order.order_id)
        except Exception as e:
            logging.error(e)
            return

    def get_last_traded_dt(self, asset) -> pd.Timestamp:
        quote = self._lime_sdk_client.market.get_current_quote(asset.symbol)
        return pd.Timestamp(quote.date)

    def get_spot_value(self, assets, field, dt, data_frequency):
        assert (field in (
            'open', 'high', 'low', 'close', 'volume', 'price'))
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        if field in ('price',):
            quotes = self._lime_sdk_client.market.get_current_quotes(symbols=symbols)
            if assets_is_scalar:
                if field == 'price':
                    if len(quotes) == 0:
                        return np.nan
                    return float(quotes[-1].last)
                else:
                    if len(quotes) == 0:
                        return pd.NaT
                    return quotes[-1].last_timestamp
            else:
                return [
                    float(quote.last) if field == 'price' else quote.last_timestamp
                    for quote in quotes
                ]

        bars_list = self._lime_sdk_client.market.get_quotes_history(symbol=symbols[0], period=Period.MINUTE,
                                                                    from_date=dt,
                                                                    to_date=dt)
        if assets_is_scalar:
            if len(bars_list) == 0:
                return np.nan
            return getattr(bars_list[0], field)
        bars_map = {a.symbol: a for a in bars_list}
        return [
            getattr(bars_map[symbol], field)
            for symbol in symbols
        ]

    def get_realtime_bars(self, assets, data_frequency):
        # TODO: cache the result. The caller
        # (DataPortalLive#get_history_window) makes use of only one
        # column at a time.
        assets_is_scalar = not isinstance(assets, (list, set, tuple))
        is_daily = 'd' in data_frequency  # 'daily' or '1d'
        if assets_is_scalar:
            symbols = [assets.symbol]
        else:
            symbols = [asset.symbol for asset in assets]
        timeframe = Period.DAY if is_daily else Period.MINUTE
        if not symbols:
            return []
        dfs = []
        to_date = datetime.datetime.now(tz=datetime.timezone.utc)
        from_date = to_date - datetime.timedelta(days=1)
        for asset in assets if not assets_is_scalar else [assets]:

            bars_list = self._lime_sdk_client.market.get_quotes_history(symbol=asset.symbol, period=timeframe,
                                                                        from_date=from_date,
                                                                        to_date=to_date
                                                                        )
            bars_map = {a.symbol: a for a in bars_list}

            symbol = asset.symbol
            df = bars_map[symbol].df.copy()
            if df.index.tz is None:
                df.index = df.index.tz_localize(
                    'utc').tz_convert('America/New_York')
            df.columns = pd.MultiIndex.from_product([[asset, ], df.columns])
            dfs.append(df)
        return pd.concat(dfs, axis=1)
