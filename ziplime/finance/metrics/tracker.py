import datetime

import pandas as pd
import structlog
from exchange_calendars import ExchangeCalendar

from ziplime.finance.domain.ledger import Ledger
from ..domain.order import Order

from ..domain.transaction import Transaction
from ziplime.assets.domain.db.asset import Asset
from ...data.data_portal import DataPortal
from ...domain.data_frequency import DataFrequency
from ziplime.domain.event import Event


class MetricsTracker:
    """The algorithm's interface to the registered risk and performance
    metrics.

    Parameters
    ----------
    trading_calendar : TradingCalendar
        The trading calendar used in the simulation.
    first_session : pd.Timestamp
        The label of the first trading session in the simulation.
    last_session : pd.Timestamp
        The label of the last trading session in the simulation.
    capital_base : float
        The starting capital for the simulation.
    emission_rate : {'daily', 'minute'}
        How frequently should a performance packet be generated?
    data_frequency : {'daily', 'minute'}
        The data frequency of the data portal.
    metrics : list[Metric]
        The metrics to track.
    """

    @staticmethod
    def _execution_open_and_close(calendar: ExchangeCalendar, session: pd.Timestamp):
        # if session.tzinfo is not None:
        #     session = session.tz_localize(None)

        open_ = calendar.session_first_minute(session)
        close = calendar.session_close(session)

        execution_open = open_
        execution_close = close

        return execution_open, execution_close

    def __init__(
            self,
            data_portal: DataPortal,
            trading_calendar: ExchangeCalendar,
            first_session: datetime.datetime,
            last_session: datetime.datetime,
            capital_base: float,
            emission_rate: datetime.timedelta,
            data_frequency: datetime.timedelta,
            metrics,
    ):
        self.emission_rate = emission_rate

        self._logger = structlog.getLogger(__name__)

        self._trading_calendar = trading_calendar
        self._first_session = first_session
        self._last_session = last_session
        self._capital_base = capital_base

        self._current_session = first_session
        self._market_open, self._market_close = self._execution_open_and_close(
            calendar=trading_calendar,
            session=first_session,
        )
        self._data_portal = data_portal
        self._session_count = 0

        self._sessions = sessions = trading_calendar.sessions_in_range(
            start=first_session,
            end=last_session,
        )
        self._total_session_count = len(sessions)

        self._ledger = Ledger(trading_sessions=sessions, capital_base=capital_base,
                              data_portal=self._data_portal, data_frequency=data_frequency)

        # self._benchmark_source = NamedExplodingObject(
        #     "self._benchmark_source",
        #     "_benchmark_source is not set until ``handle_start_of_simulation``"
        #     " is called",
        # )
        self._benchmark_source = None
        if emission_rate == DataFrequency.MINUTE:

            def progress(self):
                return 1.0  # a fake value

        else:

            def progress(self):
                return self._session_count / self._total_session_count

        # don't compare these strings over and over again!
        self._progress = progress

        # bind all of the hooks from the passed metric objects.
        for hook in self._hooks:
            registered = []
            for metric in metrics:
                try:
                    registered.append(getattr(metric, hook))
                except AttributeError:
                    pass

            def closing_over_loop_variables_is_hard(registered=registered):
                def hook_implementation(*args, **kwargs):
                    for impl in registered:
                        impl(*args, **kwargs)

                return hook_implementation

            hook_implementation = closing_over_loop_variables_is_hard()

            hook_implementation.__name__ = hook
            setattr(self, hook, hook_implementation)

    def handle_start_of_simulation(self, benchmark_source):
        self._benchmark_source = benchmark_source

        self.start_of_simulation(
            ledger=self._ledger,
            emission_rate=self.emission_rate,
            trading_calendar=self._trading_calendar,
            sessions=self._sessions,
            benchmark_source=benchmark_source,
        )

    @property
    def portfolio(self):
        return self._ledger.portfolio

    @property
    def account(self):
        return self._ledger.account

    @property
    def positions(self):
        return self._ledger.position_tracker.positions

    def update_position(
            self,
            asset: Asset,
            amount: float | None = None,
            last_sale_price: float | None = None,
            last_sale_date: pd.Timestamp | None = None,
            cost_basis: float | None = None,
    ):
        self._ledger.position_tracker.update_position(
            asset=asset,
            amount=amount,
            last_sale_price=last_sale_price,
            last_sale_date=last_sale_date,
            cost_basis=cost_basis,
        )

    def process_transaction(self, transaction: Transaction):
        self._ledger.process_transaction(transaction=transaction)

    def handle_splits(self, splits: list[tuple[Asset, float]]):
        self._ledger.process_splits(splits=splits)

    def process_order(self, order: Order):
        self._ledger.process_order(order=order)

    def process_commission(self, commission: Event) -> None:
        self._ledger.process_commission(commission=commission)

    def process_close_position(self, asset: Asset, dt: pd.Timestamp) -> None:
        self._ledger.close_position(asset=asset, dt=dt, data_frequency=self.data_frequency)

    def capital_change(self, amount: float) -> None:
        self._ledger.capital_change(change_amount=amount)

    def sync_last_sale_prices(self, dt: pd.Timestamp, handle_non_market_minutes: bool = False) -> None:
        self._ledger.sync_last_sale_prices(
            dt=dt,
            handle_non_market_minutes=handle_non_market_minutes,
        )

    def handle_minute_close(self, dt: pd.Timestamp):
        """Handles the close of the given minute in minute emission.

        Parameters
        ----------
        dt : Timestamp
            The minute that is ending

        Returns
        -------
        A minute perf packet.
        """
        self.sync_last_sale_prices(dt=dt)

        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            "capital_base": self._capital_base,
            "minute_perf": {
                "period_open": self._market_open,
                "period_close": dt,
            },
            "cumulative_perf": {
                "period_open": self._first_session,
                "period_close": self._last_session,
            },
            "progress": self._progress(self),
            "cumulative_risk_metrics": {},
        }
        ledger = self._ledger
        ledger.end_of_bar(session_ix=self._session_count)
        self.end_of_bar(
            packet=packet,
            ledger=ledger,
            session=dt,
            session_ix=self._session_count,
            data_portal=self._data_portal,
        )
        return packet

    def handle_market_open(self, session_label: pd.Timestamp, data_portal: DataPortal) -> None:
        """Handles the start of each session.

        Parameters
        ----------
        session_label : Timestamp
            The label of the session that is about to begin.
        data_portal : DataPortal
            The current data portal.
        """
        ledger = self._ledger
        ledger.start_of_session(session_label=session_label)

        adjustment_reader = data_portal._bundle_data.adjustment_repository
        if adjustment_reader is not None:
            # this is None when running with a dataframe source
            ledger.process_dividends(
                next_session=session_label,
                adjustment_reader=adjustment_reader,
            )

        self._current_session = session_label

        cal = self._trading_calendar
        self._market_open, self._market_close = self._execution_open_and_close(
            calendar=cal,
            session=session_label,
        )

        self.start_of_session(ledger=ledger, session=session_label, data_portal=data_portal)

    def handle_market_close(self, dt: pd.Timestamp, data_portal: DataPortal):
        """Handles the close of the given day.

        Parameters
        ----------
        dt : Timestamp
            The most recently completed simulation datetime.
        data_portal : DataPortal
            The current data portal.

        Returns
        -------
        A daily perf packet.
        """
        completed_session = self._current_session

        if self.emission_rate == "daily":
            # this method is called for both minutely and daily emissions, but
            # this chunk of code here only applies for daily emissions. (since
            # it's done every minute, elsewhere, for minutely emission).
            self.sync_last_sale_prices(dt=dt)

        session_ix = self._session_count
        # increment the day counter before we move markers forward.
        self._session_count += 1

        packet = {
            "period_start": self._first_session,
            "period_end": self._last_session,
            "capital_base": self._capital_base,
            "daily_perf": {
                "period_open": self._market_open,
                "period_close": dt,
            },
            "cumulative_perf": {
                "period_open": self._first_session,
                "period_close": self._last_session,
            },
            "progress": self._progress(self),
            "cumulative_risk_metrics": {},
        }
        ledger = self._ledger
        ledger.end_of_session(session_ix=session_ix)
        self.end_of_session(
            packet=packet,
            ledger=ledger,
            session=completed_session,
            session_ix=session_ix,
            data_portal=data_portal,
        )

        return packet

    def handle_simulation_end(self):
        """When the simulation is complete, run the full period risk report
        and send it out on the results socket.
        """
        self._logger.info(
            f"Simulated {self._session_count} trading days\n first open: "
            f"{self._trading_calendar.session_open(self._first_session)}\n "
            f"last close: {self._trading_calendar.session_close(self._last_session)}",
        )

        packet = {}
        self.end_of_simulation(
            packet=packet,
            ledger=self._ledger,
            trading_calendar=self._trading_calendar,
            sessions=self._sessions,
            data_portal=self._data_portal,
            benchmark_source=self._benchmark_source,
        )
        return packet

    _hooks = (
        "start_of_simulation",
        "end_of_simulation",
        "start_of_session",
        "end_of_session",
        "end_of_bar",
    )
