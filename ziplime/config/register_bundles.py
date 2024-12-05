import datetime

from lime_trader.models.market import Period

from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS, FUNDAMENTAL_DATA_COLUMN_NAMES
from ziplime.data.bundles.lime import register_lime_equities_bundle


def register_lime_symbol_list_equities_bundle(bundle_name: str,
                                              start_session: datetime.datetime | None,
                                              end_session: datetime.datetime | None,
                                              symbols: list[str],
                                              fundamental_data_list: set[str] = FUNDAMENTAL_DATA_COLUMN_NAMES,
                                              period: Period = Period.DAY,
                                              calendar_name: str = "NYSE"
                                              ):
    register_lime_equities_bundle(
        bundle_name=bundle_name,
        start_session=start_session,
        end_session=end_session,
        symbol_list=symbols,
        period=period,
        calendar_name=calendar_name,
        fundamental_data_list=fundamental_data_list,
    )
