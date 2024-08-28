import datetime

from ziplime.data.bundles.lime import register_lime_equities_bundle


def register_lime_symbol_list_equities_bundle(bundle_name: str, start_session: datetime.datetime,
                                              end_session: datetime.datetime,
                                              symbols: list[str]):
    register_lime_equities_bundle(
        bundle_name=bundle_name,
        start_session=start_session,
        end_session=end_session,
        symbol_list=symbols,
    )
