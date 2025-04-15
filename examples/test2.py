from ziplime.domain.benchmark_spec import BenchmarkSpec
from ziplime.protocol import BarData

from ziplime.constants.fundamental_data import FundamentalData, FundamentalDataValueType
from ziplime.utils.run_algo import run_algorithm
from ziplime.data.bundles import load
from ziplime.utils.bundle_utils import register_default_bundles, get_exchange


import exchange_calendars as xcals
import pandas as pd
import numpy as np
import inspect
import sys
import os
import json

register_default_bundles()


def get_benchmark_returns(start, end):
    print('bench')
    bundle_data = load('lime')
    spx_asset = bundle_data.asset_repository.lookup_symbol('AAPL', as_of_date=None)
    nys = xcals.get_calendar("XNYS")
    trading_days = nys.sessions_in_range(start, end)
    spx_data = bundle_data.historical_data_reader.load_raw_arrays(
        columns=['close'],
        start_date=trading_days.min(),
        end_date=trading_days.max(),
        assets=[spx_asset],
    )
    date_range = bundle_data.historical_data_reader.sessions
    date_range = date_range[(date_range >= start) & (date_range <= end)]
    spx_prices = pd.Series(spx_data[0].flatten(), index=pd.to_datetime(date_range))
    spx_returns = spx_prices.pct_change().dropna()
    return spx_returns


def handler(event, context):
    try:
        print('handler')
        code_to_execute = event.get('code', '')
        param_start = event.get('start', '2024-11-01')
        param_end = event.get('end', '2024-12-01')
        param_capital = event.get('capital', 100000)
        param_frequency = event.get('freq', 'daily')
        param_bundle = event.get('bundle', 'lime')
        # exec(code_to_execute, globals())
        # exec(textwrap.dedent(event), globals())
        start_session = pd.Timestamp(param_start)
        end_session = pd.Timestamp(param_end)
        benchmark_returns = get_benchmark_returns(start_session, end_session)

        benchmark_spec = BenchmarkSpec(benchmark_returns=benchmark_returns,
                                       benchmark_file=None,
                                       benchmark_sid=None,
                                       benchmark_symbol=None,
                                       no_benchmark=False,
                                       )
        result = run_algorithm(start_date=start_session,
                               end_date=end_session,
                               print_algo=True,
                               algotext=code_to_execute,
                               capital_base=param_capital,
                               emission_rate=param_frequency,
                               bundle=param_bundle,
                               benchmark_spec=benchmark_spec,
                               exchange=get_exchange('lime-trader-sdk'),
                               )
        respose = {"period_open": result.period_open.to_json(orient='values'),
                   "period_close": result.period_close.to_json(orient='values'),
                   "portfolio_value": result.portfolio_value.to_json(orient='values'),
                   "pnl": result.pnl.to_json(orient='values'),
                   "sharpe": result.sharpe.to_json(orient='values'),
                   "sortino": result.sortino.to_json(orient='values'),
                   "max_drawdown": result.max_drawdown.to_json(orient='values'),
                   "excess_return": result.excess_return.to_json(orient='values'),
                   "alpha": result.alpha.to_json(orient='values'),
                   "beta": result.beta.to_json(orient='values'),
                   "longs_count": result.longs_count.to_json(orient='values'),
                   "shorts_count": result.shorts_count.to_json(orient='values'),
                   "long_exposure": result.long_exposure.to_json(orient='values'),
                   "short_exposure": result.short_exposure.to_json(orient='values'),
                   "long_value": result.long_value.to_json(orient='values'),
                   "short_value": result.short_value.to_json(orient='values'),
                   "gross_leverage": result.gross_leverage.to_json(orient='values'),
                   "net_leverage": result.net_leverage.to_json(orient='values'),
                   "max_leverage": result.max_leverage.to_json(orient='values'),
                   "capital_used": result.capital_used.to_json(orient='values'),
                   "returns": result.returns.to_json(orient='values'),
                   "benchmark_period_return": result.benchmark_period_return.to_json(orient='values'),
                   "algorithm_period_return": result.algorithm_period_return.to_json(orient='values'),
                   "benchmark_volatility": result.benchmark_volatility.to_json(orient='values'),
                   "algo_volatility": result.algo_volatility.to_json(orient='values'),
                   "portfolio": result.positions.to_json(orient='values')}
        return respose
    except Exception as e:
        return {
            "error": str(e),
        }


class MyStrategyV1Config:
    pass


### for local testing of lambda function
event = {
    'code': """
from ziplime.api import order_target_percent, symbol
def initialize(context):
    # Define the portfolio of 10 S&P 500 symbols, excluding FB
    context.assets = [
        symbol('AAPL'),  # Apple Inc.
        symbol('MSFT'),  # Microsoft Corporation
        symbol('AMZN'),  # Amazon.com, Inc.
        symbol('GOOGL'), # Alphabet Inc. (Class A)
        symbol('BRK.B'), # Berkshire Hathaway Inc. (Class B)
        symbol('JNJ'),   # Johnson & Johnson
        symbol('V'),     # Visa Inc.
        symbol('PG'),    # Procter & Gamble Co.
        symbol('NVDA'),  # NVIDIA Corporation
        symbol('TSLA')   # Tesla, Inc.
    ]
def handle_data(context, data):
    # Invest full capital equally across the defined assets
    for asset in context.assets:
        order_target_percent(asset, 0.1)  # Allocate 10% of total capital to each asset
""",
    'start': '2024-11-01',
    'end': '2024-12-01',
    'capital': 100000,
    'freq': 'daily',
    'bundle': 'lime'
}
context = {}
response = handler(event, context)
print(response)
print(json.dumps(response, indent=4))

# 1. Missing fundamental data - 2023-09-30 for AAPL # FIXED
# 2. Values are not denormalized # FIXED
# 3. Remove FutureWarning: The behavior of DataFrame concatenation with empty or all NA entries # FIXED
# 4. Print if import failed from some
# 5. If Bar count is too big don't throw error # FIXED
# 6. Check time zone when after midnight and import date is not same as u current date


#
# poetry run python -m ziplime ingest -b lime
# --period day --start-date 2004-11-01 --end-date 2024-11-26
# --symbols SPX,TTWO,ABNB,IDXX,BKNG,JNJ,BALL,CPB,BRK.B,NFLX,DIS,SYY,DD,AMT,ORLY,MRO,UPS,ENPH,ABT,TJX,CARR,TAP,SCHW,JKHY,ANET,UDR,TMO,HLT,DLTR,LYB,CSGP,K --fundamental-data return_on_tangible_equity_value,return_on_tangible_equity_ttm
#
