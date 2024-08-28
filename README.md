# ziplime

Zipline wrapper which enables using Lime data for backtesting

## Installation

```Bash
poetry add ziplime
```

## Usage

You can find usage examples below. All commands supported by `zipline` are also supported by `ziplime` with extended
list of parameters for easier usage.

### Data ingestion

Data ingestion works by first fetching historical data and then and then running live data fetch in the background. 

Difference from original zipline:

- `--start-date` and `--end-date` parameters - used to fetch bundle data only for specific date period
- `--symbols` parameter - specifies symbols to fetch data for directly in the command
- Running live data fetch in the background

Example:
```Bash
poetry run python -m ziplime ingest -b lime --start-date 2024-08-01 --end-date 2024-08-27 --symbols AAPL,TSLA,AMZN
```

