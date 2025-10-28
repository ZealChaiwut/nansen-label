# Phoenix Flipper - Data Prep Scripts

Setup scripts for generating Phoenix Flipper test data and BigQuery schemas.

## Quick Start

```bash
# Install dependencies & run complete pipeline
pip install -r prep/requirements.txt
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper
```

## Prerequisites

1. **Python 3.11+** and **Google Cloud SDK** installed
2. **BigQuery authentication**: `gcloud auth application-default login`

## Command Options

```bash
# Basic usage
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper

# With options
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper \
  --hard-reset \          # Drop existing tables
  --no-prompt \          # Non-interactive mode
  --data-only            # Skip setup steps
```

## Generated Data

**Crisis Events**: 12 events using popular tokens (UNI, MATIC, LINK, CRO, WBTC, SHIB) renamed as CRISIS1-6 with realistic crisis dates from 2021-2022 and 3-14 day buy windows.

**DEX Pools**: Real Uniswap V2/V3 pool addresses from Ethereum blockchain - crisis tokens paired only with base tokens (DAI, USDC, USDT, WETH). No mock addresses.

**Price History**: Daily price data aligned with crisis events showing 15% drops during crisis and 8% recovery afterward, with normal 3% volatility otherwise.

**Quality Verification**: Validates crisis-price joins and confirms all pool addresses exist in Ethereum transaction logs.


## Tables Created

Creates 6 BigQuery tables: DEX pools, price history (partitioned), crisis events (partitioned), staging tables for buyers/flippers, and final wallet labels.
