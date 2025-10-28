# Phoenix Flipper - Data Prep Scripts

Setup scripts for generating Phoenix Flipper test data and BigQuery schemas.

## Quick Start

```bash
# Install dependencies & run complete pipeline
pip install -r prep/requirements.txt
python prep/00_run_prep.py --target nansen-label.phoenix_flipper
```

## Prerequisites

1. **Python 3.11+** and **Google Cloud SDK** installed
2. **BigQuery authentication**: `gcloud auth application-default login`

## Command Options

```bash
# Basic usage
python prep/00_run_prep.py --target nansen-label.phoenix_flipper

# With options
python prep/00_run_prep.py --target nansen-label.phoenix_flipper \
  --hard-reset \          # Drop existing tables
  --no-prompt \          # Non-interactive mode
  --data-only            # Skip setup steps
```

## Tables Created

Creates 6 BigQuery tables:

1. **`crisis_events_with_window`** - Crisis events with contrarian buy windows (partitioned by date)
2. **`dim_dex_pools`** - DEX liquidity pool metadata from real Ethereum logs  
3. **`dim_token_price_history`** - Daily token prices and market data (partitioned by date)
4. **`stg_crisis_buyers`** - Wallets that bought tokens during crisis windows (M3 output)
5. **`stg_profitable_flippers`** - Crisis buyers who profited from recovery (M4 output)
6. **`dim_wallet_labels`** - Final Phoenix Flipper labels with success metrics (M5 output)

## Generated Data

**Crisis Events**: 12 events using popular tokens (UNI, MATIC, LINK, CRO, WBTC, SHIB) renamed as CRISIS1-6 with realistic crisis dates from 2021-2022 and 3-14 day buy windows.

**DEX Pools**: Real Uniswap V2 pool addresses from Ethereum blockchain - crisis tokens paired only with base tokens (DAI, USDC, USDT, WETH). No mock addresses.

**Price History**: Daily price data aligned with crisis events showing 15% drops during crisis and 8% recovery afterward, with normal 3% volatility otherwise.



