# Phoenix Flipper - Data Prep Scripts

Setup scripts for generating Phoenix Flipper test data and BigQuery schemas.

## Quick Start

```bash
# Install dependencies
pip install -r prep/requirements.txt

# Run complete pipeline
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper
```

## Prerequisites

1. **Python 3.11+** and **Google Cloud SDK** installed
2. **BigQuery authentication**: `gcloud auth application-default login`
3. **Install dependencies**: `pip install -r prep/requirements.txt`

## What Each Script Does

| Script | Purpose | What It Creates |
|--------|---------|-----------------|
| `00_run_prep.py` | **Master orchestrator** - runs all steps with interactive prompts | Complete pipeline execution |
| `01_test_bq.py` | Tests BigQuery connection and permissions | Connection verification |
| `02_create_schemas.py` | Creates all table schemas in BigQuery | 6 tables with proper partitioning |
| `03_generate_m1_data.py` | Generates foundation data (M1) | DEX pools + price history |
| `04_generate_m2_data.py` | Generates crisis events (M2) | Crisis events with buy windows |

## Command Options

### Master Orchestrator (`00_run_prep.py`)
```bash
# Basic usage
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper

# With options
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper \
  --hard-reset \          # Drop existing tables
  --pools 100 \          # Generate 100 DEX pools
  --crises 10 \          # Generate 10 crisis events
  --no-prompt            # Non-interactive mode
```

### Individual Scripts
```bash
# All scripts require --target when run standalone
python prep/02_create_schemas.py --target YOUR_PROJECT.phoenix_flipper --drop
python prep/03_generate_m1_data.py --target YOUR_PROJECT.phoenix_flipper --pools 50
python prep/04_generate_m2_data.py --target YOUR_PROJECT.phoenix_flipper --count 6
```

## Generated Data

### M1 Foundation Data
- **DEX Pools**: Metadata for 50+ pools using real crisis tokens (LOOKS, APE, SUSHI, XVS, OHM, wSOL)
- **Price History**: Daily price data with realistic market movements and crisis patterns

### M2 Crisis Events  
- **6 Real Historical Crises**:
  - LOOKS: Post-Launch Incentive Decline (March 2022)
  - APE: Post-Otherside Land Sale Crash (May 2022)  
  - SUSHI: Leadership Crisis & CTO Departure (Jan 2022)
  - XVS: Market Manipulation & Liquidations (May 2021)
  - OHM: DeFi 2.0 Rebase Token Collapse (Dec 2021)
  - wSOL: FTX Collapse Fallout (Nov 2022)

## Table Schemas Created

| Table | Type | Partitioned | Description |
|-------|------|-------------|-------------|
| `dim_dex_pools` | Reference | No | DEX pool metadata |
| `dim_token_price_history` | Time-series | Yes (`dt`) | Daily price/market data |
| `crisis_events_with_window` | Event | Yes (`dt`) | Crisis events with buy windows |
| `stg_crisis_buyers` | Staging | No | Crisis buyers (M3 output) |
| `stg_profitable_flippers` | Staging | No | Profitable flippers (M4 output) |
| `dim_wallet_labels` | Reference | No | Final Phoenix Flipper labels |

## Interactive vs Non-Interactive Mode

**Interactive Mode** (default): Prompts between each step for review  
**Non-Interactive Mode**: Runs all steps without prompts (use `--no-prompt`)

## File Structure

```
prep/
├── requirements.txt            # Python dependencies
├── 00_run_prep.py             # Master orchestrator  
├── 01_test_bq.py              # BigQuery connection test
├── 02_create_schemas.py       # Schema creation
├── 03_generate_m1_data.py     # M1: Foundation data
├── 04_generate_m2_data.py     # M2: Crisis events
└── README.md                  # This guide

schemas/
├── dim_dex_pools.sql          # DEX pools schema
├── dim_token_price_history.sql # Price history schema  
├── crisis_events_with_window.sql # Crisis events schema
├── stg_crisis_buyers.sql      # Crisis buyers schema
├── stg_profitable_flippers.sql # Profitable flippers schema
└── dim_wallet_labels.sql      # Final labels schema
```