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
| `03_generate_crisis_data.py` | Generates crisis events data | Crisis events with buy windows |
| `04_generate_price_history.py` | Generates price history data | Token prices aligned with crises |
| `05_generate_dex_pools.py` | Generates DEX pools data | Real pools from Ethereum logs |
| `06_verify_data_quality.py` | Verifies data quality | Crisis-price joins + pool validation |

## Command Options

### Master Orchestrator (`00_run_prep.py`)
```bash
# Basic usage
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper

# With options
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper \
  --hard-reset \          # Drop existing tables
  --no-prompt \          # Non-interactive mode
  --data-only            # Skip setup steps (pip install, BQ test)
```

### Individual Scripts
```bash
# All scripts require --target when run standalone
python prep/02_create_schemas.py --target YOUR_PROJECT.phoenix_flipper --drop
python prep/03_generate_crisis_data.py --target YOUR_PROJECT.phoenix_flipper --count 12
python prep/04_generate_price_history.py --target YOUR_PROJECT.phoenix_flipper
python prep/05_generate_dex_pools.py --target YOUR_PROJECT.phoenix_flipper
python prep/06_verify_data_quality.py --target YOUR_PROJECT.phoenix_flipper
```

## Generated Data

### Crisis Events (Generated First)  
- **12 Crisis Events**: 6 popular tokens renamed as crisis tokens (CRISIS1-6) + 6 additional mock events
- **Popular Token Base**: Using UNI, MATIC, LINK, CRO, WBTC, SHIB (guaranteed to have Uniswap pools)
- **Mock Crisis Names**: Market Manipulation, Governance Exploit, Flash Loan Attack, Exchange Delisting, etc.
- **Random Buy Windows**: 3-14 day windows after each crisis date
- **Historical Dates**: Based on realistic crisis dates from 2021-2022
- **Simplified Schema**: Just crisis ID, token address, dates, and buy window (no pool address needed)

### Foundation Data (Matches Crisis Events)
- **REAL DEX Pools ONLY**: 100% authentic pool addresses from BigQuery public Ethereum data
  - **Uniswap V2 Focus**: Query Uniswap V2 factory for actual pools containing popular tokens
  - **No Mock Data**: Only real pool addresses from blockchain - no fake addresses  
  - **Strategic Pairs**: Popular tokens (UNI, MATIC, LINK, CRO, WBTC, SHIB) paired ONLY with major base tokens (DAI, USDC, USDT, WETH)
  - **No Cross-Pairs**: Crisis tokens are NOT paired with each other - only with base tokens
  - **Quality Guarantee**: All pools verified to exist in Ethereum transaction logs (popular tokens = guaranteed pools)
  - **Best of Both Worlds**: Real pool addresses + controllable crisis narratives
- **Price History**: Simple price movements aligned with crisis events:
  - **Normal Periods**: 3% daily volatility random walk
  - **Crisis Surge**: 15% drops during crisis (days 0-3)
  - **Recovery Bounce**: 8% daily recovery after crisis (days 4-10)
  - **Market Data**: Volume increases with volatility, simple market cap/liquidity

**💡 Simplified Approach**: Price data now reads existing crisis events from BigQuery and generates matching price patterns. Much simpler to maintain and debug!

### Data Quality Verification (Step 5)
- **Crisis-Price Join Test**: Verifies price data aligns with crisis events and shows realistic drops/recovery
- **Pool-Ethereum Join Test**: Confirms ALL pool addresses exist in Ethereum logs (should be 100% real)
- **Data Completeness**: Ensures all tables have expected data
- **Random Sampling**: Picks 1-2 tokens to analyze price movements around crisis dates  
- **Real Pool Validation**: Verifies every pool has actual transaction history on Ethereum
- **Success Validation**: Pipeline only succeeds if data quality checks pass

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
**Data-Only Mode**: Skips setup steps and goes straight to database operations (use `--data-only`)

## File Structure

```
prep/
├── requirements.txt            # Python dependencies
├── 00_run_prep.py             # Master orchestrator  
├── 01_test_bq.py              # BigQuery connection test
├── 02_create_schemas.py        # Schema creation
├── 03_generate_crisis_data.py  # Generate crisis events first
├── 04_generate_price_history.py # Generate price history (matches crises)
├── 05_generate_dex_pools.py    # Generate DEX pools from Ethereum logs
├── 06_verify_data_quality.py  # Verify data joins and quality
└── README.md                   # This guide

schemas/
├── dim_dex_pools.sql          # DEX pools schema
├── dim_token_price_history.sql # Price history schema  
├── crisis_events_with_window.sql # Crisis events schema
├── stg_crisis_buyers.sql      # Crisis buyers schema
├── stg_profitable_flippers.sql # Profitable flippers schema
└── dim_wallet_labels.sql      # Final labels schema
```