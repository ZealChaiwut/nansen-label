# Phoenix Flipper

A BigQuery-based labeling system to identify "Phoenix Flipper" wallets that buy tokens during crisis events and profit from market recovery.

## Project Structure

```
phoenix-flipper/
├── README.md                    # This overview
├── .gitignore                   # Git ignore rules
│
├── prep/                        # Data preparation scripts
│   ├── requirements.txt         # Python dependencies
│   ├── 00_run_prep.py          # Master orchestrator  
│   ├── 01_test_bq.py           # BigQuery connection test
│   ├── 02_create_schemas.py     # Schema creation
│   ├── 03_generate_crisis_data.py # Generate crisis events
│   ├── 04_generate_price_history.py # Generate price history
│   ├── 05_generate_dex_pools.py # Generate DEX pools from Ethereum logs
│   ├── 06_verify_data_quality.py # Verify data joins and quality
│   └── README.md               # Setup instructions
│
├── lib/                        # Shared utilities
│   ├── __init__.py             # Python package init
│   ├── bigquery_helpers.py     # BigQuery utility functions
│   └── ethereum_udfs.sql      # Reusable Ethereum UDFs
│
└── schemas/                    # BigQuery table definitions
    ├── dim_dex_pools.sql       # DEX pools schema
    ├── dim_token_price_history.sql # Price history schema  
    ├── crisis_events_with_window.sql # Crisis events schema
    ├── stg_crisis_buyers.sql   # Crisis buyers schema
    ├── stg_profitable_flippers.sql # Profitable flippers schema
    └── dim_wallet_labels.sql   # Final labels schema
```

## Preparation and Setup

For detailed setup instructions, data generation, and pipeline execution, see:

**📋 [Setup Guide → `prep/README.md`](prep/README.md)**

The preparation scripts will:
- Create BigQuery schemas for 6 tables
- Generate realistic crisis events using popular tokens
- Pull real DEX pool addresses from Ethereum blockchain data
- Generate aligned price history with crisis drops and recovery
- Verify data quality and joinability

## Quick Start

```bash
# Install dependencies & run complete pipeline
pip install -r prep/requirements.txt
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper
```

## Pipeline Overview

1. **Crisis Detection** → Identify market crisis events with buy windows
2. **Real Pool Discovery** → Query Ethereum for actual Uniswap pool addresses  
3. **Price Generation** → Create realistic price movements around crisis events
4. **Data Validation** → Ensure crisis-price joins and pool existence
5. **Wallet Analysis** → (Future) Identify buyers during crisis windows
6. **Profit Calculation** → (Future) Calculate PnL for recovery periods
7. **Label Assignment** → (Future) Tag profitable "Phoenix Flipper" wallets
