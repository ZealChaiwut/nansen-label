# Phoenix Flipper

A BigQuery-based labeling system to identify "Phoenix Flipper" wallets that buy tokens during crisis events and profit from market recovery.

## Setup Guide

For detailed setup instructions, data generation, and pipeline execution, see:

**📋 [Setup Guide → `prep/README.md`](prep/README.md)**

The preparation scripts will:
- Create BigQuery schemas for 6 tables
- Generate realistic crisis events using popular tokens
- Pull real DEX pool addresses from Ethereum blockchain data
- Generate aligned price history with crisis drops and recovery
- Verify data quality and joinability

### Project Structure

```
phoenix-flipper/
├── README.md                    # This overview
├── .gitignore                   # Git ignore rules
├── 01_identify_crisis_buyers.py    # Step 1: Identify crisis buyers
├── 02_calculate_pnl_leaderboard.py # Step 2: Calculate P&L and leaderboard
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

## Quick Start

```bash
# Install dependencies & run data preparation pipeline
pip install -r prep/requirements.txt
python prep/00_run_prep.py --target nansen-label.phoenix_flipper

# Step 1: Run crisis buyers analysis
python 01_identify_crisis_buyers.py --target nansen-label.phoenix_flipper

# Step 2: Calculate P&L and show leaderboard
python 02_calculate_pnl_leaderboard.py --target nansen-label.phoenix_flipper
```

## Architecture Overview

**Simple Python + BigQuery Pipeline**

- **Data Sources:** Ethereum blockchain logs (via BigQuery public datasets) + generated crisis events
- **Processing:** Python scripts orchestrate BigQuery SQL transformations and analysis
- **Storage:** All data stored in BigQuery tables with date partitioning for performance
- **Pipeline Flow:** Sequential Python scripts → BigQuery operations → Staged results → Final labels
- **Schema Design:** Star schema with dimension tables (`dim_*`) and staging tables (`stg_*`)
- **Scalability:** Serverless BigQuery handles large-scale blockchain data processing

## Technology Choices and Trade-offs

**Approach**: BigQuery via Python (Google SDK)

### Pros
- **Scalability**: Leverages BigQuery's serverless engine for core SQL and UDF execution
- **Cost-Effective Processing**: Pay-per-query model efficient for analytics. UDF compute included
- **Direct Access to Public Data**: Easily queries Google's public blockchain datasets
- **Python Flexibility**: Python client orchestrates SQL/UDFs. Can use SQL, or Python for UDF logic for debugging and development easeness
- **GCP Integration**: Seamless integration with other GCP services

### Cons
- **SQL Dependency**: Core logic is still SQL-heavy, but UDFs help encapsulate complex logic
- **Local limitation and bottlenecks**: Using Python on local and pandas has limitation on the memory and bottlenecks

## Core Milestone Overview

1. **Identify Potential Crisis Events:** Find tokens that had a sharp, token-specific price crash or liquidity drain on DEXs, filtering out general market drops.
2. **Define the Contrarian Buy Window:** Determine the specific timeframe *after* the initial crash when a "Phoenix Flipper" would likely buy (e.g., 12-84 hours post-crash).
3. **Identify Wallets Buying During the Window:** Find the specific wallets that acquired the crashed token via DEX swaps within that defined timeframe.
4. **Measure Recovery & Estimate Profit:** Check if the token's price recovered significantly later and estimate if the wallets identified in M3 achieved substantial gains (e.g., >3x).
5. **Assign the "Phoenix Flipper" Label:** Tag the wallets meeting the criteria from M4 in the final labels table.

## Implementation Scope

This implementation focuses on demonstrating the core **on-chain analysis and P&L estimation logic** (Milestones 3 & 4) for the "Phoenix Flipper" label, producing a ranked list of the top 100 potential flipper wallets based on estimated gains.

### Analysis Phase
**Step 1. Crisis Buyers** → Identify buyers during crisis windows from Ethereum logs (`01_identify_crisis_buyers.py`)
- Queries all available pools with configurable date range and transaction limits
- Stores individual buy transactions in `stg_crisis_buyers` table
- Supports `--dry-run` for testing without writing to BigQuery (recommended for first run)

**Step 2. P&L Leaderboard** → Calculate profit and loss for recovery periods (`02_calculate_pnl_leaderboard.py`)
- Finds peak recovery prices within 90-day windows after purchases
- Calculates profit percentages and USD amounts for each transaction
- Shows top 10 leaderboard with detailed transaction breakdown
- Stores profitable flippers above 10% profit threshold in `stg_profitable_flippers` table

### Data Simulation & Prep
- **Milestone 2 (Simulated First):** We will **simulate** the output of the crisis event detection. This involves defining a small set of specific `crisis_token_address`-`pool_address` pairs (targeting Uniswap V2) and their corresponding `buy_window_start`/`buy_window_end` timestamps directly in the SQL using `WITH` clauses (CTEs).
- **Milestone 1 (Simulated Second):** Pool metadata (`dim_dex_pools`) and necessary daily price/liquidity data (`fct_pool_liquidity_daily`) for *only* the specific pools/tokens/dates identified in the simulated M2 output will be **simulated** using CTEs.

## Future Works

- **Automated Crisis Detection** → Build full M1/M2 pipeline for real-time crisis event identification
- **Multi-DEX Support** → Expand beyond Uniswap V2 to include SushiSwap, Curve, and other DEXs
- **Realized P&L Tracking** → Track actual sell transactions to calculate true realized profits
- **Advanced Wallet Profiling** → Implement detailed holding checks and precise P&L accounting
- **Production Label Pipeline** → Complete M5 integration into `dim_wallet_labels` table
- **Data Quality Monitoring** → Add unit testing and automated data validation systems
- **Scalable Infrastructure** → Move from local Python/pandas to distributed processing for large datasets

