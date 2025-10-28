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
├── identify_crisis_buyers.py    # Milestone 3: Identify crisis buyers
├── calculate_pnl_leaderboard.py # Milestone 4: Calculate P&L and leaderboard
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
# Install dependencies & run data preparation pipeline
pip install -r prep/requirements.txt
python prep/00_run_prep.py --project YOUR_PROJECT --dataset phoenix_flipper

# Run crisis buyers analysis (Milestone 3)
python identify_crisis_buyers.py --target YOUR_PROJECT.phoenix_flipper

# Calculate P&L and show leaderboard (Milestone 4)
python calculate_pnl_leaderboard.py --target YOUR_PROJECT.phoenix_flipper

# With custom options
python calculate_pnl_leaderboard.py --target YOUR_PROJECT.phoenix_flipper --top-n 20 --recovery-days 60 --min-profit 25.0

# Or run dry-run to test without writing to BigQuery
python identify_crisis_buyers.py --target YOUR_PROJECT.phoenix_flipper --dry-run
python calculate_pnl_leaderboard.py --target YOUR_PROJECT.phoenix_flipper --dry-run
```

## Pipeline Overview

### Data Preparation (prep/)
1. **Crisis Detection** → Identify market crisis events with buy windows
2. **Real Pool Discovery** → Query Ethereum for actual Uniswap pool addresses  
3. **Price Generation** → Create realistic price movements around crisis events
4. **Data Validation** → Ensure crisis-price joins and pool existence

### Analysis Phase
5. **Wallet Analysis** → Identify buyers during crisis windows from Ethereum logs (`identify_crisis_buyers.py`)
   - Queries all available pools with configurable date range and transaction limits
   - Supports `--dry-run` for testing without writing to BigQuery (recommended for first run)
6. **Profit Calculation** → Calculate P&L for recovery periods (`calculate_pnl_leaderboard.py`)
   - Finds peak recovery prices within 90-day windows after purchases
   - Calculates profit percentages and USD amounts for each transaction
   - Shows top 10 leaderboard with detailed transaction breakdown
   - Stores profitable flippers in `stg_profitable_flippers` table
7. **Label Assignment** → (Future) Tag profitable "Phoenix Flipper" wallets

## Architecture Overview

_[To be filled: Data pipeline architecture, BigQuery schema design, ETL flow, data modeling approach]_

## Technology Choices and Trade-offs

**Approach**: BigQuery SQL via Python Client (+ Pandas for results) & BigQuery UDFs

### Pros
- **Scalability**: Leverages BigQuery's serverless engine for core SQL and UDF execution 🚀
- **Cost-Effective Processing**: Pay-per-query model efficient for analytics. UDF compute included
- **Direct Access to Public Data**: Easily queries Google's public blockchain datasets
- **Python Flexibility**: Python client orchestrates SQL/UDFs. Can use SQL, JS, or potentially Python for UDF logic, extending SQL capabilities server-side
- **GCP Integration**: Seamless integration with other GCP services

### Cons
- **SQL Dependency**: Core logic is still SQL-heavy, but UDFs help encapsulate complex logic
- **UDF Complexity/Limitations**: Writing, deploying, and managing UDFs adds complexity (especially JS/Python UDFs). UDFs have performance limits and resource constraints. Debugging UDFs can be harder than pure SQL
- **Potential Egress Costs**: Pulling large final results into local Pandas still incurs costs/bottlenecks

## Future Improvements Roadmap

_[To be filled: Planned features, scalability improvements, additional data sources, model enhancements]_
