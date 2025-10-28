# Phoenix Flipper Project - Environment Setup

This directory contains setup scripts for the Phoenix Flipper data pipeline project. Follow this guide to set up your development environment and run the preparation scripts.

## 🚀 Quick Start - Prep Orchestrator

**NEW!** Use the master prep orchestrator to run everything at once:

```bash
# Run complete prep pipeline
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper

# Hard reset: Drop all tables and recreate everything
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --hard-reset

# Custom settings
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --pools 100 --crises 10
```

**Prep Options:**
- `--project`: BigQuery project ID (required)
- `--dataset`: BigQuery dataset ID (required)
- `--hard-reset`: Drop all existing tables before recreating (⚠️ destroys data)
- `--pools`: Number of DEX pools to generate (default: 50)
- `--crises`: Number of crisis events to generate (default: 6)
- `--skip-test`: Skip BigQuery connection test
- `--no-prompt`: Skip interactive prompts between steps (run non-interactively)

---

## Prerequisites

### 1. Python Installation

```bash
# Using Homebrew (recommended)
brew install python@3.11

# Or download from python.org
# Visit: https://www.python.org/downloads/
```

### 2. Google Cloud SDK Installation

```bash
# Using Homebrew
brew install google-cloud-sdk

# Or using installer
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
```

### 3. Authentication Setup

```bash
# Initialize gcloud CLI
gcloud init

# Authenticate for application default credentials
gcloud auth application-default login

# Set your project (replace with your actual project ID)
gcloud config set project nansen-label
```

### 4. Python Environment Setup

```bash
# Create virtual environment (recommended)
python3 -m venv phoenix_flipper_env
source phoenix_flipper_env/bin/activate

# Install required packages
pip install google-cloud-bigquery pandas numpy
```

## Project Setup Steps

Run these scripts in order to set up your BigQuery environment:

### Step 1: Test BigQuery Connection
```bash
python prep/01_test_bq.py
```

**What it does:**
- Tests connection to BigQuery
- Queries a public dataset to verify access
- Saves sample data to verify functionality

### Step 2: Create All Table Schemas
```bash
# Create schemas (preserve existing data)
python prep/02_create_schemas.py

# OR drop and recreate all tables
python prep/02_create_schemas.py --drop

# Use different project/dataset
python prep/02_create_schemas.py --target my-project.my-dataset --drop
```

**What it creates:**
- `dim_dex_pools` - DEX pool metadata (reference table, no partitioning)
- `dim_token_price_history` - Daily price and market data (partitioned by `dt`)
- `crisis_events_with_window` - Crisis events with buy windows (partitioned by `dt`)
- `stg_crisis_buyers` - Wallets that bought during crisis windows (staging table, no partitioning)
- `stg_profitable_flippers` - Buyers who profited from recovery (staging table, no partitioning)
- `dim_wallet_labels` - Final wallet labels with PnL metrics (reference table, no partitioning)

### Step 3: Generate Test Data

Generate realistic test data based on historical crisis events using milestone-specific scripts:

```bash
# Generate M1 foundation data (DEX pools + price history)
python prep/03_generate_m1_data.py --pools 50

# Generate M2 crisis detection data
python prep/04_generate_m2_data.py --crises 6

# Use different project/dataset
python prep/03_generate_m1_data.py --target my-project.my-dataset --pools 100
python prep/04_generate_m2_data.py --target my-project.my-dataset --crises 10
```

**What it does:**

**M1 (Foundation Data)**:
- Creates the `phoenix_flipper` dataset in BigQuery (if not exists)
- Generates DEX pool data using real crisis tokens (LOOKS, APE, SUSHI, XVS, OHM, wSOL)
- Generates realistic daily price history data for the 6 crisis tokens
- Loads data into `dim_dex_pools` and `dim_token_price_history` tables

**M2 (Crisis Detection)**:
- Generates realistic crisis events based on historical data (default: 6 real events)
- Loads data into `crisis_events_with_window` table

**Real Crisis Events Included:**
- **LOOKS**: Post-Launch Incentive Decline (March 2022)
- **APE**: Post-Otherside Land Sale Crash (May 2022)
- **SUSHI**: Leadership Crisis & CTO Departure (Jan 2022)
- **XVS**: Market Manipulation & Liquidations (May 2021)
- **OHM**: DeFi 2.0 Rebase Token Collapse (Dec 2021)
- **wSOL**: FTX Collapse Fallout (Nov 2022)

*Note: Price drops and severity metrics can be calculated from the price history data when needed.*

**Price History Data:**
- Daily price records from token launch dates to present (date stored in `dt` field)
- Realistic price movements leading up to and during crisis events
- Market data including volume, market cap, liquidity, daily highs/lows
- Partitioned by `dt` (DATE field) for efficient date-based querying
- Recovery patterns following major price drops

**Milestone 1 Options** (`03_generate_m1_data.py`):
- `--target`: BigQuery target in format PROJECT_ID.DATASET_ID (required unless using pipeline)
- `--pools`: Number of DEX pools to generate (default: 50)

**Milestone 2 Options** (`04_generate_m2_data.py`):
- `--target`: BigQuery target in format PROJECT_ID.DATASET_ID (required unless using pipeline)
- `--count`: Number of crisis events to generate (default: 6 - covers all real events)

All tables are partitioned by `created_timestamp` for efficient querying.

## Troubleshooting

### Common Issues

**"Permission denied" errors:**
```bash
# Make scripts executable
chmod +x prep/*.py

# Or run with python explicitly
python prep/01_test_bq.py
```

**BigQuery authentication errors:**
```bash
# Re-authenticate
gcloud auth application-default login

# Check current project
gcloud config get-value project

# Set correct project (or use --target argument)
gcloud config set project your-project-id
```

**Python package import errors:**
```bash
# Ensure virtual environment is activated
source phoenix_flipper_env/bin/activate

# Reinstall packages
pip install --upgrade google-cloud-bigquery pandas numpy
```

**BigQuery quota/billing errors:**
- Ensure billing is enabled for your Google Cloud project
- Check BigQuery quotas in the Google Cloud Console
- Verify project permissions (BigQuery Admin or Editor role required)

## Next Steps

After completing the environment setup:
1. All BigQuery tables will be created and ready
2. M1 foundation data (pools, price history) will be populated
3. M2 crisis detection data (events, windows) will be populated  
4. You can proceed with implementing M3+ pipeline milestones
5. Use the created schemas as the foundation for your data processing scripts

## File Structure

```
prep/
├── README.md                    # This setup guide
├── 00_run_prep.py              # 🚀 Master prep orchestrator (NEW!)
├── 01_test_bq.py               # BigQuery connection test
├── 02_create_schemas.py        # Schema creation script
├── 03_generate_m1_data.py      # M1: Foundation data (pools, price history)
└── 04_generate_m2_data.py      # M2: Crisis detection data (events)

schemas/
├── dim_dex_pools.sql           # DEX pools reference table (no partitioning)
├── dim_token_price_history.sql # Token price history (partitioned by dt)
├── crisis_events_with_window.sql  # Crisis events (partitioned by dt)
├── stg_crisis_buyers.sql       # Crisis buyers staging table (no partitioning)
├── stg_profitable_flippers.sql # Profitable flippers staging table (no partitioning)
└── dim_wallet_labels.sql       # Final labels reference table (no partitioning)
```

## Environment Variables

When using the prep orchestrator (`00_run_prep.py`), it sets these environment variables for all child processes:
- `PROJECT_ID`: BigQuery project ID
- `DATASET_ID`: BigQuery dataset ID

Individual scripts can use these as defaults, but still require `--target` when run standalone.

## Usage Examples

```bash
# Complete prep setup
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper

# Hard reset (⚠️ destroys existing data) 
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --hard-reset

# Custom data generation
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --pools 100 --crises 10

# Skip connection test
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --skip-test

# Non-interactive mode (no prompts)
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --no-prompt
```

## Interactive Mode (Default)

By default, the prep orchestrator runs in **interactive mode**, which provides:

✅ **Before Each Step**: Clear description of what's about to happen  
✅ **After Each Step**: Review output and press Enter to continue  
✅ **Error Handling**: Stops immediately if any step fails  
✅ **Progress Tracking**: Visual indicators and step-by-step execution  

### Example Interactive Flow:
```
📝 About to start: Step 1
📄 Description: Test BigQuery connection and list available datasets
============================================================
Press Enter to start this step...

🚀 Step 1: Testing BigQuery Connection
============================================================
Running: python prep/01_test_bq.py
✓ BigQuery client initialized successfully
✅ Step 1: Testing BigQuery Connection completed successfully

============================================================
📋 Step completed! Review the output above.
Press Enter to continue to the next step...
```

### Non-Interactive Mode

For automated scripts or CI/CD pipelines, use `--no-prompt`:

```bash
python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --no-prompt
```
