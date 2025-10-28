#!/usr/bin/env python3
"""
Milestone 3: Identify Crisis Buyers
"""

import sys
import os
import argparse
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from collections import namedtuple

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent / "lib"))
from bigquery_helpers import create_query_with_udfs, ETHEREUM_CONSTANTS

# Configuration
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])

# Query Configuration
QUERY_START_DATE = '2021-05-01'
QUERY_END_DATE = '2023-01-01'
QUERY_LIMIT = 1000000

def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Identify wallets that bought tokens during crisis windows')
    
    project_id = os.environ.get('PROJECT_ID', '')
    dataset_id = os.environ.get('DATASET_ID', '')
    default_target = f"{project_id}.{dataset_id}" if project_id and dataset_id else ""
    
    parser.add_argument('--target', 
                       required=not bool(default_target),
                       default=default_target,
                       help='Target in format PROJECT_ID.DATASET_ID')
    
    parser.add_argument('--dry-run', 
                       action='store_true',
                       help='Run analysis without writing results to BigQuery')
    
    args = parser.parse_args()
    
    if '.' not in args.target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    project_id, dataset_id = args.target.split('.', 1)
    return BigQueryConfig(project_id=project_id, dataset_id=dataset_id), args.dry_run

def identify_crisis_buyers(config):
    """
    Main orchestration function to identify crisis buyers from Ethereum transaction logs.
    
    Returns DataFrame with crisis buyer data ready for BigQuery loading.
    """
    client = bigquery.Client()
    print("🔍 Starting step-by-step crisis buyer analysis...")
    
    # Step 1: Load crisis events
    crisis_df = get_crisis_events(client, config)
    
    # Step 2: Find DEX pools for crisis tokens  
    pools_df = get_crisis_pools(client, config, crisis_df)
    
    # Step 3: Query Ethereum swap logs
    pool_addresses = list(pools_df['pool_address'].unique())
    swaps_df = get_ethereum_swaps(client, pool_addresses)
    
    # Step 4: Filter swaps within crisis windows
    crisis_swaps_df = filter_crisis_window_swaps(swaps_df, pools_df)
    
    # Step 5: Identify crisis token buyers
    buyers_df = identify_token_buyers(crisis_swaps_df)
    
    # Step 6: Format individual buy records for BigQuery (no aggregation)
    final_df = format_individual_buys(buyers_df, config)
    
    return final_df


def get_crisis_events(client, config):
    """Step 1: Load crisis events with buy windows."""
    print("Step 1: Loading crisis events...")
    
    query = f"""
    SELECT 
      crisis_id,
      token_address,
      window_start_date,
      window_end_date,
      crisis_name
    FROM `{config.project_id}.{config.dataset_id}.crisis_events_with_window`
    ORDER BY crisis_id
    """
    
    df = execute_query_step(client, query, "crisis events")
    if len(df) == 0:
        raise Exception("No crisis events found")
    
    print(f"  → Found {len(df)} crisis events")
    print(f"  → Crisis tokens: {list(df['token_address'].unique())}")
    
    return df


def get_crisis_pools(client, config, crisis_df):
    """Step 2: Find DEX pools for crisis tokens."""
    print("\nStep 2: Finding DEX pools for crisis tokens...")
    
    query = f"""
    SELECT DISTINCT
      p.pool_address,
      p.token0_address,
      p.token1_address,
      p.dex_protocol,
      c.crisis_id,
      c.token_address AS crisis_token,
      c.window_start_date,
      c.window_end_date,
      c.crisis_name
    FROM `{config.project_id}.{config.dataset_id}.dim_dex_pools` p
    INNER JOIN `{config.project_id}.{config.dataset_id}.crisis_events_with_window` c ON (
      p.token0_address = c.token_address OR 
      p.token1_address = c.token_address
    )
    ORDER BY c.crisis_id, p.pool_address
    """
    
    df = execute_query_step(client, query, "crisis pools mapping")
    if len(df) == 0:
        raise Exception("No DEX pools found for crisis tokens")
    
    print(f"  → Found {len(df)} pool-crisis combinations")
    print(f"  → Unique pools: {df['pool_address'].nunique()}")
    print(f"  → Crisis tokens with pools: {df['crisis_token'].nunique()}")
    
    return df


def get_ethereum_swaps(client, pool_addresses):
    """Step 3: Query Ethereum swap logs for relevant pools."""
    print("\nStep 3: Querying Ethereum swap logs...")
    
    # Use all available pools
    pool_addresses_sql = "', '".join(pool_addresses)
    
    print(f"  → Querying {len(pool_addresses)} pools")
    print(f"  → Date range: {QUERY_START_DATE} to {QUERY_END_DATE}")
    print(f"  → Transaction limit: {QUERY_LIMIT:,}")
    
    main_query = f"""
    SELECT
      logs.block_timestamp,
      logs.transaction_hash,
      logs.log_index,
      logs.address AS pool_address,
      logs.topics,
      logs.data,
      -- Extract transaction sender from transaction data
      txns.from_address AS wallet_address
    FROM `bigquery-public-data.crypto_ethereum.logs` logs
    LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` txns 
      ON logs.transaction_hash = txns.hash
    WHERE logs.topics[SAFE_OFFSET(0)] IN ('{ETHEREUM_CONSTANTS['V2_SWAP_TOPIC']}', '{ETHEREUM_CONSTANTS['V3_SWAP_TOPIC']}')
      AND logs.block_timestamp >= '{QUERY_START_DATE}'
      AND logs.block_timestamp <= '{QUERY_END_DATE}'
      AND logs.address IN ('{pool_addresses_sql}')
    ORDER BY logs.block_timestamp DESC
    LIMIT {QUERY_LIMIT}
    """
    
    # Use UDFs for enhanced Ethereum processing
    query = create_query_with_udfs(main_query)
    df = execute_query_step(client, query, "Ethereum swap logs")
    
    if len(df) == 0:
        print("  ⚠️  No swap logs found for the specified pools...")
        return pd.DataFrame()
    
    print(f"  → Found {len(df)} swap transactions")
    print(f"  → Date range: {df['block_timestamp'].min()} to {df['block_timestamp'].max()}")
    print(f"  → Unique wallets: {df['wallet_address'].nunique()}")
    
    return df


def filter_crisis_window_swaps(swaps_df, pools_df):
    """Step 4: Filter swaps that occurred within crisis buy windows."""
    print("\nStep 4: Filtering swaps within crisis windows...")
    
    if len(swaps_df) == 0:
        print("  ⚠️  No swap data to filter")
        return pd.DataFrame()
    
    crisis_swaps_data = []
    
    for _, pool_row in pools_df.iterrows():
        pool_swaps = swaps_df[swaps_df['pool_address'] == pool_row['pool_address']]
        
        if len(pool_swaps) == 0:
            continue
            
        # Filter by crisis window dates
        pool_swaps = pool_swaps.copy()
        pool_swaps['block_date'] = pd.to_datetime(pool_swaps['block_timestamp']).dt.date
        
        window_swaps = pool_swaps[
            (pool_swaps['block_date'] >= pool_row['window_start_date']) &
            (pool_swaps['block_date'] <= pool_row['window_end_date'])
        ]
        
        if len(window_swaps) > 0:
            for _, swap_row in window_swaps.iterrows():
                crisis_swaps_data.append({
                    'crisis_id': pool_row['crisis_id'],
                    'crisis_token': pool_row['crisis_token'],
                    'crisis_name': pool_row['crisis_name'],
                    'token0_address': pool_row['token0_address'],
                    'token1_address': pool_row['token1_address'],
                    'dex_protocol': pool_row['dex_protocol'],
                    'wallet_address': swap_row['wallet_address'],
                    'block_timestamp': swap_row['block_timestamp'],
                    'transaction_hash': swap_row['transaction_hash'],
                    'topics': swap_row['topics'],
                    'data': swap_row['data']
                })
    
    if len(crisis_swaps_data) == 0:
        print("  ⚠️  No swaps found within crisis windows")
        return pd.DataFrame()
    
    df = pd.DataFrame(crisis_swaps_data)
    print(f"  → Found {len(df)} swaps during crisis windows")
    print(f"  → Unique wallets: {df['wallet_address'].nunique()}")
    print(f"  → Crisis events with activity: {df['crisis_id'].nunique()}")
    
    return df


def identify_token_buyers(crisis_swaps_df):
    """Step 5: Process swaps to identify crisis token buyers."""
    print("\nStep 5: Processing swaps to identify crisis token purchases...")
    
    if len(crisis_swaps_df) == 0:
        print("  ⚠️  No crisis swaps to process")
        return pd.DataFrame()
    
    buyers_data = []
    processed = 0
    
    for _, row in crisis_swaps_df.iterrows():
        processed += 1
        if processed % 100 == 0:
            print(f"    → Processing swap {processed}/{len(crisis_swaps_df)}")
            
        try:
            # Analyze if this swap bought the crisis token
            is_buy, token_amount = analyze_swap_for_crisis_token(row, ETHEREUM_CONSTANTS['V2_SWAP_TOPIC'], ETHEREUM_CONSTANTS['V3_SWAP_TOPIC'])
            
            if is_buy and token_amount > 0 and row['wallet_address'] and row['wallet_address'] != ETHEREUM_CONSTANTS['ZERO_ADDRESS']:
                buyers_data.append({
                    'crisis_id': row['crisis_id'],
                    'crisis_name': row['crisis_name'],
                    'wallet_address': row['wallet_address'],
                    'token_address': row['crisis_token'],
                    'block_timestamp': row['block_timestamp'],
                    'transaction_hash': row['transaction_hash'],
                    'token_amount': token_amount,
                    'dex_protocol': row['dex_protocol']
                })
                
        except Exception as e:
            print(f"    Warning: Failed to process swap {row.get('transaction_hash', 'unknown')}: {e}")
            continue
    
    if len(buyers_data) == 0:
        print("  ⚠️  No crisis token buyers identified")
        return pd.DataFrame()
    
    df = pd.DataFrame(buyers_data)
    print(f"  → Identified {len(df)} crisis token purchase transactions")
    print(f"  → Unique crisis buyers: {df['wallet_address'].nunique()}")
    print(f"  → Crisis events with buyers: {df['crisis_id'].nunique()}")
    
    return df


def format_individual_buys(buyers_df, config):
    """Step 6: Format individual buy records for BigQuery (no aggregation)."""
    print("\nStep 6: Formatting individual buy records for BigQuery...")
    
    if len(buyers_df) == 0:
        print("  ⚠️  No buyer data to format")
        return pd.DataFrame()
    
    # Rename columns to match BigQuery schema for individual transactions
    final_df = buyers_df.rename(columns={
        'block_timestamp': 'first_buy_timestamp',
        'token_amount': 'total_amount_bought'
    })
    
    # Add num_transactions field
    final_df['num_transactions'] = 1    # Each record represents 1 transaction
    
    # Remove fields not needed in final schema
    columns_to_drop = ['crisis_name', 'dex_protocol', 'transaction_hash']
    final_df = final_df.drop(columns=[col for col in columns_to_drop if col in final_df.columns])
    
    # Get price and USD value for each transaction
    final_df = calculate_price_and_usd_spent(final_df, config)
    
    # Ensure correct data types for BigQuery schema compliance
    final_df = format_for_bigquery_schema(final_df)
    
    # Sort by crisis and transaction timestamp
    final_df = final_df.sort_values(['crisis_id', 'first_buy_timestamp'], ascending=[True, False])
    
    print(f"  → Final result: {len(final_df)} individual crisis buy transactions")
    if len(final_df) > 0:
        print(f"  → Largest single purchase: {final_df['total_amount_bought'].max():.6f} tokens")
        print(f"  → Unique wallets: {final_df['wallet_address'].nunique()}")
        print(f"  → Crisis events with buys: {final_df['crisis_id'].nunique()}")
    
    return final_df


def calculate_price_and_usd_spent(df, config):
    """Calculate first_buy_price and total_usd_spent by joining with price history."""
    print("  → Calculating prices and USD values from price history...")
    
    if len(df) == 0:
        return df
    
    try:
        # Get BigQuery client
        client = bigquery.Client()
        
        # Use config for project and dataset info
        project_id = config.project_id
        dataset_id = config.dataset_id
        
        # Query price history for all tokens and dates in our dataset
        unique_tokens = df['token_address'].unique()
        tokens_sql = "', '".join(unique_tokens)
        
        min_date = df['first_buy_timestamp'].min().date()
        max_date = df['first_buy_timestamp'].max().date()
        
        price_query = f"""
        SELECT 
          token_address,
          dt as price_date,
          price_usd
        FROM `{project_id}.{dataset_id}.dim_token_price_history`
        WHERE token_address IN ('{tokens_sql}')
          AND dt BETWEEN '{min_date}' AND '{max_date}'
        ORDER BY token_address, dt
        """
        
        print(f"    → Querying prices for {len(unique_tokens)} tokens from {min_date} to {max_date}")
        
        price_df = client.query(price_query).to_dataframe()
        
        if len(price_df) == 0:
            print("    ⚠️  No price data found - using fallback prices")
            df['first_buy_price'] = 1.0  # Fallback price
            df['total_usd_spent'] = df['total_amount_bought'] * 1.0
            return df
        
        print(f"    → Found price data: {len(price_df)} records")
        
        # Convert timestamps to dates for joining
        df['buy_date'] = pd.to_datetime(df['first_buy_timestamp']).dt.date
        price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.date
        
        # Join with price data (using nearest available price)
        df_with_prices = []
        
        for _, row in df.iterrows():
            token_prices = price_df[price_df['token_address'] == row['token_address']]
            
            if len(token_prices) == 0:
                # No price data for this token - use fallback
                price = 1.0
            else:
                # Find closest price date (prefer same date or earlier)
                suitable_prices = token_prices[token_prices['price_date'] <= row['buy_date']]
                
                if len(suitable_prices) == 0:
                    # No earlier price, use first available
                    price = token_prices.iloc[0]['price_usd']
                else:
                    # Use most recent price before/on buy date
                    price = suitable_prices.iloc[-1]['price_usd']
            
            # Calculate USD spent
            usd_spent = row['total_amount_bought'] * price
            
            # Add to result
            row_dict = row.to_dict()
            row_dict['first_buy_price'] = price
            row_dict['total_usd_spent'] = usd_spent
            df_with_prices.append(row_dict)
        
        result_df = pd.DataFrame(df_with_prices)
        
        # Drop temporary column
        result_df = result_df.drop(columns=['buy_date'])
        
        avg_price = result_df['first_buy_price'].mean()
        total_usd = result_df['total_usd_spent'].sum()
        
        print(f"    → Price calculation complete: avg ${avg_price:.4f}, total ${total_usd:,.2f} USD")
        
        return result_df
        
    except Exception as e:
        print(f"    ⚠️  Error calculating prices: {e}")
        print("    → Using fallback prices")
        df['first_buy_price'] = 1.0  # Fallback price
        df['total_usd_spent'] = df['total_amount_bought'] * 1.0
        return df


def format_for_bigquery_schema(df):
    """Format DataFrame to match stg_crisis_buyers BigQuery schema exactly."""
    if len(df) == 0:
        return df
    
    # Ensure required string fields are strings and not null
    string_fields = ['crisis_id', 'wallet_address', 'token_address']
    for field in string_fields:
        df[field] = df[field].astype(str)
        # Remove any null/empty values
        df = df[df[field].notna() & (df[field] != '') & (df[field] != 'nan')]
    
    # Ensure timestamp field is proper timestamp
    df['first_buy_timestamp'] = pd.to_datetime(df['first_buy_timestamp'])
    
    # Ensure float fields are proper floats
    float_fields = ['first_buy_price', 'total_amount_bought', 'total_usd_spent']
    for field in float_fields:
        df[field] = pd.to_numeric(df[field], errors='coerce').astype(float)
    
    # Ensure integer field is proper integer
    df['num_transactions'] = pd.to_numeric(df['num_transactions'], errors='coerce').astype('Int64')
    
    # Reorder columns to match BigQuery schema order
    column_order = [
        'crisis_id',
        'wallet_address', 
        'token_address',
        'first_buy_timestamp',
        'first_buy_price',
        'total_amount_bought',
        'total_usd_spent',
        'num_transactions'
    ]
    
    df = df[column_order]
    
    # Filter out any rows with null required fields
    required_fields = ['crisis_id', 'wallet_address', 'token_address', 'first_buy_timestamp']
    for field in required_fields:
        df = df[df[field].notna()]
    
    print(f"  → Data validation: {len(df)} records after schema formatting")
    
    return df


def execute_query_step(client, query, step_name):
    """Execute a single query step with error handling and logging."""
    try:
        print(f"  ⏳ Executing {step_name} query...")
        query_job = client.query(query)
        df = query_job.to_dataframe()
        print(f"  ✅ {step_name} query completed: {len(df)} rows")
        return df
        
    except Exception as e:
        print(f"  ❌ {step_name} query failed: {e}")
        raise Exception(f"{step_name} query failed: {e}")


def analyze_swap_for_crisis_token(swap_row, v2_topic, v3_topic):
    """
    Analyze a swap transaction to determine if it's buying the crisis token.
    
    Returns (is_buy: bool, token_amount: float)
    """
    try:
        crisis_token = swap_row['crisis_token'].lower()
        token0 = swap_row['token0_address'].lower()
        token1 = swap_row['token1_address'].lower()
        dex_protocol = swap_row['dex_protocol']
        data = swap_row['data']
        
        if not data or len(data) < 10:
            return False, 0.0
        
        # Simplified detection logic (this is a complex topic and may need refinement)
        if dex_protocol == 'Uniswap V2':
            # V2 data format: amount0In, amount1In, amount0Out, amount1Out
            # If crisis token is token0, check amount0Out > 0 (receiving crisis token)
            if token0 == crisis_token:
                try:
                    # Extract amount0Out (bytes 65-96 of data, accounting for 0x prefix)
                    amount_hex = data[66:130] if len(data) >= 130 else '0'
                    amount = int(amount_hex, 16) if amount_hex else 0
                    if amount > 0:
                        return True, float(amount) / 1e18
                except:
                    pass
                    
            # If crisis token is token1, check amount1Out > 0
            elif token1 == crisis_token:
                try:
                    # Extract amount1Out (bytes 97-128 of data)
                    amount_hex = data[130:194] if len(data) >= 194 else '0'
                    amount = int(amount_hex, 16) if amount_hex else 0
                    if amount > 0:
                        return True, float(amount) / 1e18
                except:
                    pass
                    
        elif dex_protocol == 'Uniswap V3':
            # V3 is more complex, simplified logic for now
            if token0 == crisis_token or token1 == crisis_token:
                # Assume any V3 swap involving crisis token is a potential buy
                # This is overly simplified and would need proper V3 data parsing
                return True, 1.0  # Placeholder amount
        
        return False, 0.0
        
    except Exception as e:
        print(f"    Error analyzing swap: {e}")
        return False, 0.0

def load_to_bigquery(df, config, dry_run=False):
    """Load crisis buyers data to BigQuery with proper schema validation."""
    if len(df) == 0:
        print("⚠️  No data to load - skipping BigQuery load")
        return
    
    table_id = f"{config.project_id}.{config.dataset_id}.stg_crisis_buyers"
    
    # Validate data before loading
    validate_crisis_buyers_data(df)
    
    # Show sample records before loading
    show_sample_records(df)
    
    if dry_run:
        print(f"🔍 DRY RUN: Would load {len(df)} records to {table_id}")
        print(f"  → Schema validation: PASSED")
        print(f"  → Columns: {list(df.columns)}")
        print(f"  → Data types: {dict(df.dtypes)}")
        if len(df) > 0:
            print(f"  → Sample record:")
            sample = df.iloc[0].to_dict()
            for key, value in sample.items():
                print(f"    {key}: {value} ({type(value).__name__})")
        print(f"✅ DRY RUN complete - no data was written to BigQuery")
        return
    
    client = bigquery.Client()
    
    # Define explicit schema to ensure proper data types
    schema = [
        bigquery.SchemaField("crisis_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("first_buy_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("first_buy_price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount_bought", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_usd_spent", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("num_transactions", "INTEGER", mode="NULLABLE"),
    ]
    
    # Configure load job with explicit schema
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        print(f"📤 Loading {len(df)} records to {table_id}...")
        print(f"  → Schema validation: PASSED")
        
        # Load data to BigQuery
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        # Verify the load
        table = client.get_table(table_id)
        print(f"✅ Crisis buyers data loaded successfully")
        print(f"  → Table now contains: {table.num_rows:,} rows")
        
    except Exception as e:
        print(f"❌ Failed to load data to BigQuery: {e}")
        print(f"  → Data shape: {df.shape}")
        print(f"  → Data types: {dict(df.dtypes)}")
        if hasattr(e, 'errors') and e.errors:
            for error in e.errors[:3]:  # Show first 3 errors
                print(f"  → Error detail: {error}")
        raise


def validate_crisis_buyers_data(df):
    """Validate DataFrame matches stg_crisis_buyers schema requirements."""
    required_columns = [
        'crisis_id', 'wallet_address', 'token_address', 'first_buy_timestamp',
        'first_buy_price', 'total_amount_bought', 'total_usd_spent', 'num_transactions'
    ]
    
    # Check all required columns exist
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for null values in required fields  
    required_not_null = ['crisis_id', 'wallet_address', 'token_address', 'first_buy_timestamp']
    for col in required_not_null:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            raise ValueError(f"Column '{col}' has {null_count} null values but is required")
    
    # Check data types
    if not pd.api.types.is_datetime64_any_dtype(df['first_buy_timestamp']):
        raise ValueError("first_buy_timestamp must be datetime/timestamp type")
    
    numeric_columns = ['first_buy_price', 'total_amount_bought', 'total_usd_spent', 'num_transactions']
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"Column '{col}' must be numeric type, got {df[col].dtype}")
    
    # Check for reasonable data values
    if (df['total_amount_bought'] < 0).any():
        raise ValueError("total_amount_bought cannot be negative")
    
    if (df['first_buy_price'] < 0).any():
        raise ValueError("first_buy_price cannot be negative")
    
    if (df['total_usd_spent'] < 0).any():
        raise ValueError("total_usd_spent cannot be negative")
    
    if (df['num_transactions'] != 1).any():
        raise ValueError("num_transactions must be exactly 1 for individual transaction records")
    
    print(f"  ✓ Data validation passed: {len(df)} records, {len(df.columns)} columns")


def show_sample_records(df):
    """Display sample records before loading to BigQuery."""
    print(f"\n📋 Sample Records (showing first 10 of {len(df)} total):")
    print("=" * 80)
    
    if len(df) == 0:
        print("No records to display")
        return
    
    # Show up to 10 records
    sample_size = min(10, len(df))
    sample_df = df.head(sample_size)
    
    for i, (_, row) in enumerate(sample_df.iterrows(), 1):
        print(f"\nRecord {i}:")
        print(f"  Crisis ID: {row['crisis_id']}")
        print(f"  Wallet: {row['wallet_address']}")
        print(f"  Token: {row['token_address']}")
        print(f"  Buy Time: {row['first_buy_timestamp']}")
        print(f"  Amount Bought: {row['total_amount_bought']:.6f} tokens")
        print(f"  Price per Token: ${row['first_buy_price']:.6f}")
        print(f"  Total USD Spent: ${row['total_usd_spent']:.2f}")
        print(f"  Transactions: {row['num_transactions']}")
        
    if len(df) > 10:
        print(f"\n... and {len(df) - 10} more records")
    
    print("=" * 80)


def main():
    """Main execution function."""
    try:
        config, dry_run = get_args()
        
        if dry_run:
            print("🔍 Running in DRY RUN mode - no data will be written to BigQuery")
        
        # Generate crisis buyers data
        df_buyers = identify_crisis_buyers(config)
        
        # Load to BigQuery (or show preview if dry run)
        load_to_bigquery(df_buyers, config, dry_run)
        
        if dry_run:
            print(f"✓ DRY RUN complete: {len(df_buyers)} individual buy transactions identified (not saved)")
        else:
            print(f"✓ Crisis buyers identification complete: {len(df_buyers)} individual buy transactions stored")
        
    except Exception as e:
        print(f"❌ Error in crisis buyers identification: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
