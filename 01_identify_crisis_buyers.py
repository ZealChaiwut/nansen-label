#!/usr/bin/env python3
"""
Milestone 3: Identify Crisis Buyers
"""

import sys
import os
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent / "lib"))
from bigquery_helpers import (
    get_standard_args, execute_query, load_to_bigquery_table, 
    create_query_with_udfs, ETHEREUM_CONSTANTS
)

# Query Configuration
QUERY_START_DATE = '2021-05-01'
QUERY_END_DATE = '2023-01-01'
QUERY_LIMIT = 1000000

# BigQuery Schema
CRISIS_BUYERS_SCHEMA = [
    bigquery.SchemaField("crisis_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("first_buy_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("first_buy_price", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_amount_bought", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("total_usd_spent", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("num_transactions", "INTEGER", mode="NULLABLE"),
]


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
    """Load crisis events with buy windows."""
    print("Loading crisis events...")
    
    query = f"""
    SELECT crisis_id, token_address, window_start_date, window_end_date, crisis_name
    FROM `{config.project_id}.{config.dataset_id}.crisis_events_with_window`
    ORDER BY crisis_id
    """
    
    df = execute_query(client, query, "crisis events")
    if len(df) == 0:
        raise Exception("No crisis events found")
    
    print(f"  → {len(df)} events, {df['token_address'].nunique()} tokens")
    return df


def get_crisis_pools(client, config, crisis_df):
    """Find DEX pools for crisis tokens."""
    print("Finding crisis pools...")
    
    query = f"""
    SELECT DISTINCT p.pool_address, p.token0_address, p.token1_address, p.dex_protocol,
           c.crisis_id, c.token_address AS crisis_token, c.window_start_date, c.window_end_date, c.crisis_name
    FROM `{config.project_id}.{config.dataset_id}.dim_dex_pools` p
    INNER JOIN `{config.project_id}.{config.dataset_id}.crisis_events_with_window` c ON (
      p.token0_address = c.token_address OR p.token1_address = c.token_address
    )
    ORDER BY c.crisis_id, p.pool_address
    """
    
    df = execute_query(client, query, "crisis pools")
    if len(df) == 0:
        raise Exception("No DEX pools found for crisis tokens")
    
    print(f"  → {len(df)} pool-crisis combinations, {df['pool_address'].nunique()} unique pools")
    return df


def get_ethereum_swaps(client, pool_addresses):
    """Query Ethereum swap logs for relevant pools."""
    print("Querying Ethereum swap logs...")
    
    pool_addresses_sql = "', '".join(pool_addresses)
    print(f"  → {len(pool_addresses)} pools, {QUERY_START_DATE} to {QUERY_END_DATE}, limit {QUERY_LIMIT:,}")
    
    main_query = f"""
    SELECT logs.block_timestamp, logs.transaction_hash, logs.log_index,
           logs.address AS pool_address, logs.topics, logs.data, txns.from_address AS wallet_address
    FROM `bigquery-public-data.crypto_ethereum.logs` logs
    LEFT JOIN `bigquery-public-data.crypto_ethereum.transactions` txns ON logs.transaction_hash = txns.hash
    WHERE logs.topics[SAFE_OFFSET(0)] = '{ETHEREUM_CONSTANTS['V2_SWAP_TOPIC']}'
      AND logs.block_timestamp >= '{QUERY_START_DATE}' AND logs.block_timestamp <= '{QUERY_END_DATE}'
      AND logs.address IN ('{pool_addresses_sql}')
    ORDER BY logs.block_timestamp DESC
    LIMIT {QUERY_LIMIT}
    """
    
    query = create_query_with_udfs(main_query)
    df = execute_query(client, query, "Ethereum swaps")
    
    if len(df) == 0:
        print("  ⚠️  No swap logs found")
        return pd.DataFrame()
    
    print(f"  → {len(df)} swaps, {df['wallet_address'].nunique()} wallets")
    return df


def filter_crisis_window_swaps(swaps_df, pools_df):
    """Filter swaps that occurred within crisis buy windows."""
    print("Filtering swaps within crisis windows...")
    
    if len(swaps_df) == 0:
        return pd.DataFrame()
    
    crisis_swaps_data = []
    
    for _, pool_row in pools_df.iterrows():
        pool_swaps = swaps_df[swaps_df['pool_address'] == pool_row['pool_address']]
        if len(pool_swaps) == 0:
            continue
            
        pool_swaps = pool_swaps.copy()
        pool_swaps['block_date'] = pd.to_datetime(pool_swaps['block_timestamp']).dt.date
        
        window_swaps = pool_swaps[
            (pool_swaps['block_date'] >= pool_row['window_start_date']) &
            (pool_swaps['block_date'] <= pool_row['window_end_date'])
        ]
        
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
    print(f"  → {len(df)} swaps in crisis windows, {df['wallet_address'].nunique()} wallets")
    return df


def identify_token_buyers(crisis_swaps_df):
    """Process swaps to identify crisis token buyers."""
    print("Processing swaps to identify crisis token purchases...")
    
    if len(crisis_swaps_df) == 0:
        return pd.DataFrame()
    
    buyers_data = []
    
    for _, row in crisis_swaps_df.iterrows():
        try:
            is_buy, token_amount = analyze_swap_for_crisis_token(row, ETHEREUM_CONSTANTS['V2_SWAP_TOPIC'])
            
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
        except Exception:
            continue
    
    if len(buyers_data) == 0:
        print("  ⚠️  No crisis token buyers identified")
        return pd.DataFrame()
    
    df = pd.DataFrame(buyers_data)
    print(f"  → {len(df)} purchase transactions, {df['wallet_address'].nunique()} buyers")
    return df


def format_individual_buys(buyers_df, config):
    """Format individual buy records for BigQuery."""
    print("Formatting individual buy records...")
    
    if len(buyers_df) == 0:
        return pd.DataFrame()
    
    # Rename columns and add required fields
    final_df = buyers_df.rename(columns={
        'block_timestamp': 'first_buy_timestamp',
        'token_amount': 'total_amount_bought'
    })
    final_df['num_transactions'] = 1
    
    # Remove unnecessary columns
    columns_to_drop = ['crisis_name', 'dex_protocol', 'transaction_hash']
    final_df = final_df.drop(columns=[col for col in columns_to_drop if col in final_df.columns])
    
    # Calculate prices and format for BigQuery
    final_df = calculate_price_and_usd_spent(final_df, config)
    final_df = format_for_bigquery_schema(final_df)
    final_df = final_df.sort_values(['crisis_id', 'first_buy_timestamp'], ascending=[True, False])
    
    print(f"  → {len(final_df)} buy transactions, {final_df['wallet_address'].nunique()} wallets")
    return final_df


def calculate_price_and_usd_spent(df, config):
    """Calculate first_buy_price and total_usd_spent by joining with price history."""
    if len(df) == 0:
        return df
    
    try:
        client = bigquery.Client()
        unique_tokens = df['token_address'].unique()
        tokens_sql = "', '".join(unique_tokens)
        min_date = df['first_buy_timestamp'].min().date()
        max_date = df['first_buy_timestamp'].max().date()
        
        price_query = f"""
        SELECT token_address, dt as price_date, price_usd
        FROM `{config.project_id}.{config.dataset_id}.dim_token_price_history`
        WHERE token_address IN ('{tokens_sql}') AND dt BETWEEN '{min_date}' AND '{max_date}'
        ORDER BY token_address, dt
        """
        
        price_df = execute_query(client, price_query, "price history")
        
        if len(price_df) == 0:
            df['first_buy_price'] = 1.0
            df['total_usd_spent'] = df['total_amount_bought'] * 1.0
            return df
        
        # Convert dates and calculate prices
        df['buy_date'] = pd.to_datetime(df['first_buy_timestamp']).dt.date
        price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.date
        
        df_with_prices = []
        for _, row in df.iterrows():
            token_prices = price_df[price_df['token_address'] == row['token_address']]
            
            if len(token_prices) == 0:
                price = 1.0
            else:
                suitable_prices = token_prices[token_prices['price_date'] <= row['buy_date']]
                price = suitable_prices.iloc[-1]['price_usd'] if len(suitable_prices) > 0 else token_prices.iloc[0]['price_usd']
            
            row_dict = row.to_dict()
            row_dict['first_buy_price'] = price
            row_dict['total_usd_spent'] = row['total_amount_bought'] * price
            df_with_prices.append(row_dict)
        
        result_df = pd.DataFrame(df_with_prices).drop(columns=['buy_date'])
        print("  → Prices calculated")
        return result_df
        
    except Exception as e:
        print(f"  ⚠️  Price calculation failed: {e}")
        df['first_buy_price'] = 1.0
        df['total_usd_spent'] = df['total_amount_bought'] * 1.0
        return df


def format_for_bigquery_schema(df):
    """Format DataFrame to match stg_crisis_buyers BigQuery schema."""
    if len(df) == 0:
        return df
    
    # Convert data types
    for field in ['crisis_id', 'wallet_address', 'token_address']:
        df[field] = df[field].astype(str)
        df = df[df[field].notna() & (df[field] != '') & (df[field] != 'nan')]
    
    df['first_buy_timestamp'] = pd.to_datetime(df['first_buy_timestamp'])
    
    for field in ['first_buy_price', 'total_amount_bought', 'total_usd_spent']:
        df[field] = pd.to_numeric(df[field], errors='coerce').astype(float)
    
    df['num_transactions'] = pd.to_numeric(df['num_transactions'], errors='coerce').astype('Int64')
    
    # Reorder columns and filter nulls
    column_order = ['crisis_id', 'wallet_address', 'token_address', 'first_buy_timestamp',
                   'first_buy_price', 'total_amount_bought', 'total_usd_spent', 'num_transactions']
    df = df[column_order]
    
    for field in ['crisis_id', 'wallet_address', 'token_address', 'first_buy_timestamp']:
        df = df[df[field].notna()]
    
    return df




def analyze_swap_for_crisis_token(swap_row, v2_topic):
    """
    Analyze a Uniswap V2 swap transaction to determine if it's buying the crisis token.
    
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
        
        # Only handle Uniswap V2 - V3 removed for data accuracy
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
        
        return False, 0.0
        
    except Exception as e:
        print(f"    Error analyzing swap: {e}")
        return False, 0.0



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
        config, dry_run = get_standard_args('Identify wallets that bought tokens during crisis windows')
        
        if dry_run:
            print("🔍 Running in DRY RUN mode")
        
        # Generate crisis buyers data
        df_buyers = identify_crisis_buyers(config)
        
        # Load to BigQuery using helper function
        load_to_bigquery_table(
            df_buyers, config, 'stg_crisis_buyers', CRISIS_BUYERS_SCHEMA, 
            dry_run, validate_crisis_buyers_data, show_sample_records
        )
        
        result_msg = f"✓ {len(df_buyers)} buy transactions {'identified (not saved)' if dry_run else 'stored'}"
        print(result_msg)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
