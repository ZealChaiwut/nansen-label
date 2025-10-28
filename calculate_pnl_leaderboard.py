#!/usr/bin/env python3
"""
Milestone 4: Calculate P&L and Leaderboard
"""

import sys
import os
import argparse
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from collections import namedtuple
from datetime import datetime, timedelta

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent / "lib"))
from bigquery_helpers import create_query_with_udfs, ETHEREUM_CONSTANTS

# Configuration
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])

# Analysis Configuration  
RECOVERY_PERIOD_DAYS = 90  # Look for peak price within 90 days after purchase
MIN_PROFIT_PCT = 10.0      # Only consider profits above 10%

def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Calculate P&L and leaderboard for crisis buyers')
    
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
    
    parser.add_argument('--top-n', 
                       type=int,
                       default=10,
                       help='Number of top performers to show (default: 10)')
    
    parser.add_argument('--recovery-days',
                       type=int, 
                       default=90,
                       help='Recovery period in days to look for peak price (default: 90)')
    
    parser.add_argument('--min-profit',
                       type=float,
                       default=10.0,
                       help='Minimum profit percentage to qualify as profitable flipper (default: 10.0)')
    
    args = parser.parse_args()
    
    if '.' not in args.target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    project_id, dataset_id = args.target.split('.', 1)
    return BigQueryConfig(project_id=project_id, dataset_id=dataset_id), args.dry_run, args.top_n, args.recovery_days, args.min_profit

def calculate_crisis_buyer_pnl(config):
    """
    Main orchestration function to calculate P&L for all crisis buyers.
    
    Returns DataFrame with profitable flipper data ready for BigQuery loading.
    """
    client = bigquery.Client()
    print("🏆 Starting P&L calculation for crisis buyers...")
    
    # Step 1: Get all crisis buyers
    buyers_df = get_crisis_buyers(client, config)
    
    # Step 2: Calculate P&L for each buyer
    pnl_df = calculate_pnl_metrics(client, config, buyers_df)
    
    # Step 3: Filter for profitable flippers only
    profitable_df = filter_profitable_flippers(pnl_df)
    
    # Step 4: Format for BigQuery schema
    final_df = format_for_profitable_flippers_schema(profitable_df)
    
    return final_df

def get_crisis_buyers(client, config):
    """Step 1: Load all crisis buyers from stg_crisis_buyers table."""
    print("\nStep 1: Loading crisis buyers...")
    
    query = f"""
    SELECT 
      crisis_id,
      wallet_address,
      token_address,
      first_buy_timestamp,
      first_buy_price,
      total_amount_bought,
      total_usd_spent,
      num_transactions
    FROM `{config.project_id}.{config.dataset_id}.stg_crisis_buyers`
    ORDER BY crisis_id, wallet_address, first_buy_timestamp
    """
    
    df = client.query(query).to_dataframe()
    
    if len(df) == 0:
        raise Exception("No crisis buyers found in stg_crisis_buyers table")
    
    print(f"  → Found {len(df)} crisis buyer transactions")
    print(f"  → Unique wallets: {df['wallet_address'].nunique()}")
    print(f"  → Unique tokens: {df['token_address'].nunique()}")
    print(f"  → Date range: {df['first_buy_timestamp'].min()} to {df['first_buy_timestamp'].max()}")
    
    return df

def calculate_pnl_metrics(client, config, buyers_df):
    """Step 2: Calculate P&L metrics for each crisis buyer transaction."""
    print(f"\nStep 2: Calculating P&L metrics for {len(buyers_df)} transactions...")
    
    # Get all unique tokens and date ranges for price lookup
    unique_tokens = buyers_df['token_address'].unique()
    min_buy_date = buyers_df['first_buy_timestamp'].min().date()
    max_recovery_date = (buyers_df['first_buy_timestamp'].max() + timedelta(days=RECOVERY_PERIOD_DAYS)).date()
    
    print(f"  → Querying price data for {len(unique_tokens)} tokens")
    print(f"  → Price date range: {min_buy_date} to {max_recovery_date}")
    
    # Query all relevant price history
    tokens_sql = "', '".join(unique_tokens)
    price_query = f"""
    SELECT 
      token_address,
      dt as price_date,
      price_usd
    FROM `{config.project_id}.{config.dataset_id}.dim_token_price_history`
    WHERE token_address IN ('{tokens_sql}')
      AND dt BETWEEN '{min_buy_date}' AND '{max_recovery_date}'
    ORDER BY token_address, dt
    """
    
    price_df = client.query(price_query).to_dataframe()
    
    if len(price_df) == 0:
        raise Exception("No price history data found for analysis period")
    
    print(f"  → Found {len(price_df)} price records")
    
    # Convert dates for easier processing
    price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.date
    buyers_df['buy_date'] = pd.to_datetime(buyers_df['first_buy_timestamp']).dt.date
    
    # Calculate P&L for each transaction
    pnl_results = []
    
    for idx, buyer_row in buyers_df.iterrows():
        if idx % 100 == 0:
            print(f"    → Processing transaction {idx+1}/{len(buyers_df)}")
            
        try:
            pnl_result = calculate_single_transaction_pnl(buyer_row, price_df)
            if pnl_result:
                pnl_results.append(pnl_result)
        except Exception as e:
            print(f"    Warning: Failed to calculate P&L for transaction {idx}: {e}")
            continue
    
    if len(pnl_results) == 0:
        raise Exception("No P&L calculations succeeded")
    
    pnl_df = pd.DataFrame(pnl_results)
    
    print(f"  → P&L calculated for {len(pnl_df)} transactions")
    print(f"  → Average profit: {pnl_df['estimated_profit_pct'].mean():.2f}%")
    print(f"  → Total profit: ${pnl_df['estimated_profit_usd'].sum():,.2f}")
    
    return pnl_df

def calculate_single_transaction_pnl(buyer_row, price_df):
    """Calculate P&L for a single crisis buyer transaction."""
    
    # Get price history for this token
    token_prices = price_df[price_df['token_address'] == buyer_row['token_address']]
    
    if len(token_prices) == 0:
        return None
    
    buy_date = buyer_row['buy_date']
    recovery_end_date = buy_date + timedelta(days=RECOVERY_PERIOD_DAYS)
    
    # Find peak price during recovery period (after buy date)
    recovery_prices = token_prices[
        (token_prices['price_date'] > buy_date) & 
        (token_prices['price_date'] <= recovery_end_date)
    ]
    
    if len(recovery_prices) == 0:
        return None
    
    # Find peak recovery price and its timestamp
    peak_price_row = recovery_prices.loc[recovery_prices['price_usd'].idxmax()]
    peak_recovery_price = peak_price_row['price_usd']
    peak_recovery_date = peak_price_row['price_date']
    
    # Calculate profit metrics
    buy_price = buyer_row['first_buy_price']
    amount_bought = buyer_row['total_amount_bought']
    
    if buy_price <= 0:
        return None
    
    profit_pct = ((peak_recovery_price - buy_price) / buy_price) * 100
    profit_usd = (peak_recovery_price - buy_price) * amount_bought
    
    return {
        'crisis_id': buyer_row['crisis_id'],
        'wallet_address': buyer_row['wallet_address'], 
        'token_address': buyer_row['token_address'],
        'buy_price': buy_price,
        'peak_recovery_price': peak_recovery_price,
        'estimated_profit_pct': profit_pct,
        'estimated_profit_usd': profit_usd,
        'buy_timestamp': buyer_row['first_buy_timestamp'],
        'peak_recovery_timestamp': pd.to_datetime(peak_recovery_date),
        'amount_bought': amount_bought,
        'original_usd_spent': buyer_row['total_usd_spent']
    }

def filter_profitable_flippers(pnl_df):
    """Step 3: Filter for profitable flippers only."""
    print(f"\nStep 3: Filtering for profitable flippers...")
    
    if len(pnl_df) == 0:
        return pd.DataFrame()
    
    # Filter for profitable transactions only (profit > minimum threshold)
    profitable_df = pnl_df[pnl_df['estimated_profit_pct'] >= MIN_PROFIT_PCT].copy()
    
    print(f"  → Profitable transactions: {len(profitable_df)} out of {len(pnl_df)}")
    print(f"  → Profit threshold: {MIN_PROFIT_PCT}%")
    
    if len(profitable_df) > 0:
        print(f"  → Best performer: {profitable_df['estimated_profit_pct'].max():.2f}% profit")
        print(f"  → Average profit: {profitable_df['estimated_profit_pct'].mean():.2f}%")
        print(f"  → Total profits: ${profitable_df['estimated_profit_usd'].sum():,.2f}")
    
    return profitable_df

def format_for_profitable_flippers_schema(df):
    """Step 4: Format DataFrame to match stg_profitable_flippers BigQuery schema."""
    print(f"\nStep 4: Formatting for BigQuery schema...")
    
    if len(df) == 0:
        print("  ⚠️  No profitable flippers to format")
        return pd.DataFrame()
    
    # Select and rename columns to match schema
    final_df = df[[
        'crisis_id',
        'wallet_address', 
        'token_address',
        'buy_price',
        'peak_recovery_price',
        'estimated_profit_pct',
        'estimated_profit_usd',
        'buy_timestamp',
        'peak_recovery_timestamp'
    ]].copy()
    
    # Ensure correct data types
    final_df['crisis_id'] = final_df['crisis_id'].astype(str)
    final_df['wallet_address'] = final_df['wallet_address'].astype(str) 
    final_df['token_address'] = final_df['token_address'].astype(str)
    final_df['buy_price'] = pd.to_numeric(final_df['buy_price'], errors='coerce')
    final_df['peak_recovery_price'] = pd.to_numeric(final_df['peak_recovery_price'], errors='coerce')
    final_df['estimated_profit_pct'] = pd.to_numeric(final_df['estimated_profit_pct'], errors='coerce')
    final_df['estimated_profit_usd'] = pd.to_numeric(final_df['estimated_profit_usd'], errors='coerce')
    final_df['buy_timestamp'] = pd.to_datetime(final_df['buy_timestamp'])
    final_df['peak_recovery_timestamp'] = pd.to_datetime(final_df['peak_recovery_timestamp'])
    
    # Remove any rows with invalid data
    final_df = final_df.dropna()
    
    # Sort by profit percentage descending
    final_df = final_df.sort_values('estimated_profit_pct', ascending=False)
    
    print(f"  → Final dataset: {len(final_df)} profitable flipper records")
    
    return final_df

def show_leaderboard(df, top_n=10):
    """Display top N performers and their detailed transactions."""
    print(f"\n🏆 TOP {top_n} CRISIS FLIPPER LEADERBOARD")
    print("=" * 100)
    
    if len(df) == 0:
        print("No profitable flippers found!")
        return
    
    # Aggregate by wallet to show top performers
    wallet_summary = df.groupby('wallet_address').agg({
        'estimated_profit_usd': 'sum',
        'estimated_profit_pct': 'mean', 
        'crisis_id': 'count'
    }).round(2)
    
    wallet_summary.columns = ['total_profit_usd', 'avg_profit_pct', 'num_profitable_trades']
    wallet_summary = wallet_summary.sort_values('total_profit_usd', ascending=False)
    
    top_wallets = wallet_summary.head(top_n)
    
    for rank, (wallet, summary) in enumerate(top_wallets.iterrows(), 1):
        print(f"\n#{rank} WALLET: {wallet}")
        print(f"   💰 Total Profit: ${summary['total_profit_usd']:,.2f}")
        print(f"   📈 Average Profit: {summary['avg_profit_pct']:.2f}%") 
        print(f"   🔄 Profitable Trades: {int(summary['num_profitable_trades'])}")
        
        # Show detailed transactions for this wallet
        wallet_trades = df[df['wallet_address'] == wallet].sort_values('estimated_profit_pct', ascending=False)
        
        print(f"   📊 Transaction Details:")
        for i, (_, trade) in enumerate(wallet_trades.iterrows(), 1):
            profit_pct = trade['estimated_profit_pct']
            profit_usd = trade['estimated_profit_usd'] 
            crisis = trade['crisis_id']
            buy_price = trade['buy_price']
            peak_price = trade['peak_recovery_price']
            
            print(f"      Trade {i}: {crisis}")
            print(f"        Buy: ${buy_price:.6f} → Peak: ${peak_price:.6f}")
            print(f"        Profit: {profit_pct:.2f}% (${profit_usd:,.2f})")
    
    print(f"\n📈 SUMMARY STATISTICS:")
    print(f"   Total Profitable Flippers: {len(wallet_summary)}")
    print(f"   Total Profitable Trades: {len(df)}")
    print(f"   Total Profits: ${df['estimated_profit_usd'].sum():,.2f}")
    print(f"   Average Profit per Trade: {df['estimated_profit_pct'].mean():.2f}%")
    print(f"   Best Single Trade: {df['estimated_profit_pct'].max():.2f}%")
    print("=" * 100)

def load_to_bigquery(df, config, dry_run=False):
    """Load profitable flippers data to BigQuery."""
    if len(df) == 0:
        print("⚠️  No profitable flippers to load - skipping BigQuery load")
        return
    
    table_id = f"{config.project_id}.{config.dataset_id}.stg_profitable_flippers"
    
    if dry_run:
        print(f"🔍 DRY RUN: Would load {len(df)} records to {table_id}")
        print(f"  → Columns: {list(df.columns)}")
        if len(df) > 0:
            print(f"  → Sample record: {df.iloc[0].to_dict()}")
        print(f"✅ DRY RUN complete - no data was written to BigQuery")
        return
    
    client = bigquery.Client()
    
    # Define explicit schema
    schema = [
        bigquery.SchemaField("crisis_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("wallet_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("token_address", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("buy_price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("peak_recovery_price", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("estimated_profit_pct", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("estimated_profit_usd", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("buy_timestamp", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("peak_recovery_timestamp", "TIMESTAMP", mode="NULLABLE"),
    ]
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        print(f"📤 Loading {len(df)} records to {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        # Verify the load
        table = client.get_table(table_id)
        print(f"✅ Profitable flippers data loaded successfully")
        print(f"  → Table now contains: {table.num_rows:,} rows")
        
    except Exception as e:
        print(f"❌ Failed to load data to BigQuery: {e}")
        raise

def main():
    """Main execution function."""
    try:
        config, dry_run, top_n, recovery_days, min_profit = get_args()
        
        # Update global configuration with command line arguments
        global RECOVERY_PERIOD_DAYS, MIN_PROFIT_PCT
        RECOVERY_PERIOD_DAYS = recovery_days
        MIN_PROFIT_PCT = min_profit
        
        if dry_run:
            print("🔍 Running in DRY RUN mode - no data will be written to BigQuery")
        
        print(f"📊 Analysis configuration:")
        print(f"  → Recovery period: {RECOVERY_PERIOD_DAYS} days")
        print(f"  → Minimum profit threshold: {MIN_PROFIT_PCT}%")
        print(f"  → Top performers to show: {top_n}")
        
        # Calculate P&L for all crisis buyers
        profitable_df = calculate_crisis_buyer_pnl(config)
        
        # Show leaderboard
        show_leaderboard(profitable_df, top_n)
        
        # Load to BigQuery (or show preview if dry run)
        load_to_bigquery(profitable_df, config, dry_run)
        
        if dry_run:
            print(f"✓ DRY RUN complete: {len(profitable_df)} profitable flippers identified (not saved)")
        else:
            print(f"✓ P&L analysis complete: {len(profitable_df)} profitable flippers stored")
        
    except Exception as e:
        print(f"❌ Error in P&L analysis: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
