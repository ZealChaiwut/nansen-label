#!/usr/bin/env python3
"""
Milestone 4: Calculate P&L and Leaderboard
"""

import sys
import os
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent / "lib"))
from bigquery_helpers import (
    get_standard_args, execute_query, load_to_bigquery_table
)

# Analysis Configuration  
RECOVERY_PERIOD_DAYS = 90  # Look for peak price within 90 days after purchase
MIN_PROFIT_PCT = 10.0      # Only consider profits above 10%

# BigQuery Schema
PROFITABLE_FLIPPERS_SCHEMA = [
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


def calculate_crisis_buyer_pnl(config):
    """Calculate P&L for all crisis buyers and return profitable flippers."""
    client = bigquery.Client()
    print("🏆 Starting P&L calculation...")
    
    buyers_df = get_crisis_buyers(client, config)
    pnl_df = calculate_pnl_metrics(client, config, buyers_df)
    profitable_df = filter_profitable_flippers(pnl_df)
    final_df = format_for_profitable_flippers_schema(profitable_df)
    
    return final_df

def get_crisis_buyers(client, config):
    """Load all crisis buyers from stg_crisis_buyers table."""
    print("Loading crisis buyers...")
    
    query = f"""
    SELECT crisis_id, wallet_address, token_address, first_buy_timestamp,
           first_buy_price, total_amount_bought, total_usd_spent, num_transactions
    FROM `{config.project_id}.{config.dataset_id}.stg_crisis_buyers`
    ORDER BY crisis_id, wallet_address, first_buy_timestamp
    """
    
    df = execute_query(client, query, "crisis buyers")
    
    if len(df) == 0:
        raise Exception("No crisis buyers found")
    
    print(f"  → {len(df)} transactions, {df['wallet_address'].nunique()} wallets")
    return df

def calculate_pnl_metrics(client, config, buyers_df):
    """Calculate P&L metrics for each crisis buyer transaction."""
    print("Calculating P&L metrics...")
    
    unique_tokens = buyers_df['token_address'].unique()
    min_date = buyers_df['first_buy_timestamp'].min().date()
    max_date = (buyers_df['first_buy_timestamp'].max() + timedelta(days=RECOVERY_PERIOD_DAYS)).date()
    
    tokens_sql = "', '".join(unique_tokens)
    price_query = f"""
    SELECT token_address, dt as price_date, price_usd
    FROM `{config.project_id}.{config.dataset_id}.dim_token_price_history`
    WHERE token_address IN ('{tokens_sql}') AND dt BETWEEN '{min_date}' AND '{max_date}'
    ORDER BY token_address, dt
    """
    
    price_df = execute_query(client, price_query, "price history")
    if len(price_df) == 0:
        raise Exception("No price history data found")
    
    price_df['price_date'] = pd.to_datetime(price_df['price_date']).dt.date
    buyers_df['buy_date'] = pd.to_datetime(buyers_df['first_buy_timestamp']).dt.date
    
    # Calculate P&L for each transaction
    pnl_results = []
    for idx, buyer_row in buyers_df.iterrows():
        pnl_result = calculate_single_transaction_pnl(buyer_row, price_df)
        if pnl_result:
            pnl_results.append(pnl_result)
    
    if len(pnl_results) == 0:
        raise Exception("No P&L calculations succeeded")
    
    pnl_df = pd.DataFrame(pnl_results)
    print(f"  → {len(pnl_df)} P&L calculations, avg {pnl_df['estimated_profit_pct'].mean():.1f}% profit")
    
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
    """Filter for profitable flippers above minimum threshold."""
    if len(pnl_df) == 0:
        return pd.DataFrame()
    
    profitable_df = pnl_df[pnl_df['estimated_profit_pct'] >= MIN_PROFIT_PCT].copy()
    print(f"Filtering: {len(profitable_df)}/{len(pnl_df)} profitable (>{MIN_PROFIT_PCT}%)")
    
    return profitable_df

def format_for_profitable_flippers_schema(df):
    """Format DataFrame for BigQuery schema."""
    if len(df) == 0:
        return pd.DataFrame()
    
    # Select columns and ensure data types
    final_df = df[[
        'crisis_id', 'wallet_address', 'token_address', 'buy_price',
        'peak_recovery_price', 'estimated_profit_pct', 'estimated_profit_usd',
        'buy_timestamp', 'peak_recovery_timestamp'
    ]].copy()
    
    # Convert data types
    for col in ['crisis_id', 'wallet_address', 'token_address']:
        final_df[col] = final_df[col].astype(str)
    for col in ['buy_price', 'peak_recovery_price', 'estimated_profit_pct', 'estimated_profit_usd']:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
    for col in ['buy_timestamp', 'peak_recovery_timestamp']:
        final_df[col] = pd.to_datetime(final_df[col])
    
    final_df = final_df.dropna().sort_values('estimated_profit_pct', ascending=False)
    print(f"Formatting: {len(final_df)} records ready for BigQuery")
    
    return final_df

def show_leaderboard(df, top_n=10):
    """Display top N performers and their detailed transactions."""
    print(f"\n🏆 TOP {top_n} CRISIS FLIPPER LEADERBOARD")
    print("=" * 80)
    
    if len(df) == 0:
        print("No profitable flippers found!")
        return
    
    # Aggregate by wallet
    wallet_summary = df.groupby('wallet_address').agg({
        'estimated_profit_usd': 'sum',
        'estimated_profit_pct': 'mean', 
        'crisis_id': 'count'
    }).round(2)
    
    wallet_summary.columns = ['total_profit_usd', 'avg_profit_pct', 'num_trades']
    wallet_summary = wallet_summary.sort_values('total_profit_usd', ascending=False).head(top_n)
    
    for rank, (wallet, summary) in enumerate(wallet_summary.iterrows(), 1):
        print(f"\n#{rank} {wallet}")
        print(f"   💰 ${summary['total_profit_usd']:,.2f} | 📈 {summary['avg_profit_pct']:.1f}% | 🔄 {int(summary['num_trades'])} trades")
        
        # Show transaction details
        wallet_trades = df[df['wallet_address'] == wallet].sort_values('estimated_profit_pct', ascending=False)
        for i, (_, trade) in enumerate(wallet_trades.iterrows(), 1):
            print(f"      {trade['crisis_id']}: ${trade['buy_price']:.4f} → ${trade['peak_recovery_price']:.4f} ({trade['estimated_profit_pct']:.1f}%)")
    
    print(f"\n📊 Summary: {len(wallet_summary)} flippers, {len(df)} trades, ${df['estimated_profit_usd'].sum():,.0f} total profit")
    print("=" * 80)


def main():
    """Main execution function."""
    try:
        config, dry_run = get_standard_args('Calculate P&L and leaderboard for crisis buyers')
        
        if dry_run:
            print("🔍 Running in DRY RUN mode")
        
        # Calculate P&L for all crisis buyers
        profitable_df = calculate_crisis_buyer_pnl(config)
        
        # Show leaderboard
        show_leaderboard(profitable_df)
        
        # Load to BigQuery using helper function
        load_to_bigquery_table(profitable_df, config, 'stg_profitable_flippers', PROFITABLE_FLIPPERS_SCHEMA, dry_run)
        
        result_msg = f"✓ {len(profitable_df)} profitable flippers {'identified (not saved)' if dry_run else 'stored'}"
        print(result_msg)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
