#!/usr/bin/env python3
"""
Generate token price history data for Phoenix Flipper project.
Creates dim_token_price_history based on crisis_events_with_window.
"""
import os
import sys
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "lib"))
from bigquery_helpers import get_standard_args, BigQueryConfig, load_to_bigquery_table, execute_query


def create_dataset_if_not_exists(config):
    """Create the dataset if it doesn't exist."""
    client = bigquery.Client(project=config.project_id)
    dataset_id = f"{config.project_id}.{config.dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"✓ Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Created dataset: {dataset_id}")


def generate_token_price_history(config):
    """Generate simple price history that matches existing crisis events."""
    np.random.seed(42)
    
    # First, get the crisis events from BigQuery to align price data
    client = bigquery.Client(project=config.project_id)
    crisis_query = f"""
        SELECT token_address, crisis_date, window_start_date, window_end_date
        FROM `{config.project_id}.{config.dataset_id}.crisis_events_with_window`
        ORDER BY crisis_date
    """
    
    try:
        crisis_df = execute_query(client, crisis_query, "crisis events")
        print(f"✅ Found {len(crisis_df)} crisis events")
    except Exception as e:
        print(f"❌ Crisis events table not found: {e}")
        raise Exception(f"Crisis events table not found: {e}")
    
    # Create dynamic token info based on actual crisis tokens
    # Generate reasonable mock price and volume data
    token_info = {}
    np.random.seed(42)  # For consistent mock data
    
    base_prices = [0.5, 1.0, 5.0, 15.0, 100.0, 1000.0]  # Range of realistic token prices
    base_volumes = [1000000, 5000000, 10000000, 20000000, 50000000]  # Range of volumes
    
    unique_tokens = crisis_df['token_address'].unique()
    for i, token_address in enumerate(unique_tokens):
        # Use deterministic randomness based on token address for consistency
        token_seed = hash(token_address) % 1000
        np.random.seed(token_seed)
        
        token_info[token_address] = {
            "symbol": f"CRISIS{i+1}",
            "base_price": np.random.choice(base_prices),
            "volume_base": np.random.choice(base_volumes)
        }
    
    all_price_data = []
    
    # Get unique tokens from crisis events
    unique_tokens = crisis_df['token_address'].unique()
    
    for token_address in unique_tokens:
        if token_address not in token_info:
            continue
            
        info = token_info[token_address]
        token_crises = crisis_df[crisis_df['token_address'] == token_address]
        
        # Generate 2 years of price data
        start_date = datetime(2020, 1, 1).date()
        end_date = datetime.now().date()
        current_date = start_date
        current_price = info["base_price"]
        
        while current_date <= end_date:
            # Check if we're near any crisis
            near_crisis = False
            crisis_intensity = 0.0
            
            for _, crisis in token_crises.iterrows():
                days_to_crisis = (crisis['crisis_date'] - current_date).days
                
                # Crisis surge (big drop) around crisis date
                if abs(days_to_crisis) <= 7:
                    near_crisis = True
                    if days_to_crisis >= 0 and days_to_crisis <= 3:
                        # During crisis - big drop
                        crisis_intensity = -0.15  # 15% drop per day during crisis
                    elif days_to_crisis < 0 and days_to_crisis >= -7:
                        # Recovery after crisis - bounce back
                        crisis_intensity = 0.08  # 8% recovery per day
                
            if near_crisis:
                # Crisis period - big movements
                daily_change = crisis_intensity + np.random.normal(0, 0.1)  # High volatility
            else:
                # Normal period - random walk
                daily_change = np.random.normal(0, 0.03)  # Normal 3% volatility
            
            # Apply price change
            new_price = current_price * (1 + daily_change)
            new_price = max(new_price, 0.01)  # Price floor
            
            # Simple market data
            price_change_pct = daily_change * 100
            volume_24h = info["volume_base"] * (1 + abs(daily_change) * 3) * np.random.uniform(0.5, 2.0)
            market_cap = new_price * np.random.uniform(100000000, 1000000000)
            liquidity = volume_24h * np.random.uniform(0.1, 0.5)
            
            high_24h = new_price * np.random.uniform(1.01, 1.05)
            low_24h = new_price * np.random.uniform(0.95, 0.99)
            
            all_price_data.append({
                "token_address": token_address,
                "price_usd": round(new_price, 6),
                "volume_24h_usd": round(volume_24h, 2),
                "market_cap_usd": round(market_cap, 2),
                "price_change_24h_pct": round(price_change_pct, 2),
                "liquidity_usd": round(liquidity, 2),
                "high_24h_usd": round(high_24h, 6),
                "low_24h_usd": round(low_24h, 6),
                "dt": current_date
            })
            
            current_price = new_price
            current_date += timedelta(days=1)
    
    return pd.DataFrame(all_price_data)



def main():
    # Parse command line arguments using standard helper
    config, dry_run = get_standard_args("Generate token price history data based on crisis events")
    
    print("=" * 80)
    print("Generating Token Price History Data")
    print("=" * 80)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Source: crisis_events_with_window table")
    print()
    
    try:
        # Create dataset
        create_dataset_if_not_exists(config)
        
        # Generate token price history data
        df_price_history = generate_token_price_history(config)
        
        # Load to BigQuery using helper function
        load_to_bigquery_table(
            df_price_history, 
            config, 
            "dim_token_price_history", 
            schema=None,  # Let BigQuery infer schema
            dry_run=dry_run
        )
        
        print(f"✓ Price history generation complete: {len(df_price_history)} records")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
