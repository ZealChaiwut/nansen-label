#!/usr/bin/env python3
"""
Generate Milestone 1 (M1) foundation data for Phoenix Flipper project.
Creates DEX pool metadata and token price history data.
"""
import argparse
import os
from collections import namedtuple
from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configuration container for BigQuery project and dataset
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])



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


def generate_dim_dex_pools(count=50):
    """Generate DEX pool data using real crisis tokens."""
    np.random.seed(42)
    
    # Real crisis tokens with their addresses
    crisis_tokens = [
        ("0xf4d2888d29D722226FafA5d9B24F9164cfdF5Bd4", "LOOKS"),   # LooksRare
        ("0x4d224452801ACEd8B2F0aebE155379bb5D594381", "APE"),     # ApeCoin
        ("0x6B3595068778DD592e39A122f4f5a5cF09C90fE2", "SUSHI"),   # SushiSwap
        ("0xcF6BB5389c92Bdda8a3747Ddb454cB7a64626C63", "XVS"),     # Venus Protocol
        ("0x64aa3364F17a4D01c6f1751Fd97C2BD3D7e7f1D5", "OHM"),     # Olympus DAO (gOHM)
        ("0xD31a59c85aE9D8edEFeC411D448f90841571b89c", "wSOL"),    # Wrapped SOL
    ]
    
    # Common stable/base tokens for pairs
    base_tokens = [
        ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC"),
        ("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH"),
        ("0xdAC17F958D2ee523a2206206994597C13D831ec7", "USDT"),
        ("0x6B175474E89094C44Da98b954EedeAC495271d0F", "DAI"),
    ]
    
    dex_protocols = ["Uniswap V3", "Uniswap V2", "Balancer", "Curve", "1inch"]
    
    data = []
    # Create pools for each crisis token paired with base tokens
    for crisis_token, crisis_symbol in crisis_tokens:
        for base_token, base_symbol in base_tokens:
            pool_address = f"0x{''.join(np.random.choice(list('0123456789abcdef'), size=40))}"
            dex = np.random.choice(dex_protocols)
            
            # Set creation date before crisis events (pools existed before crises)
            days_ago = np.random.randint(365, 900)  # 1-2.5 years ago
            created_ts = datetime.now() - timedelta(days=days_ago)
            
            data.append({
                "pool_address": pool_address,
                "pool_name": f"{crisis_symbol}-{base_symbol} Pool",
                "token0_address": crisis_token,
                "token0_symbol": crisis_symbol,
                "token1_address": base_token,
                "token1_symbol": base_symbol,
                "dex_protocol": dex,
                "chain": "ethereum",  # All these tokens are on Ethereum
            })
    
    # Generate additional random pools to reach target count
    all_tokens = crisis_tokens + base_tokens
    for i in range(count - len(data)):
        token0, symbol0 = all_tokens[i % len(all_tokens)]
        token1, symbol1 = all_tokens[(i + 1) % len(all_tokens)]
        
        # Skip if same token
        if token0 == token1:
            continue
            
        pool_address = f"0x{''.join(np.random.choice(list('0123456789abcdef'), size=40))}"
        dex = np.random.choice(dex_protocols)
        
        days_ago = np.random.randint(0, 730)
        created_ts = datetime.now() - timedelta(days=days_ago)
        
        data.append({
            "pool_address": pool_address,
            "pool_name": f"{symbol0}-{symbol1} Pool",
            "token0_address": token0,
            "token0_symbol": symbol0,
            "token1_address": token1,
            "token1_symbol": symbol1,
            "dex_protocol": dex,
            "chain": "ethereum",
        })
    
    return pd.DataFrame(data)


def generate_token_price_history():
    """Generate daily price history for crisis tokens."""
    np.random.seed(42)
    
    # Crisis tokens with baseline prices (before major events)
    crisis_tokens = [
        {
            "address": "0xf4d2888d29D722226FafA5d9B24F9164cfdF5Bd4",  # LOOKS
            "symbol": "LOOKS",
            "baseline_price": 5.0,
            "crisis_date": datetime(2022, 3, 15).date(),
            "crisis_price": 0.45,
            "launch_date": datetime(2022, 1, 10).date()  # Token launch
        },
        {
            "address": "0x4d224452801ACEd8B2F0aebE155379bb5D594381",  # APE
            "symbol": "APE", 
            "baseline_price": 22.0,
            "crisis_date": datetime(2022, 5, 15).date(),
            "crisis_price": 4.0,
            "launch_date": datetime(2022, 3, 17).date()  # Token launch
        },
        {
            "address": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2",  # SUSHI
            "symbol": "SUSHI",
            "baseline_price": 8.0,
            "crisis_date": datetime(2022, 1, 10).date(),
            "crisis_price": 3.2,
            "launch_date": datetime(2020, 8, 28).date()  # Much older token
        },
        {
            "address": "0xcF6BB5389c92Bdda8a3747Ddb454cB7a64626C63",  # XVS
            "symbol": "XVS",
            "baseline_price": 130.0,
            "crisis_date": datetime(2021, 5, 19).date(),
            "crisis_price": 25.0,
            "launch_date": datetime(2020, 10, 1).date()
        },
        {
            "address": "0x64aa3364F17a4D01c6f1751Fd97C2BD3D7e7f1D5",  # OHM
            "symbol": "OHM",
            "baseline_price": 520.0,
            "crisis_date": datetime(2021, 12, 1).date(),
            "crisis_price": 80.0,
            "launch_date": datetime(2021, 3, 1).date()
        },
        {
            "address": "0xD31a59c85aE9D8edEFeC411D448f90841571b89c",  # wSOL
            "symbol": "wSOL",
            "baseline_price": 35.0,
            "crisis_date": datetime(2022, 11, 9).date(),
            "crisis_price": 13.5,
            "launch_date": datetime(2020, 4, 1).date()  # SOL has been around
        }
    ]
    
    all_price_data = []
    
    for token in crisis_tokens:
        # Generate price history from launch to present
        start_date = token["launch_date"]
        end_date = datetime.now().date()
        current_date = start_date
        
        current_price = token["baseline_price"]
        
        while current_date <= end_date:
            # Calculate days to crisis
            days_to_crisis = (token["crisis_date"] - current_date).days
            
            if days_to_crisis > 30:
                # Normal market volatility (±5% daily)
                daily_change = np.random.normal(0, 0.02)
                price_change_pct = daily_change * 100
                
            elif days_to_crisis > 0:
                # Building up to crisis - increased volatility
                daily_change = np.random.normal(-0.01, 0.04)  # Slight downward trend
                price_change_pct = daily_change * 100
                
            elif days_to_crisis == 0:
                # Crisis day - major drop
                drop_ratio = token["crisis_price"] / token["baseline_price"]
                daily_change = drop_ratio - 1  # Negative percentage
                price_change_pct = daily_change * 100
                
            elif days_to_crisis >= -30:
                # Recovery period after crisis (30 days)
                days_since_crisis = abs(days_to_crisis)
                if days_since_crisis <= 7:
                    # Immediate aftermath - continued volatility
                    daily_change = np.random.normal(0.02, 0.06)  # Slight recovery bias
                else:
                    # Later recovery - gradual improvement
                    daily_change = np.random.normal(0.03, 0.04)  # Recovery trend
                price_change_pct = daily_change * 100
                
            else:
                # Long-term post-crisis
                daily_change = np.random.normal(0.01, 0.03)  # Gradual recovery
                price_change_pct = daily_change * 100
            
            # Apply price change
            new_price = current_price * (1 + daily_change)
            new_price = max(new_price, 0.01)  # Floor price
            
            # Generate other market data
            volume_24h = np.random.uniform(100000, 5000000)  # $100K - $5M daily volume
            market_cap = new_price * np.random.uniform(50000000, 500000000)  # Varies by token
            liquidity = volume_24h * np.random.uniform(0.1, 0.5)  # 10-50% of volume
            
            # High/low prices (within reasonable bounds of current price)
            high_24h = new_price * np.random.uniform(1.0, 1.1)
            low_24h = new_price * np.random.uniform(0.9, 1.0)
            
            all_price_data.append({
                "token_address": token["address"],
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


def load_to_bigquery(df, config, table_name):
    """Load DataFrame to BigQuery table."""
    client = bigquery.Client(project=config.project_id)
    table_id = f"{config.project_id}.{config.dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )
    
    print(f"Loading {len(df)} rows to {table_name}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    table = client.get_table(table_id)
    print(f"✓ Loaded {table.num_rows} rows to {table_id}")


def get_args():
    """Parse command line arguments."""
    # Get defaults from environment variables if available
    default_project = os.environ.get('PROJECT_ID')
    default_dataset = os.environ.get('DATASET_ID')
    default_target = f"{default_project}.{default_dataset}" if default_project and default_dataset else None
    
    parser = argparse.ArgumentParser(
        description="Generate Milestone 1 (M1) foundation data: DEX pools and token price history"
    )
    parser.add_argument(
        "--target",
        default=default_target,
        required=default_target is None,
        help='BigQuery target in format PROJECT_ID.DATASET_ID' + 
             (f' (default: {default_target})' if default_target else ' (required)')
    )
    parser.add_argument(
        "--pools",
        type=int,
        default=50,
        help="Number of DEX pools to generate (default: 50)"
    )
    
    return parser.parse_args()


def main():
    # Parse command line arguments
    args = get_args()
    
    # Parse target format: PROJECT_ID.DATASET_ID
    try:
        target_parts = args.target.split('.')
        if len(target_parts) != 2:
            raise ValueError(f"Invalid target format. Expected PROJECT_ID.DATASET_ID, got: {args.target}")
        
        project_id, dataset_id = target_parts
        
        # Validate parts are not empty
        if not project_id.strip() or not dataset_id.strip():
            raise ValueError("Project ID and Dataset ID cannot be empty")
            
        # Create configuration object
        config = BigQueryConfig(project_id=project_id.strip(), dataset_id=dataset_id.strip())
            
    except ValueError as e:
        print(f"✗ Error parsing target: {e}")
        print("Expected format: PROJECT_ID.DATASET_ID")
        print("Example: --target nansen-label.phoenix_flipper")
        exit(1)
    
    print("=" * 80)
    print("Generating Milestone 1 (M1) Foundation Data")
    print("=" * 80)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Pools to generate: {args.pools}")
    print()
    
    try:
        # Create dataset
        create_dataset_if_not_exists(config)
        
        # Generate DEX pools data
        print(f"\nM1.1 - Generating dim_dex_pools data ({args.pools} pools)...")
        df_pools = generate_dim_dex_pools(count=args.pools)
        load_to_bigquery(df_pools, config, "dim_dex_pools")
        
        # Generate token price history data
        print(f"\nM1.2 - Generating dim_token_price_history data...")
        df_price_history = generate_token_price_history()
        load_to_bigquery(df_price_history, config, "dim_token_price_history")
        
        print("\n" + "=" * 80)
        print("✓ Milestone 1 (M1) data generation complete!")
        print("=" * 80)
        print(f"\nDataset: {config.project_id}.{config.dataset_id}")
        print(f"- dim_dex_pools: {len(df_pools)} pools")
        print(f"- dim_token_price_history: {len(df_price_history)} price records")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
