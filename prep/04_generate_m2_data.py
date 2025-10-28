#!/usr/bin/env python3
"""
Generate Milestone 2 (M2) crisis detection data for Phoenix Flipper project.
Creates crisis events with contrarian buy windows.
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


def generate_crisis_events(count=6):
    """Generate crisis events based on real historical data."""
    np.random.seed(42)
    
    # Real crisis events with historical data
    real_crises = [
        {
            "token_address": "0xf4d2888d29D722226FafA5d9B24F9164cfdF5Bd4",  # LOOKS
            "token_symbol": "LOOKS",
            "crisis_name": "Post-Launch Incentive Decline",
            "crisis_date": datetime(2022, 3, 15).date(),
            "window_days": 7
        },
        {
            "token_address": "0x4d224452801ACEd8B2F0aebE155379bb5D594381",  # APE
            "token_symbol": "APE", 
            "crisis_name": "Post-Otherside Land Sale Crash",
            "crisis_date": datetime(2022, 5, 15).date(),
            "window_days": 10
        },
        {
            "token_address": "0x6B3595068778DD592e39A122f4f5a5cF09C90fE2",  # SUSHI
            "token_symbol": "SUSHI",
            "crisis_name": "Leadership Crisis & CTO Departure", 
            "crisis_date": datetime(2022, 1, 10).date(),
            "window_days": 5
        },
        {
            "token_address": "0xcF6BB5389c92Bdda8a3747Ddb454cB7a64626C63",  # XVS
            "token_symbol": "XVS",
            "crisis_name": "Market Manipulation & Liquidations",
            "crisis_date": datetime(2021, 5, 19).date(),
            "window_days": 4
        },
        {
            "token_address": "0x64aa3364F17a4D01c6f1751Fd97C2BD3D7e7f1D5",  # OHM
            "token_symbol": "OHM",
            "crisis_name": "DeFi 2.0 Rebase Token Collapse",
            "crisis_date": datetime(2021, 12, 1).date(),
            "window_days": 14
        },
        {
            "token_address": "0xD31a59c85aE9D8edEFeC411D448f90841571b89c",  # wSOL
            "token_symbol": "wSOL",
            "crisis_name": "FTX Collapse Fallout",
            "crisis_date": datetime(2022, 11, 9).date(),
            "window_days": 8
        }
    ]
    
    data = []
    for i, crisis in enumerate(real_crises[:count]):
        # Generate a realistic pool address for this token
        pool_address = f"0x{''.join(np.random.choice(list('0123456789abcdef'), size=40))}"
        
        crisis_id = f"{crisis['token_symbol']}_crisis_{crisis['crisis_date'].strftime('%Y%m%d')}"
        
        # Calculate buy window
        window_start = crisis['crisis_date']
        window_end = crisis['crisis_date'] + timedelta(days=crisis['window_days'])
        
        data.append({
            "crisis_id": crisis_id,
            "pool_address": pool_address,
            "token_address": crisis["token_address"],
            "crisis_date": crisis["crisis_date"],
            "crisis_name": crisis["crisis_name"],
            "window_start_date": window_start,
            "window_end_date": window_end,
            "dt": crisis['crisis_date']
        })
    
    # If more crises requested than real ones, generate additional mock ones
    if count > len(real_crises):
        # Use the real crisis tokens for additional mock events
        crisis_tokens = [c["token_address"] for c in real_crises]
        
        for i in range(len(real_crises), count):
            token_addr = np.random.choice(crisis_tokens)
            token_symbol = next(c["token_symbol"] for c in real_crises if c["token_address"] == token_addr)
            
            # Generate additional crisis in different time period
            days_ago = np.random.randint(60, 400)
            crisis_date = (datetime.now() - timedelta(days=days_ago)).date()
            
            pool_address = f"0x{''.join(np.random.choice(list('0123456789abcdef'), size=40))}"
            crisis_id = f"{token_symbol}_additional_{i}_{crisis_date.strftime('%Y%m%d')}"
            
            # Realistic but varied crisis parameters
            window_days = np.random.randint(3, 12)
            
            window_start = crisis_date
            window_end = crisis_date + timedelta(days=window_days)
            
            data.append({
                "crisis_id": crisis_id,
                "pool_address": pool_address,
                "token_address": token_addr,
                "crisis_date": crisis_date,
                "crisis_name": f"{token_symbol} Additional Crisis Event",
                "window_start_date": window_start,
                "window_end_date": window_end,
                "dt": crisis_date
            })
    
    return pd.DataFrame(data)


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
        description="Generate Milestone 2 (M2) crisis detection data: crisis events with buy windows"
    )
    parser.add_argument(
        "--target",
        default=default_target,
        required=default_target is None,
        help='BigQuery target in format PROJECT_ID.DATASET_ID' + 
             (f' (default: {default_target})' if default_target else ' (required)')
    )
    parser.add_argument(
        "--count",
        type=int,
        default=6,
        help="Number of crisis events to generate (default: 6 - covers all real events)"
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
    print("Generating Milestone 2 (M2) Crisis Detection Data")
    print("=" * 80)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Crisis events to generate: {args.count}")
    print()
    
    try:
        # Create dataset
        create_dataset_if_not_exists(config)
        
        # Generate crisis events data
        print(f"\nM2.1 - Generating crisis_events_with_window data ({args.count} events)...")
        df_crisis = generate_crisis_events(count=args.count)
        load_to_bigquery(df_crisis, config, "crisis_events_with_window")
        
        print("\n" + "=" * 80)
        print("✓ Milestone 2 (M2) data generation complete!")
        print("=" * 80)
        print(f"\nDataset: {config.project_id}.{config.dataset_id}")
        print(f"- crisis_events_with_window: {len(df_crisis)} crisis events")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
