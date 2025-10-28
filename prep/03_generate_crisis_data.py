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
    
    # Popular tokens with guaranteed pools, renamed as crisis tokens for mock data
    crisis_tokens = [
        {
            "token_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  # UNI -> CRISIS1
            "crisis_name": "CRISIS1 Token Market Manipulation",
            "crisis_date": datetime(2022, 3, 15).date(),
        },
        {
            "token_address": "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0",  # MATIC -> CRISIS2
            "crisis_name": "CRISIS2 Token Governance Exploit",
            "crisis_date": datetime(2022, 5, 15).date(),
        },
        {
            "token_address": "0x514910771af9ca656af840dff83e8264ecf986ca",  # LINK -> CRISIS3
            "crisis_name": "CRISIS3 Token Flash Loan Attack", 
            "crisis_date": datetime(2022, 1, 10).date(),
        },
        {
            "token_address": "0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b",  # CRO -> CRISIS4
            "crisis_name": "CRISIS4 Token Exchange Delisting",
            "crisis_date": datetime(2021, 5, 19).date(),
        },
        {
            "token_address": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  # WBTC -> CRISIS5
            "crisis_name": "CRISIS5 Token Bridge Vulnerability",
            "crisis_date": datetime(2021, 12, 1).date(),
        },
        {
            "token_address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",  # SHIB -> CRISIS6
            "crisis_name": "CRISIS6 Token Whale Dumping Event",
            "crisis_date": datetime(2022, 11, 9).date(),
        }
    ]
    
    data = []
    for i, crisis in enumerate(crisis_tokens[:count]):
        crisis_id = f"crisis_{i+1:03d}"
        
        # Simple random buy window (3-14 days)
        window_days = np.random.randint(3, 15)
        window_start = crisis['crisis_date']
        window_end = crisis['crisis_date'] + timedelta(days=window_days)
        
        data.append({
            "crisis_id": crisis_id,
            "token_address": crisis["token_address"],
            "crisis_date": crisis["crisis_date"],
            "crisis_name": crisis["crisis_name"],
            "window_start_date": window_start,
            "window_end_date": window_end,
            "dt": crisis['crisis_date']
        })
    
    # If more crises requested than available tokens, generate additional mock ones
    if count > len(crisis_tokens):
        for i in range(len(crisis_tokens), count):
            # Pick a random token from existing ones
            base_crisis = np.random.choice(crisis_tokens)
            token_addr = base_crisis["token_address"]
            
            # Generate additional crisis in different time period
            days_ago = np.random.randint(60, 400)
            crisis_date = (datetime.now() - timedelta(days=days_ago)).date()
            
            crisis_id = f"crisis_{i+1:03d}"
            
            # Simple random window
            window_days = np.random.randint(3, 15)
            window_start = crisis_date
            window_end = crisis_date + timedelta(days=window_days)
            
            data.append({
                "crisis_id": crisis_id,
                "token_address": token_addr,
                "crisis_date": crisis_date,
                "crisis_name": f"Additional Crisis Event {i+1}",
                "window_start_date": window_start,
                "window_end_date": window_end,
                "dt": crisis_date
            })
    
    return pd.DataFrame(data)


def load_to_bigquery(df, config, table_name):
    """Load DataFrame to BigQuery table."""
    if len(df) == 0:
        print(f"⚠️ No data to load for {table_name}")
        return
    
    client = bigquery.Client(project=config.project_id)
    table_id = f"{config.project_id}.{config.dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    table = client.get_table(table_id)
    print(f"✓ Loaded {table.num_rows} rows to {table_name}")


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
        df_crisis = generate_crisis_events(count=args.count)
        load_to_bigquery(df_crisis, config, "crisis_events_with_window")
        
        print(f"✓ Crisis events generation complete: {len(df_crisis)} events")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
