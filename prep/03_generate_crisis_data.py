#!/usr/bin/env python3
"""
Generate Milestone 2 (M2) crisis detection data for Phoenix Flipper project.
Creates crisis events with contrarian buy windows.
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
from bigquery_helpers import get_standard_args, load_to_bigquery_table



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


def get_args():
    """Parse command line arguments with custom --count option."""
    import argparse
    
    project_id = os.environ.get('PROJECT_ID', '')
    dataset_id = os.environ.get('DATASET_ID', '')
    default_target = f"{project_id}.{dataset_id}" if project_id and dataset_id else ""
    
    parser = argparse.ArgumentParser(description="Generate crisis detection data with buy windows")
    
    parser.add_argument('--target', 
                       required=not bool(default_target),
                       default=default_target,
                       help='Target in format PROJECT_ID.DATASET_ID')
    
    parser.add_argument("--count",
                       type=int,
                       default=6,
                       help="Number of crisis events to generate (default: 6)")
    
    args = parser.parse_args()
    
    if '.' not in args.target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    from bigquery_helpers import BigQueryConfig
    project_id, dataset_id = args.target.split('.', 1)
    return BigQueryConfig(project_id=project_id, dataset_id=dataset_id), args.count


def main():
    try:
        config, count = get_args()
        
        print("Generating Crisis Detection Data")
        print("=" * 40)
        print(f"Project: {config.project_id}")
        print(f"Dataset: {config.dataset_id}")
        print(f"Crisis events to generate: {count}")
        print()
        
        # Create dataset
        create_dataset_if_not_exists(config)
        
        # Generate crisis events data
        df_crisis = generate_crisis_events(count=count)
        
        # No schema needed since CREATE_IF_NEEDED is used
        load_to_bigquery_table(df_crisis, config, "crisis_events_with_window", schema=None)
        
        print(f"✓ Crisis events generation complete: {len(df_crisis)} events")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
