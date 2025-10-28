"""
BigQuery helper functions for Phoenix Flipper project.
Provides utilities for loading UDFs, common query patterns, and data loading.
"""
import os
import argparse
from pathlib import Path
from collections import namedtuple
from google.cloud import bigquery
import pandas as pd


# Configuration
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])


def load_ethereum_udfs():
    """Load Ethereum UDF definitions from lib/ethereum_udfs.sql"""
    lib_dir = Path(__file__).parent
    udf_file = lib_dir / "ethereum_udfs.sql"
    
    with open(udf_file, 'r') as f:
        return f.read()


def create_query_with_udfs(query_sql):
    """Create a complete query with UDFs prepended"""
    udfs = load_ethereum_udfs()
    return f"{udfs}\n\n{query_sql}"


def get_standard_args(description):
    """Parse standard command line arguments for Phoenix Flipper scripts."""
    parser = argparse.ArgumentParser(description=description)
    
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


def execute_query(client, query, description="query"):
    """Execute a BigQuery query with error handling."""
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        return df
    except Exception as e:
        raise Exception(f"{description} failed: {e}")


def load_to_bigquery_table(df, config, table_name, schema, dry_run=False, validator_func=None, sample_func=None):
    """Generic function to load DataFrame to BigQuery table."""
    if len(df) == 0:
        print("⚠️  No data to load")
        return
    
    table_id = f"{config.project_id}.{config.dataset_id}.{table_name}"
    
    # Run validation if provided
    if validator_func:
        validator_func(df)
    
    # Show sample if provided
    if sample_func:
        sample_func(df)
    
    if dry_run:
        print(f"🔍 DRY RUN: Would load {len(df)} records to {table_name}")
        return
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    
    try:
        print(f"📤 Loading {len(df)} records to {table_name}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print("✅ Data loaded successfully")
        
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        raise


# Common constants for Ethereum analysis
ETHEREUM_CONSTANTS = {
    'UNISWAP_V2_FACTORY': '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f',
    'V2_PAIR_CREATED_TOPIC': '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
    'V2_SWAP_TOPIC': '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
    'ZERO_ADDRESS': '0x0000000000000000000000000000000000000000',
    'BASE_TOKENS': {
        '0x6b175474e89094c44da98b954eedeac495271d0f': 'DAI',
        '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
        '0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2': 'WETH',
    }
}
