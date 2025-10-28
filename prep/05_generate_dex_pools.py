#!/usr/bin/env python3
"""
Generate DEX pools data for Phoenix Flipper project.
Creates dim_dex_pools from real Ethereum logs based on crisis tokens.
"""
import os
import sys
from pathlib import Path
from google.cloud import bigquery
import pandas as pd
import numpy as np

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "lib"))
from bigquery_helpers import get_standard_args, BigQueryConfig, execute_query, load_to_bigquery_table, create_query_with_udfs, ETHEREUM_CONSTANTS


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


def generate_dim_dex_pools(config):
    """Generate DEX pool data using REAL pool addresses from BigQuery public data."""
    from google.cloud import bigquery
    
    # First, get the crisis tokens from the crisis_events_with_window table
    print("🔍 Reading crisis tokens from crisis_events_with_window table...")
    
    client = bigquery.Client(project=config.project_id)
    crisis_query = f"""
        SELECT DISTINCT LOWER(token_address) as token_address
        FROM `{config.project_id}.{config.dataset_id}.crisis_events_with_window`
        ORDER BY token_address
    """
    
    try:
        crisis_result = execute_query(client, crisis_query, "crisis events")
        crisis_tokens = crisis_result['token_address'].tolist()
        print(f"✅ Found {len(crisis_tokens)} crisis tokens")
    except Exception as e:
        print(f"❌ Crisis events table not found: {e}")
        raise Exception(f"Crisis events table not found: {e}")
    
    # Get constants from shared library
    UNISWAP_V2_FACTORY = ETHEREUM_CONSTANTS['UNISWAP_V2_FACTORY']
    V2_PAIR_CREATED_TOPIC = ETHEREUM_CONSTANTS['V2_PAIR_CREATED_TOPIC']
    BASE_TOKEN_SYMBOLS = ETHEREUM_CONSTANTS['BASE_TOKENS']
    
    # Get base token addresses from the dictionary
    base_tokens = list(BASE_TOKEN_SYMBOLS.keys())
    
    # Create dynamic token symbol mapping 
    token_symbols = {}
    
    # Add crisis tokens with CRISIS labels
    for i, token in enumerate(crisis_tokens, 1):
        token_symbols[token] = f"CRISIS{i}"
    
    # Add base token symbols
    token_symbols.update(BASE_TOKEN_SYMBOLS)
    
    print(f"🔍 Querying Uniswap V2 pools for {len(crisis_tokens)} crisis tokens...")
    client = bigquery.Client()
    
    # Create SQL arrays for token filtering
    crisis_tokens_sql = "', '".join([t.lower() for t in crisis_tokens])
    base_tokens_sql = "', '".join([t.lower() for t in base_tokens])
    
    # Create query with UDFs using helper function
    main_query = f"""
    
    SELECT
      block_timestamp,
      GET_DEX_PROTOCOL(address) AS dex_protocol,
      'ethereum' as chain,
      EXTRACT_TOKEN_ADDRESS(topics, 1) AS token0_address,
      EXTRACT_TOKEN_ADDRESS(topics, 2) AS token1_address,
      EXTRACT_V2_PAIR_ADDRESS(data) AS pool_address
    FROM `bigquery-public-data.crypto_ethereum.logs`
    WHERE address = '{UNISWAP_V2_FACTORY}'
      AND topics[SAFE_OFFSET(0)] = '{V2_PAIR_CREATED_TOPIC}'  -- V2 PairCreated
      AND block_timestamp >= '2020-01-01'
      AND (
        (EXTRACT_TOKEN_ADDRESS(topics, 1) IN ('{crisis_tokens_sql}') 
         AND EXTRACT_TOKEN_ADDRESS(topics, 2) IN ('{base_tokens_sql}'))
        OR 
        (EXTRACT_TOKEN_ADDRESS(topics, 1) IN ('{base_tokens_sql}')
         AND EXTRACT_TOKEN_ADDRESS(topics, 2) IN ('{crisis_tokens_sql}'))
      )
    ORDER BY block_timestamp DESC
    """
    
    # Combine UDFs with main query
    query = create_query_with_udfs(main_query)
    
    try:
        real_pools_df = execute_query(client, query, "DEX pools from Ethereum logs")
        
        if len(real_pools_df) == 0:
            print("❌ No DEX pools found for crisis tokens")
            raise Exception("No real DEX pools found for crisis tokens")
        
        # Convert to our format - ONLY real pools
        data = []
        for _, row in real_pools_df.iterrows():
            # Addresses are already lowercase from query
            token0_addr = str(row['token0_address']) 
            token1_addr = str(row['token1_address'])
            pool_addr = str(row['pool_address'])
            
            # Ensure 0x prefix
            if not token0_addr.startswith('0x'):
                token0_addr = '0x' + token0_addr
            if not token1_addr.startswith('0x'):
                token1_addr = '0x' + token1_addr
            if not pool_addr.startswith('0x'):
                pool_addr = '0x' + pool_addr
                
            # Skip if addresses are invalid length
            if len(token0_addr) != 42 or len(token1_addr) != 42 or len(pool_addr) != 42:
                continue
                
            # Get symbols (addresses are already lowercase)
            token0_symbol = token_symbols.get(token0_addr, "UNK")
            token1_symbol = token_symbols.get(token1_addr, "UNK")
            
            data.append({
                "pool_address": pool_addr,
                "pool_name": f"{token0_symbol}-{token1_symbol} Pool",
                "token0_address": token0_addr,
                "token0_symbol": token0_symbol,
                "token1_address": token1_addr, 
                "token1_symbol": token1_symbol,
                "dex_protocol": str(row['dex_protocol']),
                "chain": "ethereum"
            })
        
        result_df = pd.DataFrame(data)
        print(f"✅ Found {len(result_df)} real DEX pools")
        return result_df
        
    except Exception as e:
        print(f"❌ BigQuery query failed: {e}")
        raise Exception(f"BigQuery pool query failed: {e}")


def load_to_bigquery(df, config, table_name):
    """Load DataFrame to BigQuery table."""
    if len(df) == 0:
        raise Exception(f"No data generated for table {table_name}")
    
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


def main():
    # Parse command line arguments using standard helper
    config, dry_run = get_standard_args("Generate DEX pools data from real Ethereum logs")
    
    print("=" * 80)
    print("Generating DEX Pools Data from Real Ethereum Logs")
    print("=" * 80)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Source: crisis_events_with_window + bigquery-public-data.crypto_ethereum")
    print()
    
    try:
        # Create dataset
        create_dataset_if_not_exists(config)
        
        # Generate DEX pools data
        df_pools = generate_dim_dex_pools(config)
        
        # Load to BigQuery using helper function
        load_to_bigquery_table(
            df_pools, 
            config, 
            "dim_dex_pools", 
            schema=None,  # Let BigQuery infer schema
            dry_run=dry_run
        )
        
        print(f"✓ DEX pools generation complete: {len(df_pools)} pools")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
