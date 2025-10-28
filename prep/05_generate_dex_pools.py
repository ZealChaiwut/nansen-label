#!/usr/bin/env python3
"""
Generate DEX pools data for Phoenix Flipper project.
Creates dim_dex_pools from real Ethereum logs based on crisis tokens.
"""
import argparse
import os
from collections import namedtuple
from google.cloud import bigquery
import pandas as pd
import numpy as np

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
        crisis_result = client.query(crisis_query).to_dataframe()
        crisis_tokens = crisis_result['token_address'].tolist()
        print(f"✅ Found {len(crisis_tokens)} unique crisis tokens in the table")
        for i, token in enumerate(crisis_tokens, 1):
            print(f"   {i}. {token}")
    except Exception as e:
        print(f"❌ Could not read crisis events table: {e}")
        print("💡 Make sure crisis_events_with_window table exists (run step 3 first)")
        print("🚨 CRITICAL: Cannot generate DEX pools without crisis tokens!")
        raise Exception(f"Crisis events table not found: {e}")
    
    # Major base tokens ONLY (lowercase for comparison) 
    base_tokens = [
        "0x6b175474e89094c44da98b954eedeac495271d0f",   # DAI
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",   # USDC
        "0xdac17f958d2ee523a2206206994597c13d831ec7",   # USDT
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",   # WETH
    ]
    
    # Create dynamic token symbol mapping 
    token_symbols = {}
    
    # Add crisis tokens with CRISIS labels
    for i, token in enumerate(crisis_tokens, 1):
        token_symbols[token] = f"CRISIS{i}"
    
    # Add base tokens
    base_token_symbols = {
        "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
        "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    }
    token_symbols.update(base_token_symbols)
    
    print(f"🔍 Querying BigQuery for real DEX pools containing crisis tokens...")
    
    client = bigquery.Client()
    
    print(f"\n🎯 Looking for ALL pools pairing {len(crisis_tokens)} crisis tokens with {len(base_tokens)} base tokens...")
    print(f"📋 Base tokens: DAI, USDC, USDT, WETH")
    print(f"💡 Finding all real Uniswap V2 & V3 pools for these specific crisis token pairs (no limit)")
    
    # Create parameter strings for SQL IN clauses (lowercase for consistency)
    crisis_tokens_sql = "', '".join([t.lower() for t in crisis_tokens])
    base_tokens_sql = "', '".join([t.lower() for t in base_tokens])
    
    # Advanced query using proper topic decoding (based on working query)
    query = f"""
    WITH constants AS (
      SELECT
        -- Factory Addresses
        '0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f' AS uniswap_v2_factory,
        '0x1f98431c8ad98523631ae4a59f267346ea31f984' AS uniswap_v3_factory,
        -- Event Topic Hashes
        '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' AS v2_pair_created_topic,
        '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118' AS v3_pool_created_topic
    ),
    crisis_tokens AS (
      SELECT token_address FROM UNNEST(['{crisis_tokens_sql}']) AS token_address
    ),
    base_tokens AS (
      SELECT token_address FROM UNNEST(['{base_tokens_sql}']) AS token_address  
    )
    
    SELECT
      block_timestamp,
      CASE
        WHEN address = (SELECT uniswap_v2_factory FROM constants) THEN 'Uniswap V2'
        WHEN address = (SELECT uniswap_v3_factory FROM constants) THEN 'Uniswap V3'
        ELSE 'Unknown Factory'
      END AS dex_protocol,
      'ethereum' as chain,
      -- Decode token addresses from topics
      LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(1)], 27, 40))) AS token0_address,
      LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27, 40))) AS token1_address,
      -- Decode pool/pair addresses from data field
      CASE
        WHEN address = (SELECT uniswap_v2_factory FROM constants)
          THEN LOWER(CONCAT('0x', SUBSTR(data, 27, 40)))  -- V2: Pair address in data
        WHEN address = (SELECT uniswap_v3_factory FROM constants)
          THEN LOWER(CONCAT('0x', SUBSTR(data, 1+96, 40)))  -- V3: Pool address in data
        ELSE NULL
      END AS pool_address
    FROM
      `bigquery-public-data.crypto_ethereum.logs`
    CROSS JOIN constants
    WHERE
      -- Filter by known factory addresses
      address IN (
          (SELECT uniswap_v2_factory FROM constants),
          (SELECT uniswap_v3_factory FROM constants)
        )
      -- Filter by creation event topics
      AND topics[SAFE_OFFSET(0)] IN (
          (SELECT v2_pair_created_topic FROM constants),
          (SELECT v3_pool_created_topic FROM constants)
        )
      -- Filter for crisis token + base token pairs ONLY
      AND (
        -- Crisis token as token0, base token as token1
        (LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(1)], 27, 40))) IN (SELECT token_address FROM crisis_tokens)
         AND LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27, 40))) IN (SELECT token_address FROM base_tokens))  
        OR 
        -- Base token as token0, crisis token as token1
        (LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(1)], 27, 40))) IN (SELECT token_address FROM base_tokens)
         AND LOWER(CONCAT('0x', SUBSTR(topics[SAFE_OFFSET(2)], 27, 40))) IN (SELECT token_address FROM crisis_tokens))
      )
      -- Get historical data
      AND block_timestamp >= '2020-01-01'
    ORDER BY
      block_timestamp DESC
    """
    
    try:
        print("🔍 Executing query for ALL real DEX pools (no limit)...")
        query_job = client.query(query)
        real_pools_df = query_job.to_dataframe()
        
        print(f"✅ Found {len(real_pools_df)} real DEX pools from BigQuery")
        
        if len(real_pools_df) == 0:
            print("❌ No real DEX pools found for crisis tokens")
            print("💡 This could mean:")
            print("   - Token addresses might be incorrect")
            print("   - Tokens don't have Uniswap V2 pools with major pairs") 
            print("   - BigQuery connection or access issues")
            print("🚨 CRITICAL: Cannot proceed without DEX pools!")
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
                print(f"⚠️ Skipping invalid address lengths: {pool_addr}")
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
        print(f"📊 Found {len(result_df)} REAL DEX pools from Ethereum blockchain (V2 & V3)")
        print(f"💎 Coverage: {len(crisis_tokens)} crisis tokens × {len(base_tokens)} base tokens = {len(crisis_tokens) * len(base_tokens)} possible pairs")
        print(f"✅ Success rate: {len(result_df)}/{len(crisis_tokens) * len(base_tokens)} pairs found ({len(result_df)/(len(crisis_tokens) * len(base_tokens))*100:.1f}%)")
        
        # Show some examples
        if len(result_df) > 0:
            print("\n🎯 Sample pools found:")
            for _, pool in result_df.head(5).iterrows():
                print(f"   • {pool['pool_name']} - {pool['pool_address'][:10]}...")
        
        return result_df
        
    except Exception as e:
        print(f"❌ Failed to query real pools from BigQuery: {e}")
        print("💡 Check your BigQuery connection and token addresses")
        print("🚫 NO FALLBACK TO MOCK DATA - Only real pools allowed")
        print("🚨 CRITICAL: BigQuery query failed!")
        raise Exception(f"BigQuery pool query failed: {e}")


def load_to_bigquery(df, config, table_name):
    """Load DataFrame to BigQuery table."""
    # Check for empty DataFrame - this should not happen now!
    if len(df) == 0:
        print(f"🚨 CRITICAL ERROR: {table_name} has no data to load!")
        print(f"💡 This indicates a serious issue with data generation")
        raise Exception(f"No data generated for table {table_name}")
    
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
        description="Generate DEX pools data from real Ethereum logs"
    )
    parser.add_argument(
        "--target",
        default=default_target,
        required=default_target is None,
        help='BigQuery target in format PROJECT_ID.DATASET_ID' + 
             (f' (default: {default_target})' if default_target else ' (required)')
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
        print(f"\n🏊 Generating real DEX pools from Ethereum...")
        df_pools = generate_dim_dex_pools(config)
        print(f"✅ Generated {len(df_pools)} DEX pool records")
        
        print(f"\n💾 Loading DEX pools to BigQuery...")
        load_to_bigquery(df_pools, config, "dim_dex_pools")
        
        print("\n" + "=" * 80)
        print("✓ DEX pools generation complete!")
        print("=" * 80)
        print(f"\nDataset: {config.project_id}.{config.dataset_id}")
        print(f"- dim_dex_pools: {len(df_pools)} real pools")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
