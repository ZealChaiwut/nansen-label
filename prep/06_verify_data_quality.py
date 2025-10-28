#!/usr/bin/env python3
"""
Phoenix Flipper Data Quality Verification
Verifies that generated mock data is usable and can join with real data sources.
"""
import argparse
import sys
from google.cloud import bigquery
import pandas as pd
from collections import namedtuple

# Configuration container for BigQuery project and dataset
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Verify Phoenix Flipper mock data quality and joinability"
    )
    
    parser.add_argument(
        '--target',
        required=True,
        help='Target in format PROJECT_ID.DATASET_ID'
    )
    
    return parser.parse_args()


def parse_target(target):
    """Parse target argument into project and dataset IDs."""
    if '.' not in target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    project_id, dataset_id = target.split('.', 1)
    return BigQueryConfig(project_id=project_id, dataset_id=dataset_id)


def verify_crisis_price_join(config):
    """Verify that crisis events can join with price data and show realistic patterns."""
    print("\n🔍 Testing Crisis Events ↔ Price History Join...")
    
    client = bigquery.Client(project=config.project_id)
    
    # Query to join crisis events with price data and analyze price movements
    query = f"""
    WITH crisis_price_analysis AS (
      SELECT 
        c.crisis_id,
        c.token_address,
        c.crisis_date,
        c.window_start_date,
        c.window_end_date,
        
        -- Price data around crisis
        p_before.price_usd as price_before_crisis,
        p_crisis.price_usd as price_during_crisis,
        p_after.price_usd as price_after_recovery,
        
        -- Calculate price changes
        ROUND(
          ((p_crisis.price_usd - p_before.price_usd) / p_before.price_usd) * 100, 2
        ) as crisis_drop_pct,
        ROUND(
          ((p_after.price_usd - p_crisis.price_usd) / p_crisis.price_usd) * 100, 2  
        ) as recovery_gain_pct
        
      FROM `{config.project_id}.{config.dataset_id}.crisis_events_with_window` c
      
      -- Price 7 days before crisis
      LEFT JOIN `{config.project_id}.{config.dataset_id}.dim_token_price_history` p_before
        ON c.token_address = p_before.token_address 
        AND p_before.dt = DATE_SUB(c.crisis_date, INTERVAL 7 DAY)
      
      -- Price during crisis window  
      LEFT JOIN `{config.project_id}.{config.dataset_id}.dim_token_price_history` p_crisis
        ON c.token_address = p_crisis.token_address
        AND p_crisis.dt = c.crisis_date
        
      -- Price 7 days after recovery window
      LEFT JOIN `{config.project_id}.{config.dataset_id}.dim_token_price_history` p_after
        ON c.token_address = p_after.token_address
        AND p_after.dt = DATE_ADD(c.window_end_date, INTERVAL 7 DAY)
    )
    
    SELECT *
    FROM crisis_price_analysis
    WHERE price_before_crisis IS NOT NULL 
      AND price_during_crisis IS NOT NULL 
      AND price_after_recovery IS NOT NULL
    ORDER BY RAND()
    LIMIT 2  -- Pick 2 random examples
    """
    
    try:
        results_df = client.query(query).to_dataframe()
        
        if len(results_df) == 0:
            print("❌ No joinable crisis-price data found")
            return False
            
        print(f"✅ Found {len(results_df)} crisis events with complete price data")
        
        success = True
        for _, row in results_df.iterrows():
            print(f"\n📊 Crisis Analysis: {row['crisis_id']}")
            print(f"   Token: {row['token_address'][:10]}...")
            print(f"   Crisis Date: {row['crisis_date']}")
            print(f"   Price Before: ${row['price_before_crisis']:.4f}")
            print(f"   Price During Crisis: ${row['price_during_crisis']:.4f}")
            print(f"   Price After Recovery: ${row['price_after_recovery']:.4f}")
            print(f"   📉 Crisis Drop: {row['crisis_drop_pct']}%")
            print(f"   📈 Recovery Gain: {row['recovery_gain_pct']}%")
            
            # Validate realistic price movements
            if row['crisis_drop_pct'] > -5:  # Should show some drop during crisis
                print(f"   ⚠️  Warning: Crisis drop only {row['crisis_drop_pct']}% (expected more negative)")
                success = False
            else:
                print("   ✅ Realistic crisis price drop detected")
                
        return success
        
    except Exception as e:
        print(f"❌ Crisis-price join verification failed: {e}")
        return False


def verify_dex_pools_ethereum_logs(config):
    """Verify that generated DEX pool addresses exist in public Ethereum logs."""
    print("\n🔍 Testing DEX Pools ↔ Ethereum Logs Join...")
    
    client = bigquery.Client(project=config.project_id)
    
    # Get all pool addresses from our generated data
    pools_query = f"""
    SELECT pool_address, pool_name, dex_protocol
    FROM `{config.project_id}.{config.dataset_id}.dim_dex_pools`
    ORDER BY dex_protocol, pool_name
    """
    
    try:
        pools_df = client.query(pools_query).to_dataframe()
        
        if len(pools_df) == 0:
            print("❌ No DEX pools found in our dataset")
            return False
            
        print(f"📋 Testing {len(pools_df)} DEX pool addresses...")
        
        # Create IN clause with all pool addresses
        pool_addresses = [f"'{addr}'" for addr in pools_df['pool_address'].tolist()]
        addresses_in_clause = "(" + ", ".join(pool_addresses) + ")"
        
        # Single query to check ALL pool addresses at once
        logs_query = f"""
        SELECT 
          address as pool_address,
          COUNT(*) as transaction_count,
          MIN(block_timestamp) as first_seen,
          MAX(block_timestamp) as last_seen
        FROM `bigquery-public-data.crypto_ethereum.logs`
        WHERE address IN {addresses_in_clause}
          AND block_timestamp >= '2020-01-01'
        GROUP BY address
        ORDER BY transaction_count DESC
        """
        
        print("🔍 Querying Ethereum logs for all pool addresses...")
        logs_df = client.query(logs_query).to_dataframe()
        
        # Join results with our pool data
        results_df = pools_df.merge(
            logs_df, 
            on='pool_address', 
            how='left'
        )
        
        # Fill NaN values for pools not found in logs
        results_df['transaction_count'] = results_df['transaction_count'].fillna(0)
        
        # Report results
        real_pools = results_df[results_df['transaction_count'] > 0]
        mock_pools = results_df[results_df['transaction_count'] == 0]
        
        if len(real_pools) > 0:
            print(f"\n✅ Found {len(real_pools)} REAL pools with Ethereum transactions:")
            for _, pool in real_pools.iterrows():
                first_seen = pool['first_seen'].date() if pd.notna(pool['first_seen']) else 'Unknown'
                last_seen = pool['last_seen'].date() if pd.notna(pool['last_seen']) else 'Unknown'
                print(f"   🏊 {pool['pool_name']} ({pool['dex_protocol']})")
                print(f"      📍 {pool['pool_address']}")
                print(f"      📊 {int(pool['transaction_count']):,} transactions ({first_seen} → {last_seen})")
        
        if len(mock_pools) > 0:
            print(f"\n📦 Found {len(mock_pools)} MOCK pools (no Ethereum transactions):")
            for _, pool in mock_pools.head(5).iterrows():  # Show first 5 mock pools
                print(f"   📦 {pool['pool_name']} ({pool['dex_protocol']})")
                print(f"      📍 {pool['pool_address']}")
            
            if len(mock_pools) > 5:
                print(f"      ... and {len(mock_pools) - 5} more mock pools")
        
        print(f"\n📊 Pool Verification Summary:")
        print(f"   ✅ Real pools: {len(real_pools)}/{len(pools_df)} ({len(real_pools)/len(pools_df)*100:.1f}%)")
        print(f"   📦 Mock pools: {len(mock_pools)}/{len(pools_df)} ({len(mock_pools)/len(pools_df)*100:.1f}%)")
        
        # Consider it successful if we have at least some real pools
        if len(real_pools) > 0:
            print("   🎉 SUCCESS: Found real DEX pools that can join with Ethereum data")
            return True
        else:
            print("   ⚠️  WARNING: All pools appear to be mock data")
            return False
        
    except Exception as e:
        print(f"❌ DEX pools verification failed: {e}")
        return False


def verify_data_completeness(config):
    """Verify that all expected tables exist and have data."""
    print("\n🔍 Testing Data Completeness...")
    
    client = bigquery.Client(project=config.project_id)
    
    expected_tables = [
        'crisis_events_with_window',
        'dim_dex_pools', 
        'dim_token_price_history'
    ]
    
    success = True
    
    for table_name in expected_tables:
        try:
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{config.project_id}.{config.dataset_id}.{table_name}`
            """
            
            result = client.query(count_query).to_dataframe()
            row_count = result.iloc[0]['row_count']
            
            if row_count > 0:
                print(f"   ✅ {table_name}: {row_count:,} rows")
            else:
                print(f"   ❌ {table_name}: No data found")
                success = False
                
        except Exception as e:
            print(f"   ❌ {table_name}: Table not found or error - {e}")
            success = False
            
    return success


def main():
    """Run data quality verification."""
    args = get_args()
    
    try:
        config = parse_target(args.target)
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    print("🔍 Phoenix Flipper Data Quality Verification")
    print("=" * 60)
    print(f"Project ID: {config.project_id}")
    print(f"Dataset ID: {config.dataset_id}")
    
    success = True
    
    # Test 1: Data completeness
    success &= verify_data_completeness(config)
    
    # Test 2: Crisis events and price data join
    success &= verify_crisis_price_join(config)
    
    # Test 3: DEX pools and Ethereum logs join  
    success &= verify_dex_pools_ethereum_logs(config)
    
    # Final result
    print(f"\n{'='*60}")
    if success:
        print("🎉 DATA QUALITY VERIFICATION PASSED!")
        print("✅ Mock data is usable and realistic")
        print("✅ All joins work as expected")
        print("✅ Ready for Phoenix Flipper development")
    else:
        print("❌ DATA QUALITY VERIFICATION FAILED!")
        print("⚠️  Some issues found with mock data")
        print("🔧 Review the warnings above and regenerate if needed")
        sys.exit(1)


if __name__ == "__main__":
    main()
