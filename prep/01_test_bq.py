#!/usr/bin/env python3
"""
Test BigQuery connection and basic functionality.
"""
import os
import sys
from pathlib import Path
from google.cloud import bigquery

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "lib"))
from bigquery_helpers import get_standard_args, execute_query

def test_connection(config):
    """Test BigQuery connection."""
    try:
        client = bigquery.Client(project=config.project_id)
        print("✓ BigQuery client initialized successfully")
        print(f"✓ Using project: {config.project_id}")
        
        # List datasets to verify connection
        datasets = list(client.list_datasets())
        print(f"✓ Connection successful. Found {len(datasets)} datasets.")
        
        if datasets:
            print("Available datasets:")
            for dataset in datasets[:5]:  # Show first 5 datasets
                print(f"  • {dataset.dataset_id}")
            if len(datasets) > 5:
                print(f"  ... and {len(datasets) - 5} more")
        else:
            print("No datasets found in this project.")

        # Test with a simple query on public data
        print("\n" + "=" * 60)
        print("Testing query execution on public dataset")
        print("=" * 60)
        
        query = """
        SELECT 
            LOWER(word) as word,
            word_count,
            corpus
        FROM `bigquery-public-data.samples.shakespeare`
        WHERE word_count > 100
        ORDER BY word_count DESC
        LIMIT 10
        """
        
        print("Running test query...")
        results_df = execute_query(client, query, "Shakespeare test query")
        
        print("✓ Query executed successfully!")
        print("\nSample results:")
        print(f"{'Word':<15} {'Count':<8} Corpus")
        print("-" * 40)
        
        for _, row in results_df.iterrows():
            print(f"{row['word']:<15} {row['word_count']:<8} {row['corpus']}")
        
        print(f"\n✓ BigQuery connection and query test completed successfully!")
        return True
            
    except Exception as e:
        print(f"✗ BigQuery connection failed: {e}")
        return False

def main():
    """Main function to test BigQuery connection."""
    # Parse command line arguments using standard helper
    config, dry_run = get_standard_args("Test BigQuery connection and basic functionality")
    
    print("Testing BigQuery connection...")
    success = test_connection(config)
    if success:
        print("\n🎉 BigQuery setup is working correctly!")
    else:
        print("\n❌ BigQuery setup needs attention.")
        exit(1)

if __name__ == "__main__":
    main()