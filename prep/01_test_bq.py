#!/usr/bin/env python3
"""
Test BigQuery connection and basic functionality.
"""
import os
from google.cloud import bigquery

def test_connection():
    """Test BigQuery connection."""
    try:
        # Use PROJECT_ID from environment if available
        project_id = os.environ.get('PROJECT_ID')
        client = bigquery.Client(project=project_id)
        print("✓ BigQuery client initialized successfully")
        
        if project_id:
            print(f"✓ Using project: {project_id}")
        
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
        query_job = client.query(query)
        results = query_job.result()
        
        print("✓ Query executed successfully!")
        print("\nSample results:")
        print(f"{'Word':<15} {'Count':<8} Corpus")
        print("-" * 40)
        
        for row in results:
            print(f"{row.word:<15} {row.word_count:<8} {row.corpus}")
        
        print(f"\n✓ BigQuery connection and query test completed successfully!")
        return True
            
    except Exception as e:
        print(f"✗ BigQuery connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing BigQuery connection...")
    success = test_connection()
    if success:
        print("\n🎉 BigQuery setup is working correctly!")
    else:
        print("\n❌ BigQuery setup needs attention.")
        exit(1)