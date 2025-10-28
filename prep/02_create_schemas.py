#!/usr/bin/env python3
"""
Create BigQuery table schemas for Phoenix Flipper project.
Executes all SQL schema files with option to drop existing tables.
"""
import os
import argparse
import glob
from pathlib import Path
from collections import namedtuple
from google.cloud import bigquery

# Configuration container for BigQuery project and dataset
BigQueryConfig = namedtuple('BigQueryConfig', ['project_id', 'dataset_id'])



def create_dataset_if_not_exists(client, config):
    """Create the dataset if it doesn't exist."""
    dataset_id = f"{config.project_id}.{config.dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_id)
        print(f"✓ Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"✓ Created dataset: {dataset_id}")


def get_schema_files():
    """Get all SQL schema files from the schemas directory."""
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    # Go up one level to find schemas directory
    schemas_dir = script_dir.parent / "schemas"
    
    if not schemas_dir.exists():
        raise FileNotFoundError(f"Schemas directory not found: {schemas_dir}")
    
    # Find all .sql files in schemas directory
    sql_files = list(schemas_dir.glob("*.sql"))
    
    if not sql_files:
        raise FileNotFoundError(f"No SQL files found in {schemas_dir}")
    
    # Sort files for consistent execution order
    sql_files.sort(key=lambda x: x.name)
    
    print(f"Found {len(sql_files)} schema files:")
    for file in sql_files:
        print(f"  - {file.name}")
    
    return sql_files


def drop_table_if_exists(client, config, table_name, drop_tables):
    """Drop table if it exists and drop_tables is True."""
    if not drop_tables:
        return
    
    table_id = f"{config.project_id}.{config.dataset_id}.{table_name}"
    
    try:
        client.get_table(table_id)
        # Table exists, drop it
        client.delete_table(table_id)
        print(f"  ✓ Dropped existing table: {table_name}")
    except Exception:
        # Table doesn't exist, nothing to drop
        print(f"  - Table {table_name} doesn't exist, nothing to drop")


def extract_table_name_from_sql(sql_content):
    """Extract table name from CREATE TABLE statement (after parameter substitution)."""
    import re
    
    # Look for pattern: CREATE TABLE IF NOT EXISTS `project.dataset.table_name`
    pattern = r'CREATE TABLE IF NOT EXISTS `[^.]+\.[^.]+\.([^`]+)`'
    match = re.search(pattern, sql_content, re.IGNORECASE)
    
    if match:
        return match.group(1)
    
    # Fallback pattern without backticks
    pattern = r'CREATE TABLE IF NOT EXISTS\s+\w+\.\w+\.(\w+)'
    match = re.search(pattern, sql_content, re.IGNORECASE)
    
    if match:
        return match.group(1)
    
    return None


def execute_schema_file(client, config, file_path, drop_tables=False):
    """Execute a single schema SQL file with parameter substitution."""
    print(f"\nProcessing {file_path.name}...")
    
    # Read SQL file
    with open(file_path, 'r') as f:
        sql_content = f.read()
    
    # Substitute parameters
    sql_content = sql_content.format(
        PROJECT_ID=config.project_id,
        DATASET_ID=config.dataset_id
    )
    
    # Extract table name for potential dropping
    table_name = extract_table_name_from_sql(sql_content)
    
    if table_name and drop_tables:
        drop_table_if_exists(client, config, table_name, drop_tables)
    
    # Execute the CREATE TABLE statement
    try:
        query_job = client.query(sql_content)
        query_job.result()  # Wait for completion
        
        if table_name:
            print(f"  ✓ Created/updated table: {table_name}")
        else:
            print(f"  ✓ Executed SQL from {file_path.name}")
            
    except Exception as e:
        print(f"  ✗ Error executing {file_path.name}: {e}")
        raise


def get_args():
    """Parse command line arguments."""
    # Get defaults from environment variables if available
    default_project = os.environ.get('PROJECT_ID')
    default_dataset = os.environ.get('DATASET_ID')
    default_target = f"{default_project}.{default_dataset}" if default_project and default_dataset else None
    
    parser = argparse.ArgumentParser(
        description="Create BigQuery schemas for Phoenix Flipper project"
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing tables before creating new ones"
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
    print("Creating BigQuery Schemas for Phoenix Flipper Project")
    print("=" * 80)
    print(f"Project: {config.project_id}")
    print(f"Dataset: {config.dataset_id}")
    print(f"Drop existing tables: {'Yes' if args.drop else 'No'}")
    print()
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=config.project_id)
        print(f"✓ Connected to BigQuery successfully!")
        
        # Create dataset if needed
        create_dataset_if_not_exists(client, config)
        
        # Get all schema files
        schema_files = get_schema_files()
        
        # Execute each schema file
        print(f"\nExecuting {len(schema_files)} schema files...")
        
        for file_path in schema_files:
            execute_schema_file(client, config, file_path, args.drop)
        
        print("\n" + "=" * 80)
        print("✓ All schemas created successfully!")
        print("=" * 80)
        
        # List created tables
        dataset_ref = client.dataset(config.dataset_id)
        tables = list(client.list_tables(dataset_ref))
        
        print(f"\nTables in {config.project_id}.{config.dataset_id}:")
        for table in tables:
            print(f"  - {table.table_id}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
