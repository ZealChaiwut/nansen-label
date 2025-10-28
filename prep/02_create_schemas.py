#!/usr/bin/env python3
"""
Create BigQuery table schemas for Phoenix Flipper project.
Executes all SQL schema files with option to drop existing tables.
"""
import os
import sys
import glob
from pathlib import Path
from google.cloud import bigquery

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "lib"))
from bigquery_helpers import get_standard_args, execute_query



def create_dataset_if_not_exists(client, config):
    """Create the dataset if it doesn't exist."""
    dataset_id = f"{config.project_id}.{config.dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_id)
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
        print(f"  ✓ Dropped {table_name}")
    except Exception:
        # Table doesn't exist, nothing to drop
        pass


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
            print(f"  ✓ Created {table_name}")
        else:
            print(f"  ✓ Executed {file_path.name}")
            
    except Exception as e:
        print(f"  ✗ Error executing {file_path.name}: {e}")
        raise


def get_args():
    """Parse command line arguments with custom --drop option."""
    import argparse
    
    project_id = os.environ.get('PROJECT_ID', '')
    dataset_id = os.environ.get('DATASET_ID', '')
    default_target = f"{project_id}.{dataset_id}" if project_id and dataset_id else ""
    
    parser = argparse.ArgumentParser(description="Create BigQuery schemas for Phoenix Flipper project")
    
    parser.add_argument('--target', 
                       required=not bool(default_target),
                       default=default_target,
                       help='Target in format PROJECT_ID.DATASET_ID')
    
    parser.add_argument("--drop",
                       action="store_true",
                       help="Drop existing tables before creating new ones")
    
    args = parser.parse_args()
    
    if '.' not in args.target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    from bigquery_helpers import BigQueryConfig
    project_id, dataset_id = args.target.split('.', 1)
    return BigQueryConfig(project_id=project_id, dataset_id=dataset_id), args.drop


def main():
    try:
        config, drop_tables = get_args()
        
        print("Creating BigQuery Schemas for Phoenix Flipper Project")
        print("=" * 60)
        print(f"Project: {config.project_id}")
        print(f"Dataset: {config.dataset_id}")
        print(f"Drop existing tables: {'Yes' if drop_tables else 'No'}")
        print()
        
        # Initialize BigQuery client
        client = bigquery.Client(project=config.project_id)
        
        # Create dataset if needed
        create_dataset_if_not_exists(client, config)
        
        # Get all schema files
        schema_files = get_schema_files()
        
        # Execute each schema file
        for file_path in schema_files:
            execute_schema_file(client, config, file_path, drop_tables)
        
        print(f"✓ Created {len(schema_files)} schemas successfully")
        
        # List created tables
        dataset_ref = client.dataset(config.dataset_id)
        tables = list(client.list_tables(dataset_ref))
        
        print(f"\nTables in {config.project_id}.{config.dataset_id}:")
        for table in tables:
            print(f"  - {table.table_id}")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
