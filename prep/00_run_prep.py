#!/usr/bin/env python3
"""
Phoenix Flipper Pipeline Orchestrator
Runs the complete data pipeline from schema creation to mock data generation.
"""
import argparse
import subprocess
import sys
import os
from pathlib import Path

# Add lib directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'lib'))
from bigquery_helpers import get_standard_args, BigQueryConfig

# Configuration constants
CRISES = 12


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the complete Phoenix Flipper data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default settings
  python prep/00_run_prep.py --target my-project.phoenix_flipper

  # Hard reset - drop all tables and recreate everything
  python prep/00_run_prep.py --target my-project.phoenix_flipper --hard-reset

  # Data-only mode - skip setup steps
  python prep/00_run_prep.py --target my-project.phoenix_flipper --data-only
        """
    )
    
    # Get standard arguments (--target)
    project_id = os.environ.get('PROJECT_ID', '')
    dataset_id = os.environ.get('DATASET_ID', '')
    default_target = f"{project_id}.{dataset_id}" if project_id and dataset_id else ""
    
    parser.add_argument('--target', 
                       required=not bool(default_target),
                       default=default_target,
                       help='Target in format PROJECT_ID.DATASET_ID')
    
    parser.add_argument(
        '--hard-reset', 
        action='store_true',
        help='Drop all existing tables before recreating (WARNING: destroys existing data)'
    )
    
    parser.add_argument(
        '--skip-test', 
        action='store_true',
        help='Skip BigQuery connection test'
    )
    
    parser.add_argument(
        '--no-prompt', 
        action='store_true',
        help='Skip interactive prompts between steps (run non-interactively)'
    )
    
    parser.add_argument(
        '--data-only',
        action='store_true', 
        help='Skip setup steps (pip install, BQ test) and go straight to schema creation and data generation'
    )
    
    args = parser.parse_args()
    
    if '.' not in args.target:
        raise ValueError("Target must be in format PROJECT_ID.DATASET_ID")
    
    return args


def run_command(cmd, description, prompt_after=True, env=None):
    """Run a command and handle errors."""
    print(f"\n🚀 {description}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, env=env)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        print(f"✅ {description} completed successfully")
        
        # Interactive prompt after successful completion
        if prompt_after:
            print(f"\n{'='*60}")
            print("📋 Step completed! Review the output above.")
            input("Press Enter to continue to the next step... ")
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False




def main():
    """Run the complete Phoenix Flipper pipeline."""
    args = get_args()
    
    # Parse target format: PROJECT_ID.DATASET_ID
    project_id, dataset_id = args.target.split('.', 1)
    
    # Set environment variables for child processes
    env = os.environ.copy()
    env['PROJECT_ID'] = project_id
    env['DATASET_ID'] = dataset_id
    
    print("🏷️  Phoenix Flipper Data Pipeline")
    print(f"Target: {args.target} | Reset: {'YES' if args.hard_reset else 'NO'} | Interactive: {'OFF' if args.no_prompt else 'ON'}")
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    success = True
    interactive = not args.no_prompt
    
    # Skip setup steps if data-only mode is enabled
    if not args.data_only:
        # Step 0: Install Python dependencies
        cmd = [sys.executable, "-m", "pip", "install", "-r", str(script_dir / "requirements.txt")]
        success &= run_command(cmd, "Step 0: Installing Python Dependencies", interactive, env)
        if not success:
            print("❌ Pipeline failed at dependency installation")
            sys.exit(1)
        
        # Step 1: Test BigQuery connection (optional)
        if not args.skip_test:
            cmd = [sys.executable, str(script_dir / "01_test_bq.py"), "--target", args.target]
            success &= run_command(cmd, "Step 1: Testing BigQuery Connection", interactive, env)
            if not success:
                print("❌ Pipeline failed at BigQuery connection test")
                sys.exit(1)
        else:
            print("\n⏭️  Skipping BigQuery connection test")
    else:
        print("\n🚀 DATA-ONLY MODE: Skipping setup steps, going straight to schema creation and data generation")
    
    # Step 2: Create schemas
    cmd = [
        sys.executable, 
        str(script_dir / "02_create_schemas.py"), 
        "--target", args.target
    ]
    if args.hard_reset or args.data_only:
        cmd.append("--drop")
    
    success &= run_command(cmd, "Step 2: Creating BigQuery Schemas", interactive, env)
    if not success:
        print("❌ Pipeline failed at schema creation")
        sys.exit(1)
    
    # Step 3: Generate crisis events first
    cmd = [
        sys.executable, 
        str(script_dir / "03_generate_crisis_data.py"), 
        "--target", args.target,
        "--count", str(CRISES)
    ]
    
    success &= run_command(cmd, "Step 3: Generating Crisis Events Data", interactive, env)
    if not success:
        print("❌ Pipeline failed at crisis data generation")
        sys.exit(1)
    
    # Step 4: Generate price history data
    cmd = [
        sys.executable, 
        str(script_dir / "04_generate_price_history.py"), 
        "--target", args.target
    ]
    
    success &= run_command(cmd, "Step 4: Generating Price History Data", interactive, env)
    if not success:
        print("❌ Pipeline failed at price history generation")
        sys.exit(1)
    
    # Step 5: Generate DEX pools data
    cmd = [
        sys.executable, 
        str(script_dir / "05_generate_dex_pools.py"), 
        "--target", args.target
    ]
    
    success &= run_command(cmd, "Step 5: Generating DEX Pools Data", interactive, env)
    if not success:
        print("❌ Pipeline failed at DEX pools generation")
        sys.exit(1)
    
    # Step 6: Verify data quality
    cmd = [
        sys.executable, 
        str(script_dir / "06_verify_data_quality.py"), 
        "--target", args.target
    ]
    
    success &= run_command(cmd, "Step 6: Verifying Data Quality", interactive, env)
    if not success:
        print("❌ Pipeline failed at data quality verification")
        sys.exit(1)
    
    # Pipeline completed successfully
    print(f"\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"✅ All data generated in: {args.target}")



if __name__ == "__main__":
    main()
