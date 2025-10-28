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

# Configuration constants
CRISES = 12


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the complete Phoenix Flipper data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default settings (50 pools, 12 crises)
  python prep/00_run_prep.py --project my-project --dataset phoenix_flipper

  # Hard reset - drop all tables and recreate everything
  python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --hard-reset

  # Data-only mode - skip setup steps
  python prep/00_run_prep.py --project my-project --dataset phoenix_flipper --data-only
        """
    )
    
    parser.add_argument(
        '--project', 
        required=True,
        help='BigQuery project ID'
    )
    
    parser.add_argument(
        '--dataset', 
        required=True,
        help='BigQuery dataset ID'
    )
    
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
    
    return parser.parse_args()


def run_command(cmd, description, prompt_after=True):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"Running: {' '.join(str(c) for c in cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
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
    
    # Set environment variables for child processes
    env = os.environ.copy()
    env['PROJECT_ID'] = args.project
    env['DATASET_ID'] = args.dataset
    
    target = f"{args.project}.{args.dataset}"
    
    print("🏷️  Phoenix Flipper Data Pipeline")
    print("="*50)
    print(f"Project ID: {args.project}")
    print(f"Dataset ID: {args.dataset}")
    print(f"Target: {target}")
    print(f"Hard Reset: {'YES' if args.hard_reset else 'NO'}")
    print(f"DEX Pools: ALL available real pools from Ethereum (crisis tokens × base tokens)")
    print(f"Crisis Events: {CRISES}")
    print(f"Interactive Mode: {'OFF' if args.no_prompt else 'ON'}")
    print(f"Data Only Mode: {'ON' if args.data_only else 'OFF'}")
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    success = True
    interactive = not args.no_prompt
    
    # Skip setup steps if data-only mode is enabled
    if not args.data_only:
        # Step 0: Install Python dependencies
        cmd = [sys.executable, "-m", "pip", "install", "-r", str(script_dir / "requirements.txt")]
        success &= run_command(cmd, "Step 0: Installing Python Dependencies", interactive)
        if not success:
            print("❌ Pipeline failed at dependency installation")
            sys.exit(1)
        
        # Step 1: Test BigQuery connection (optional)
        if not args.skip_test:
            cmd = [sys.executable, str(script_dir / "01_test_bq.py")]
            success &= run_command(cmd, "Step 1: Testing BigQuery Connection", interactive)
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
        "--target", target
    ]
    if args.hard_reset or args.data_only:
        cmd.append("--drop")
    
    success &= run_command(cmd, "Step 2: Creating BigQuery Schemas", interactive)
    if not success:
        print("❌ Pipeline failed at schema creation")
        sys.exit(1)
    
    # Step 3: Generate crisis events first
    cmd = [
        sys.executable, 
        str(script_dir / "03_generate_crisis_data.py"), 
        "--target", target,
        "--count", str(CRISES)
    ]
    
    success &= run_command(cmd, "Step 3: Generating Crisis Events Data", interactive)
    if not success:
        print("❌ Pipeline failed at crisis data generation")
        sys.exit(1)
    
    # Step 4: Generate price history data
    cmd = [
        sys.executable, 
        str(script_dir / "04_generate_price_history.py"), 
        "--target", target
    ]
    
    success &= run_command(cmd, "Step 4: Generating Price History Data", interactive)
    if not success:
        print("❌ Pipeline failed at price history generation")
        sys.exit(1)
    
    # Step 5: Generate DEX pools data
    cmd = [
        sys.executable, 
        str(script_dir / "05_generate_dex_pools.py"), 
        "--target", target
    ]
    
    success &= run_command(cmd, "Step 5: Generating DEX Pools Data", interactive)
    if not success:
        print("❌ Pipeline failed at DEX pools generation")
        sys.exit(1)
    
    # Step 6: Verify data quality
    cmd = [
        sys.executable, 
        str(script_dir / "06_verify_data_quality.py"), 
        "--target", target
    ]
    
    success &= run_command(cmd, "Step 6: Verifying Data Quality", interactive)
    if not success:
        print("❌ Pipeline failed at data quality verification")
        sys.exit(1)
    
    # Pipeline completed successfully
    print(f"\n{'='*60}")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"✅ Python dependencies installed")
    print(f"✅ All tables created in: {target}")
    print(f"✅ Generated {CRISES} crisis events")
    print(f"✅ Generated comprehensive price history data")
    print(f"✅ Generated ALL available real DEX pools from Ethereum (crisis tokens × base tokens)")
    print(f"✅ Data quality verification passed")



if __name__ == "__main__":
    main()
