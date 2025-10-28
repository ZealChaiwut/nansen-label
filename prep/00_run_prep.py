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


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the complete Phoenix Flipper data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline with default settings
  python prep/00_run_pipeline.py --project my-project --dataset phoenix_flipper

  # Hard reset - drop all tables and recreate everything
  python prep/00_run_pipeline.py --project my-project --dataset phoenix_flipper --hard-reset

  # Custom data generation settings
  python prep/00_run_pipeline.py --project my-project --dataset phoenix_flipper --pools 100 --crises 10
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
    
    # Data generation options
    parser.add_argument(
        '--pools', 
        type=int, 
        default=50,
        help='Number of DEX pools to generate (default: 50)'
    )
    
    parser.add_argument(
        '--crises', 
        type=int, 
        default=6,
        help='Number of crisis events to generate (default: 6)'
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
    print(f"DEX Pools: {args.pools}")
    print(f"Crisis Events: {args.crises}")
    print(f"Interactive Mode: {'OFF' if args.no_prompt else 'ON'}")
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    
    success = True
    interactive = not args.no_prompt
    
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
    
    # Step 2: Create schemas
    cmd = [
        sys.executable, 
        str(script_dir / "02_create_schemas.py"), 
        "--target", target
    ]
    if args.hard_reset:
        cmd.append("--drop")
    
    success &= run_command(cmd, "Step 2: Creating BigQuery Schemas", interactive)
    if not success:
        print("❌ Pipeline failed at schema creation")
        sys.exit(1)
    
    # Step 3: Generate M1 data (DEX pools and price history)
    cmd = [
        sys.executable, 
        str(script_dir / "03_generate_m1_data.py"), 
        "--target", target,
        "--pools", str(args.pools)
    ]
    
    success &= run_command(cmd, "Step 3: Generating M1 Foundation Data", interactive)
    if not success:
        print("❌ Pipeline failed at M1 data generation")
        sys.exit(1)
    
    # Step 4: Generate M2 data (crisis events)
    cmd = [
        sys.executable, 
        str(script_dir / "04_generate_m2_data.py"), 
        "--target", target,
        "--count", str(args.crises)
    ]
    
    success &= run_command(cmd, "Step 4: Generating M2 Crisis Data", interactive)
    if not success:
        print("❌ Pipeline failed at M2 data generation")
        sys.exit(1)
    
    # Pipeline completed successfully
    print(f"\n{'='*60}")
    print("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print(f"✅ Python dependencies installed")
    print(f"✅ All tables created in: {target}")
    print(f"✅ Generated {args.pools} DEX pools")
    print(f"✅ Generated {args.crises} crisis events")
    print(f"✅ Generated comprehensive price history data")



if __name__ == "__main__":
    main()
