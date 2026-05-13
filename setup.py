# ============================================
# SETUP SCRIPT - QUICK START
# ============================================

import sys
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def print_header(text):
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def run_command(cmd, description):
    """Run a command with output streaming and error handling."""
    print(f"\n[RUN] {description}...")
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        assert process.stdout is not None

        for line in process.stdout:
            print(line.rstrip())

        return_code = process.wait()
        if return_code == 0:
            print(f"[OK] {description} completed")
            return True

        print(f"[ERROR] {description} failed")
        return False
    except KeyboardInterrupt:
        print(f"[ERROR] {description} interrupted")
        return False
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        return False


def setup_project():
    """Setup the entire project"""
    print_header("FLIGHT ANALYTICS ETL - PROJECT SETUP")
    
    # Step 1: Create directories
    print_header("Step 1: Creating directories...")
    dirs = [
        BASE_DIR / "data" / "raw",
        BASE_DIR / "data" / "processed",
        BASE_DIR / "tests",
        BASE_DIR / "dbt" / "models" / "staging",
        BASE_DIR / "dbt" / "models" / "marts",
        BASE_DIR / "logs"
    ]
    
    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"[CREATE] {dir_path}")
    
    # Step 2: Install dependencies
    print_header("Step 2: Installing Python dependencies...")
    if not run_command([
        sys.executable,
        "-m",
        "pip",
        "install",
        "-r",
        str(BASE_DIR / "requirements.txt")
    ], "Pip install"):
        sys.exit(1)
    
    # Step 3: Generate sample data
    print_header("Step 3: Generating sample data...")
    if not run_command([
        sys.executable,
        str(BASE_DIR / "etl_data_generator.py")
    ], "Data generation"):
        sys.exit(1)
    
    # Step 4: Run ETL pipeline
    print_header("Step 4: Running ETL pipeline...")
    if not run_command([
        sys.executable,
        str(BASE_DIR / "etl_pipeline.py")
    ], "ETL pipeline (may take a few minutes)"):
        sys.exit(1)
    
    # Step 5: Verify installation
    print_header("Step 5: Verifying installation...")
    
    # Check data files
    data_dir = BASE_DIR / "data" / "raw"
    expected_files = ["flights.csv", "weather.csv", "revenue.csv"]
    
    print("\n[CHECK] Raw data files:")
    all_present = True
    for filename in expected_files:
        filepath = data_dir / filename
        if filepath.exists():
            size = filepath.stat().st_size / 1024 / 1024  # MB
            print(f"  ✓ {filename} ({size:.2f} MB)")
        else:
            print(f"  ✗ {filename} (MISSING)")
            all_present = False
    
    # Check database
    db_path = BASE_DIR / "data" / "flights.duckdb"
    if db_path.exists():
        size = db_path.stat().st_size / 1024 / 1024  # MB
        print(f"\n[CHECK] Database: ✓ flights.duckdb ({size:.2f} MB)")
    else:
        print("\n[CHECK] Database: ✗ flights.duckdb (MISSING)")
        all_present = False
    
    # Final status
    print_header("Setup Complete!")
    
    if all_present:
        print("""
✓ Project setup successful!

Next steps:
1. Run the dashboard:
   python etl_pipeline.py
   
2. Access the dashboard at:
   http://localhost:8050

3. Run tests:
   pytest tests/ -v

4. Setup Airflow (optional):
   airflow db init
   cp airflow_dag.py ~/airflow/dags/
   airflow scheduler

5. Run dbt (optional):
   cd dbt
   dbt run
   dbt test
        """)
    else:
        print("\n✗ Setup encountered issues. Please check the logs above.")
        sys.exit(1)


if __name__ == '__main__':
    setup_project()
