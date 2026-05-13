# ============================================
# AIRFLOW DAG - FLIGHT ANALYTICS ETL
# Orchestrates the complete ETL pipeline
# ============================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import dag
from datetime import datetime, timedelta
import sys
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Add project path to sys.path
PROJECT_PATH = Path(__file__).resolve().parent
if str(PROJECT_PATH) not in sys.path:
    sys.path.insert(0, str(PROJECT_PATH))

DB_PATH = PROJECT_PATH / 'data' / 'flights.duckdb'

from etl_pipeline import ETLPipeline

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

# DAG definition
dag = DAG(
    'flight_analytics_etl',
    default_args=default_args,
    description='Complete ETL pipeline for flight analytics',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['ETL', 'Analytics', 'Flights']
)

# Task 1: Extract Data

def task_extract():
    """Extract data from sources"""
    logger.info("[TASK] Extracting data sources...")
    pipeline = ETLPipeline()
    extracted = pipeline.extract_all_sources()
    logger.info(f"[SUCCESS] Extracted {len(extracted)} sources")
    return True

task_extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=task_extract,
    dag=dag
)

# Task 2: Transform Data
def task_transform():
    """Transform extracted data"""
    print("[TASK] Transforming data...")
    pipeline = ETLPipeline()
    extracted = pipeline.extract_all_sources()
    transformed = pipeline.transform_all(extracted)
    print(f"[SUCCESS] Transformed {len(transformed)} sources")
    return True

task_transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=task_transform,
    dag=dag
)

# Task 3: Load Data to DuckDB
def task_load():
    """Load data to DuckDB"""
    print("[TASK] Loading data to DuckDB...")
    pipeline = ETLPipeline()
    extracted = pipeline.extract_all_sources()
    transformed = pipeline.transform_all(extracted)
    db_path = pipeline.load_to_duckdb(transformed)
    print(f"[SUCCESS] Data loaded to {db_path}")
    return db_path

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=task_load,
    dag=dag
)

# Task 5: Data Validation
def task_validate():
    """Validate loaded data"""
    print("[TASK] Validating data...")
    import duckdb
    
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Check tables
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"[CHECK] Found {len(tables)} tables")
        
        # Check row counts
        for table in tables:
            table_name = table[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[CHECK] {table_name}: {count} rows")
        
        conn.close()
        print("[SUCCESS] Data validation passed")
        return True
    except Exception as e:
        print(f"[ERROR] Validation failed: {e}")
        conn.close()
        return False

task_validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=task_validate,
    dag=dag
)

# Task 6: Generate Reports
def task_generate_reports():
    """Generate summary reports"""
    print("[TASK] Generating reports...")
    import duckdb
    
    conn = duckdb.connect(str(DB_PATH))
    
    try:
        # Summary statistics
        flight_summary = conn.execute("""
            SELECT 
                COUNT(*) as total_flights,
                ROUND(AVG(dep_delay), 2) as avg_delay,
                ROUND(AVG(distance), 2) as avg_distance
            FROM flights
        """).fetchdf()
        
        print("[REPORT] Flight Summary:")
        print(flight_summary.to_string())
        
        # Top carriers
        top_carriers = conn.execute("""
            SELECT * FROM carrier_stats LIMIT 5
        """).fetchdf()
        
        print("\n[REPORT] Top 5 Airlines by Volume:")
        print(top_carriers.to_string())
        
        conn.close()
        print("[SUCCESS] Reports generated")
        return True
    except Exception as e:
        print(f"[ERROR] Report generation failed: {e}")
        conn.close()
        return False

task_reports = PythonOperator(
    task_id='generate_reports',
    python_callable=task_generate_reports,
    dag=dag
)

# Set dependencies (pipeline flow)
task_extract_data >> task_transform_data >> task_load_data >> task_validate_data >> task_reports
