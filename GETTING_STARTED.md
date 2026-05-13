# GETTING STARTED GUIDE

## Quick Start (5 minutes)

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Generate Sample Data

```bash
python etl_data_generator.py
```

This creates:
- `data/raw/flights.csv` - Flight data in CSV format
- `data/raw/flights.parquet` - Flight data in Parquet format
- `data/raw/weather.csv` - Weather data
- `data/raw/revenue.csv` - Revenue data

### 3. Run the ETL Pipeline

```bash
python etl_pipeline.py
```

This will:
- Extract data from multiple sources
- Transform and validate the data
- Load into DuckDB analytics database
- Launch interactive dashboard

### 4. Access the Dashboard

Open your browser to: **http://localhost:8050**

---

## Detailed Installation

### Windows Setup

```powershell
# 1. Create virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run setup script
python setup.py

# 4. Run dashboard
python etl_pipeline.py
```

### Linux/Mac Setup

```bash
# 1. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run setup script
python setup.py

# 4. Run dashboard
python etl_pipeline.py
```

---

## Advanced Features

### Airflow Orchestration

```bash
# Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Copy DAG to Airflow
cp airflow_dag.py $AIRFLOW_HOME/dags/

# Start scheduler (in one terminal)
airflow scheduler

# Start webserver (in another terminal)
airflow webserver -p 8080
```

Access Airflow UI: **http://localhost:8080**

### dbt Transformations

```bash
# Install dbt
pip install dbt-core dbt-duckdb

# Initialize dbt project (already done)
cd dbt

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=.

# Run specific test file
pytest tests/test_etl_pipeline.py -v
```

---

## File Structure

```
flight-analytics-etl/
├── etl_data_generator.py          # Data source generator
├── etl_pipeline.py                # Main ETL pipeline
├── airflow_dag.py                 # Airflow orchestration
├── setup.py                       # Project setup script
├── requirements.txt               # Python dependencies
├── README.md                      # Full documentation
├── GETTING_STARTED.md             # This file
│
├── data/
│   ├── raw/                       # Raw data sources
│   │   ├── flights.csv
│   │   ├── flights.parquet
│   │   ├── weather.csv
│   │   └── revenue.csv
│   ├── processed/                 # Processed data
│   └── flights.duckdb             # Analytics database
│
├── dbt/                           # dbt transformation models
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_flights.sql
│   │   │   └── stg_weather.sql
│   │   └── marts/
│   │       ├── fct_flights.sql
│   │       ├── dim_carriers.sql
│   │       └── dim_routes.sql
│   ├── tests/
│   │   ├── fct_flights_valid_routes.sql
│   │   ├── fct_flights_delay_consistency.sql
│   │   └── generic_delay_non_negative.sql
│   ├── dbt_project.yml
│   └── models/schema.yml
│
└── tests/                         # Python unit tests
    ├── test_etl_pipeline.py
    └── test_data_quality.py
```

---

## Common Tasks

### Query Data with DuckDB

```python
import duckdb

conn = duckdb.connect('data/flights.duckdb')

# Get top airlines
df = conn.execute("""
    SELECT carrier_name, total_flights, avg_delay
    FROM carrier_stats
    ORDER BY total_flights DESC
    LIMIT 5
""").fetchdf()

print(df)
```

### Generate Reports

```bash
# Create summary report
python -c "
import duckdb
conn = duckdb.connect('data/flights.duckdb')
stats = conn.execute('SELECT * FROM carrier_stats').fetchdf()
print(stats.to_string())
"
```

### Export Data

```python
import duckdb

conn = duckdb.connect('data/flights.duckdb')

# Export to CSV
conn.execute("""
    COPY (SELECT * FROM flights LIMIT 1000)
    TO 'data/flights_export.csv' (FORMAT CSV, HEADER TRUE)
""")

# Export to Parquet
conn.execute("""
    COPY (SELECT * FROM flights LIMIT 1000)
    TO 'data/flights_export.parquet' (FORMAT PARQUET)
""")
```

---

## Troubleshooting

### Issue: Port 8050 already in use

```bash
# Find and kill process
lsof -i :8050
kill -9 <PID>

# Or use different port
python etl_pipeline.py --port 8051
```

### Issue: Out of memory with Spark

```python
# Reduce data volume
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Or limit rows
flights_df = flights_df.limit(5000)
```

### Issue: DuckDB table already exists

```python
# Drop table first
conn.execute("DROP TABLE IF EXISTS flights")

# Or use CREATE OR REPLACE
conn.execute("CREATE OR REPLACE TABLE flights AS SELECT * FROM temp")
```

### Issue: Parquet file not found

Make sure data generator was run:
```bash
python etl_data_generator.py
```

---

## Performance Tips

1. **Use Parquet format** for large datasets (compression: 47% reduction)
2. **Partition data** by date or category for faster queries
3. **Create indexes** in DuckDB for frequently queried columns
4. **Cache intermediate** Spark DataFrames
5. **Use columnar storage** (Parquet, DuckDB) instead of CSV

---

## Next Steps

1. ✅ Install dependencies
2. ✅ Generate sample data
3. ✅ Run ETL pipeline
4. ✅ View dashboard
5. 🔲 Set up Airflow for production scheduling
6. 🔲 Implement dbt for advanced transformations
7. 🔲 Deploy to cloud (AWS EMR, GCP Dataproc, Azure Synapse)
8. 🔲 Integrate with BI tool (Tableau, Looker, Power BI)

---

For more information, see [README.md](README.md)
