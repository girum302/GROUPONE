# ============================================
# FLIGHT ANALYTICS ETL PIPELINE
# Uses PySpark extraction + transformation of real dataset sources,
# loads transformed information into DuckDB, and exposes a Dash dashboard.
# ============================================

import sys
import subprocess
import warnings
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import numpy as np
import pandas as pd
import duckdb
import plotly.express as px
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_timestamp
from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def install_packages():
    packages = [
        'pyspark',
        'duckdb',
        'pandas',
        'plotly',
        'dash',
        'dash-bootstrap-components'
    ]

    logger.info('Verifying required packages...')
    for package in packages:
        try:
            if package == 'dash-bootstrap-components':
                __import__('dash_bootstrap_components')
            else:
                __import__(package.replace('-', '_'))
            logger.info(f'[OK] {package}')
        except ImportError:
            logger.info(f'[INSTALL] {package}')
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            logger.info(f'[INSTALLED] {package}')


install_packages()


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PARQUET_DIR = DATA_DIR / 'parquet'
DB_PATH = DATA_DIR / 'flights.duckdb'

DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

logger.info(f'Base directory: {BASE_DIR}')
logger.info(f'Data directory: {DATA_DIR}')


class ETLPipeline:
    def __init__(self, app_name='FlightAnalyticsETL'):
        logger.info('Initializing Spark session...')
        try:
            import os
            # Set dummy HADOOP_HOME for Windows
            if 'HADOOP_HOME' not in os.environ:
                os.environ['HADOOP_HOME'] = str(BASE_DIR / 'hadoop_dummy')
                (BASE_DIR / 'hadoop_dummy').mkdir(exist_ok=True)
            
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .master('local[*]') \
                .config('spark.driver.memory', '4g') \
                .config('spark.executor.memory', '4g') \
                .config('spark.sql.adaptive.enabled', 'true') \
                .config('spark.hadoop.fs.file.impl', 'org.apache.hadoop.fs.LocalFileSystem') \
                .config('spark.local.dir', str(DATA_DIR / 'temp')) \
                .getOrCreate()
            self.spark.sparkContext.setLogLevel('WARN')
            logger.info('[OK] Spark session created')
        except Exception as error:
            logger.error('Spark initialization failed. Please ensure a compatible Java runtime (Java 17+) is installed and SPARK_HOME is configured.')
            logger.error(f'Spark error: {error}')
            raise

    def extract_csv(self, filepath: str):
        logger.info(f'Extracting CSV: {filepath}')
        return self.spark.read.option('header', 'true').option('inferSchema', 'true').csv(filepath)

    def extract_parquet(self, filepath: str):
        logger.info(f'Extracting Parquet: {filepath}')
        return self.spark.read.parquet(filepath)

    def extract_all_sources(self):
        logger.info('\n' + '=' * 60)
        logger.info('EXTRACTION PHASE')
        logger.info('=' * 60)

        dataframes = {}

        flights_csv = RAW_DATA_DIR / 'flights.csv'
        if flights_csv.exists():
            dataframes['flights_csv'] = self.extract_csv(str(flights_csv))
        else:
            logger.warning('Missing flights.csv in data/raw. Falling back to available sources.')

        flights_parquet = PARQUET_DIR / 'flights_parquet'
        if flights_parquet.exists():
            dataframes['flights_parquet'] = self.extract_parquet(str(flights_parquet))
        else:
            logger.warning('Missing flights_parquet file in data/parquet.')

        crashes_csv = RAW_DATA_DIR / 'Traffic_Crashes_-_Crashes.csv'
        if crashes_csv.exists():
            dataframes['crashes'] = self.extract_csv(str(crashes_csv))
        else:
            logger.warning('Missing crash dataset in data/raw.')

        cities_csv = RAW_DATA_DIR / 'uscities.csv'
        if cities_csv.exists():
            dataframes['us_cities'] = self.extract_csv(str(cities_csv))
        else:
            logger.warning('Missing uscities.csv in data/raw.')

        if len(dataframes) == 0:
            raise FileNotFoundError('No input sources found under data/raw or data/parquet.')

        logger.info(f'[OK] Extracted {len(dataframes)} source(s): {list(dataframes.keys())}')
        return dataframes

    def transform_flights(self, flights_df):
        logger.info('Transforming flights CSV dataset...')
        df = flights_df

        for column in ['dep_delay', 'arr_delay', 'distance', 'year', 'month', 'day', 'hour']:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast('double'))

        df = df.filter(
            col('dep_delay').isNotNull() &
            col('arr_delay').isNotNull() &
            col('distance').isNotNull()
        )

        if 'carrier_name' not in df.columns and 'name' in df.columns:
            df = df.withColumn('carrier_name', col('name'))
        elif 'carrier_name' not in df.columns and 'carrier' in df.columns:
            df = df.withColumn('carrier_name', col('carrier'))
        else:
            df = df.withColumn('carrier_name', when(col('carrier_name').isNull(), col('carrier')).otherwise(col('carrier_name')))

        df = df.withColumn(
            'delay_category',
            when(col('dep_delay') <= 0, lit('On Time'))
            .when((col('dep_delay') > 0) & (col('dep_delay') <= 15), lit('Minor Delay'))
            .when((col('dep_delay') > 15) & (col('dep_delay') <= 45), lit('Moderate Delay'))
            .otherwise(lit('Severe Delay'))
        )

        projected_columns = [
            'flight_id', 'year', 'month', 'day', 'hour',
            'carrier', 'carrier_name', 'origin', 'origin_city',
            'dest', 'dest_city', 'dep_delay', 'arr_delay',
            'distance', 'delay_category'
        ]
        selected = [column for column in projected_columns if column in df.columns]
        transformed = df.select(*selected)
        logger.info(f'[OK] Flights CSV transformed ({transformed.count()} rows)')
        return transformed

    def transform_parquet_flights(self, flights_df):
        logger.info('Transforming flights Parquet dataset...')
        df = flights_df

        for column in ['dep_delay', 'arr_delay', 'distance', 'year', 'month', 'day', 'hour']:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast('double'))

        if 'carrier_name' not in df.columns and 'name' in df.columns:
            df = df.withColumn('carrier_name', col('name'))
        elif 'carrier_name' not in df.columns and 'carrier' in df.columns:
            df = df.withColumn('carrier_name', col('carrier'))
        else:
            df = df.withColumn('carrier_name', when(col('carrier_name').isNull(), col('carrier')).otherwise(col('carrier_name')))

        if 'delay_category' not in df.columns and 'dep_delay' in df.columns:
            df = df.withColumn(
                'delay_category',
                when(col('dep_delay') <= 0, lit('On Time'))
                .when((col('dep_delay') > 0) & (col('dep_delay') <= 15), lit('Minor Delay'))
                .when((col('dep_delay') > 15) & (col('dep_delay') <= 45), lit('Moderate Delay'))
                .otherwise(lit('Severe Delay'))
            )

        selected = [
            c for c in [
                'id', 'year', 'month', 'day', 'hour', 'carrier',
                'carrier_name', 'origin', 'dest', 'dep_delay',
                'arr_delay', 'distance', 'delay_category', 'time_hour',
                'name', 'tailnum', 'air_time', 'month_name', 'day_of_week'
            ] if c in df.columns
        ]

        transformed = df.select(*selected)
        logger.info(f'[OK] Flights Parquet transformed ({transformed.count()} rows)')
        return transformed

    def transform_crashes(self, crashes_df):
        logger.info('Transforming crash dataset...')
        df = crashes_df

        if 'CRASH_DATE' in df.columns:
            df = df.withColumn('CRASH_DATE', to_timestamp(col('CRASH_DATE'), 'MM/dd/yyyy hh:mm:ss a'))

        for column in ['INJURIES_FATAL', 'INJURIES_INCAPACITATING', 'INJURIES_TOTAL']:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast('double'))

        df = df.withColumn(
            'injury_severity',
            when(col('INJURIES_FATAL') > 0, lit('Fatal'))
            .when(col('INJURIES_INCAPACITATING') > 0, lit('Serious'))
            .when(col('INJURIES_TOTAL') > 0, lit('Minor'))
            .otherwise(lit('None'))
        )

        projected = [
            'RD_NO', 'CRASH_DATE', 'POSTED_SPEED_LIMIT', 'WEATHER_CONDITION',
            'LIGHTING_CONDITION', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE',
            'DAMAGE', 'PRIM_CONTRIBUTORY_CAUSE', 'INJURIES_TOTAL',
            'INJURIES_FATAL', 'INJURIES_INCAPACITATING', 'CRASH_HOUR',
            'CRASH_DAY_OF_WEEK', 'CRASH_MONTH', 'LATITUDE', 'LONGITUDE',
            'injury_severity'
        ]
        selected = [column for column in projected if column in df.columns]
        transformed = df.select(*selected)
        logger.info(f'[OK] Crash dataset transformed ({transformed.count()} rows)')
        return transformed

    def transform_us_cities(self, cities_df):
        logger.info('Transforming US cities dataset...')
        df = cities_df

        for column in ['population', 'density']:
            if column in df.columns:
                df = df.withColumn(column, col(column).cast('double'))

        selected = [column for column in ['city', 'state_id', 'state_name', 'population', 'density'] if column in df.columns]
        transformed = df.select(*selected).dropDuplicates()
        logger.info(f'[OK] US cities transformed ({transformed.count()} rows)')
        return transformed

    def transform_all(self, dataframes):
        logger.info('\n' + '=' * 60)
        logger.info('TRANSFORMATION PHASE')
        logger.info('=' * 60)

        transformed = {}

        if 'flights_csv' in dataframes:
            transformed['flights'] = self.transform_flights(dataframes['flights_csv'])

        if 'flights_parquet' in dataframes:
            transformed['flights_parquet'] = self.transform_parquet_flights(dataframes['flights_parquet'])
            if 'flights' not in transformed:
                transformed['flights'] = self.transform_parquet_flights(dataframes['flights_parquet'])

        if 'crashes' in dataframes:
            transformed['crashes'] = self.transform_crashes(dataframes['crashes'])

        if 'us_cities' in dataframes:
            transformed['us_cities'] = self.transform_us_cities(dataframes['us_cities'])

        logger.info(f'[OK] Transformed {len(transformed)} dataset(s)')
        return transformed

    def load_to_duckdb(self, dataframes):
        logger.info('\n' + '=' * 60)
        logger.info('LOADING PHASE')
        logger.info('=' * 60)

        conn = duckdb.connect(str(DB_PATH))
        logger.info(f'Connected to DuckDB at {DB_PATH}')

        try:
            for name, df in dataframes.items():
                # Repartition to 1 to avoid serialization issues
                pdf = df.repartition(1).toPandas()
                temp_name = f'tmp_{name}'
                conn.register(temp_name, pdf)
                conn.execute(f'CREATE OR REPLACE TABLE {name} AS SELECT * FROM {temp_name}')
                conn.unregister(temp_name)
                logger.info(f'[OK] Loaded {name}: {len(pdf)} rows')

            if 'flights' in dataframes:
                conn.execute('''
                    CREATE OR REPLACE TABLE carrier_stats AS
                    SELECT carrier_name, COUNT(*) AS total_flights,
                        ROUND(AVG(dep_delay), 2) AS avg_delay,
                        ROUND(AVG(distance), 2) AS avg_distance
                    FROM flights
                    GROUP BY carrier_name
                    ORDER BY total_flights DESC
                ''')
                conn.execute('''
                    CREATE OR REPLACE TABLE hourly_stats AS
                    SELECT hour, COUNT(*) AS flight_count,
                        ROUND(AVG(dep_delay), 2) AS avg_delay
                    FROM flights
                    GROUP BY hour
                    ORDER BY hour
                ''')
                conn.execute('''
                    CREATE OR REPLACE TABLE delay_summary AS
                    SELECT delay_category, COUNT(*) AS count
                    FROM flights
                    GROUP BY delay_category
                    ORDER BY count DESC
                ''')

            if 'crashes' in dataframes:
                conn.execute('''
                    CREATE OR REPLACE TABLE crash_weather_summary AS
                    SELECT WEATHER_CONDITION, COUNT(*) AS crash_count,
                        SUM(INJURIES_TOTAL) AS total_injuries
                    FROM crashes
                    GROUP BY WEATHER_CONDITION
                    ORDER BY crash_count DESC
                ''')

            if 'us_cities' in dataframes:
                conn.execute('''
                    CREATE OR REPLACE TABLE top_cities_by_population AS
                    SELECT city, state_name, population, density
                    FROM us_cities
                    ORDER BY population DESC
                    LIMIT 20
                ''')

            conn.close()
            logger.info('[SUCCESS] Data loaded to DuckDB')
            return str(DB_PATH)
        except Exception as error:
            conn.close()
            logger.error(f'[ERROR] Load failed: {error}')
            raise

    def validate_db(self):
        logger.info('Validating DuckDB output...')
        conn = duckdb.connect(str(DB_PATH))
        try:
            tables = conn.execute('SHOW TABLES').fetchall()
            logger.info(f'Found tables: {[table[0] for table in tables]}')
            for table in tables:
                count = conn.execute(f'SELECT COUNT(*) FROM {table[0]}').fetchone()[0]
                logger.info(f'Table {table[0]} contains {count} rows')
            conn.close()
            return True
        except Exception as error:
            conn.close()
            logger.error(f'Validation failed: {error}')
            return False

    def run(self):
        logger.info('\n' + '=' * 60)
        logger.info('RUNNING ETL PIPELINE')
        logger.info('=' * 60)

        extracted = self.extract_all_sources()
        transformed = self.transform_all(extracted)
        db_path = self.load_to_duckdb(transformed)

        logger.info('\n' + '=' * 60)
        logger.info('ETL PIPELINE COMPLETED')
        logger.info('=' * 60)
        logger.info(f'DuckDB file created at {db_path}')

        return db_path


def create_dashboard(db_path: str):
    logger.info('Creating Python Dash dashboard...')
    conn = duckdb.connect(db_path)

    carrier_data = conn.execute('SELECT * FROM carrier_stats LIMIT 10').fetchdf()
    hourly_data = conn.execute('SELECT * FROM hourly_stats').fetchdf()
    delay_data = conn.execute('SELECT * FROM delay_summary').fetchdf()
    crash_data = conn.execute('SELECT * FROM crash_weather_summary LIMIT 10').fetchdf()
    top_cities = conn.execute('SELECT * FROM top_cities_by_population').fetchdf()
    conn.close()

    # Enhanced color schemes
    carrier_fig = px.bar(
        carrier_data,
        x='carrier_name',
        y='total_flights',
        color='avg_delay',
        color_continuous_scale='Viridis',
        title='Flights by Airline',
        labels={'carrier_name': 'Airline', 'total_flights': 'Total Flights', 'avg_delay': 'Avg Delay (min)'}
    )
    carrier_fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    hourly_fig = px.line(
        hourly_data,
        x='hour',
        y='avg_delay',
        title='Average Delay by Hour',
        markers=True,
        color_discrete_sequence=['#FF6B6B'],
        labels={'hour': 'Hour', 'avg_delay': 'Avg Delay (minutes)'}
    )
    hourly_fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    delay_fig = px.pie(
        delay_data,
        names='delay_category',
        values='count',
        title='Delay Category Distribution',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    delay_fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    crash_fig = px.bar(
        crash_data,
        x='WEATHER_CONDITION',
        y='crash_count',
        title='Crash Events by Weather Condition',
        color='crash_count',
        color_continuous_scale='Reds',
        labels={'WEATHER_CONDITION': 'Weather', 'crash_count': 'Crash Count'}
    )
    crash_fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    city_fig = px.bar(
        top_cities,
        x='city',
        y='population',
        title='Top 20 US Cities by Population',
        color='population',
        color_continuous_scale='Blues',
        labels={'city': 'City', 'population': 'Population'}
    )
    city_fig.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    # Custom CSS for dark theme
    custom_css = """
    body {
        background-color: #1a1a1a;
        color: white;
    }
    .card {
        background-color: #2d2d2d;
        border: 1px solid #444;
        border-radius: 10px;
        box-shadow: 0 4px 8px rgba(0,0,0,0.3);
    }
    .card-header {
        background-color: #3d3d3d;
        border-bottom: 1px solid #444;
        font-weight: bold;
    }
    """

    app = Dash(__name__, external_stylesheets=[dbc.themes.DARKLY], external_scripts=[{'src': 'https://code.jquery.com/jquery-3.6.0.min.js'}])
    app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1('✈️ Flight Analytics Dashboard', className='text-center my-4', style={'color': '#00d4ff', 'text-shadow': '2px 2px 4px rgba(0,0,0,0.5)'}),
                html.P('Comprehensive insights into flight delays, crashes, and demographics', className='text-center mb-4', style={'color': '#ccc'})
            ], width=12)
        ]),
        dbc.Row([
            dbc.Col(dbc.Card([
                dbc.CardHeader('📊 Data Summary'),
                dbc.CardBody([
                    html.P('✅ Flights data loaded from CSV and Parquet sources', className='mb-2'),
                    html.P('✅ Crash data processed with weather correlations', className='mb-2'),
                    html.P('✅ US cities demographics integrated', className='mb-2'),
                    html.P('✅ Real-time analytics powered by DuckDB', className='mb-0')
                ])
            ]), width=12, className='mb-4')
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=carrier_fig), width=12, lg=6, className='mb-4'),
            dbc.Col(dcc.Graph(figure=hourly_fig), width=12, lg=6, className='mb-4')
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=delay_fig), width=12, lg=6, className='mb-4'),
            dbc.Col(dcc.Graph(figure=crash_fig), width=12, lg=6, className='mb-4')
        ]),
        dbc.Row([
            dbc.Col(dcc.Graph(figure=city_fig), width=12, className='mb-4')
        ]),
        dbc.Row([
            dbc.Col([
                html.Hr(style={'border-color': '#444'}),
                html.P(f'🔗 Data sourced from DuckDB: {db_path}', className='text-muted text-center'),
                html.P('Built with ❤️ using PySpark, Dash & Plotly', className='text-muted text-center small')
            ], width=12)
        ])
    ], fluid=True, style={'backgroundColor': '#1a1a1a', 'minHeight': '100vh'})

    logger.info('[OK] Dashboard assembled with professional styling')
    return app


def run_dashboard(app, port=8050, debug=False):
    run_method = getattr(app, 'run', None)
    if callable(run_method):
        logger.info('Starting Dash app with app.run()')
        run_method(debug=debug, port=port)
        return

    run_server_method = None
    try:
        run_server_method = object.__getattribute__(app, 'run_server')
    except Exception:
        run_server_method = None

    if callable(run_server_method):
        logger.info('Starting Dash app with app.run_server()')
        run_server_method(debug=debug, port=port)
        return

    raise RuntimeError('Dash app object has no run or run_server method.')


if __name__ == '__main__':
    pipeline = ETLPipeline()
    database_path = pipeline.run()
    if pipeline.validate_db():
        logger.info('Launching dashboard at http://127.0.0.1:8050')
        dashboard = create_dashboard(database_path)
        run_dashboard(dashboard, debug=False, port=8050)
    else:
        logger.error('Pipeline completed but validation failed.')
