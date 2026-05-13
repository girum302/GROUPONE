# ============================================
# DATA SOURCE GENERATOR
# Creates sample flight, weather, and revenue data in CSV, Parquet, and JSON formats
# ============================================

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import random
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / 'data' / 'raw'
RAW_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f'Raw data directory: {RAW_DIR}')

AIRPORTS = {
    'JFK': {'city': 'New York', 'lat': 40.6413, 'lon': -73.7781},
    'LAX': {'city': 'Los Angeles', 'lat': 33.9416, 'lon': -118.4085},
    'ORD': {'city': 'Chicago', 'lat': 41.9742, 'lon': -87.9073},
    'DFW': {'city': 'Dallas', 'lat': 32.8998, 'lon': -97.0403},
    'DEN': {'city': 'Denver', 'lat': 39.8561, 'lon': -104.6737},
    'SFO': {'city': 'San Francisco', 'lat': 37.6213, 'lon': -122.3790},
    'ATL': {'city': 'Atlanta', 'lat': 33.6407, 'lon': -84.4277},
    'BOS': {'city': 'Boston', 'lat': 42.3656, 'lon': -71.0096}
}

AIRLINES = {
    'AA': 'American Airlines',
    'DL': 'Delta Air Lines',
    'UA': 'United Airlines',
    'WN': 'Southwest Airlines',
    'B6': 'JetBlue',
    'NK': 'Spirit Airlines',
    'AS': 'Alaska Airlines'
}


def generate_flight_data(num_flights=20000) -> pd.DataFrame:
    """Generate synthetic flight records."""
    logger.info('Generating flight data...')
    records = []
    start_date = datetime(2024, 1, 1)

    for i in range(num_flights):
        flight_date = start_date + timedelta(hours=i)
        origin = random.choice(list(AIRPORTS.keys()))
        dest = random.choice([airport for airport in AIRPORTS.keys() if airport != origin])
        carrier = random.choice(list(AIRLINES.keys()))
        delay = max(0, random.gauss(10, 18))

        records.append({
            'flight_id': f'FL{i:06d}',
            'date': flight_date.date(),
            'time': flight_date.time().strftime('%H:%M:%S'),
            'carrier': carrier,
            'carrier_name': AIRLINES[carrier],
            'origin': origin,
            'origin_city': AIRPORTS[origin]['city'],
            'dest': dest,
            'dest_city': AIRPORTS[dest]['city'],
            'dep_delay': round(delay, 2),
            'arr_delay': round(delay * 0.85, 2),
            'distance': random.randint(200, 2600),
            'passengers': random.randint(50, 180)
        })

    df = pd.DataFrame(records)
    logger.info(f'Generated {len(df)} flight records')
    return df


def generate_weather_data() -> pd.DataFrame:
    """Generate synthetic airport weather records."""
    logger.info('Generating weather data...')
    records = []

    for airport, meta in AIRPORTS.items():
        records.append({
            'airport': airport,
            'city': meta['city'],
            'temperature': round(random.uniform(30, 90), 2),
            'humidity': random.randint(30, 90),
            'wind_speed': round(random.uniform(0, 25), 2),
            'conditions': random.choice(['Clear', 'Cloudy', 'Rainy', 'Stormy']),
            'visibility': round(random.uniform(0.5, 10), 2)
        })

    df = pd.DataFrame(records)
    logger.info(f'Generated {len(df)} weather records')
    return df


def generate_revenue_data(num_records=5000) -> pd.DataFrame:
    """Generate synthetic booking and revenue records."""
    logger.info('Generating revenue data...')
    records = []
    start_date = datetime(2024, 1, 1)

    for i in range(num_records):
        booking_date = start_date + timedelta(days=random.randint(0, 365))
        carrier = random.choice(list(AIRLINES.keys()))
        origin = random.choice(list(AIRPORTS.keys()))
        dest = random.choice([airport for airport in AIRPORTS.keys() if airport != origin])

        records.append({
            'booking_id': f'BK{i:07d}',
            'date': booking_date.date(),
            'carrier': carrier,
            'carrier_name': AIRLINES[carrier],
            'revenue': round(random.uniform(100, 1000), 2),
            'passengers': random.randint(1, 4),
            'booking_class': random.choice(['Economy', 'Business', 'First']),
            'origin': origin,
            'dest': dest,
            'booking_status': random.choice(['Confirmed', 'Cancelled', 'Completed'])
        })

    df = pd.DataFrame(records)
    logger.info(f'Generated {len(df)} revenue records')
    return df


def save_dataframe(df: pd.DataFrame, filename: str, mode: str = 'csv') -> Path:
    """Save a dataframe as CSV, Parquet, or JSON Lines."""
    target_path = RAW_DIR / filename

    if mode == 'csv':
        df.to_csv(target_path, index=False)
    elif mode == 'parquet':
        df.to_parquet(target_path, index=False, compression='snappy')
    elif mode == 'json':
        df.to_json(target_path, orient='records', lines=True)
    else:
        raise ValueError(f'Unsupported save mode: {mode}')

    logger.info(f'Saved {filename} ({len(df)} rows, mode={mode})')
    return target_path


def generate_all_sources():
    """Generate all raw source files for the ETL pipeline."""
    logger.info('Starting raw source generation')

    flights_df = generate_flight_data(20000)
    save_dataframe(flights_df, 'flights.csv', mode='csv')
    save_dataframe(flights_df, 'flights.parquet', mode='parquet')
    save_dataframe(flights_df.head(1000), 'flights_sample.jsonl', mode='json')

    weather_df = generate_weather_data()
    save_dataframe(weather_df, 'weather.csv', mode='csv')
    save_dataframe(weather_df, 'weather.parquet', mode='parquet')

    revenue_df = generate_revenue_data(5000)
    save_dataframe(revenue_df, 'revenue.csv', mode='csv')
    save_dataframe(revenue_df, 'revenue.parquet', mode='parquet')

    logger.info('Completed raw source generation')


if __name__ == '__main__':
    generate_all_sources()
