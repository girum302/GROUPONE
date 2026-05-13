# ============================================
# UNIT TESTS FOR ETL PIPELINE
# ============================================

import pytest
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession

# Add project to path
PROJECT_PATH = r"C:\Users\hp\Desktop\big data group 1"
sys.path.insert(0, PROJECT_PATH)

from etl_pipeline import ETLPipeline
from etl_data_generator import generate_flight_data, generate_weather_data

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for tests"""
    spark_session = SparkSession.builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

class TestDataGeneration:
    """Test data generation functions"""
    
    def test_flight_data_generation(self):
        """Test flight data generation"""
        df = generate_flight_data(100)
        assert len(df) == 100
        assert 'flight_id' in df.columns
        assert 'dep_delay' in df.columns
        assert df['dep_delay'].notna().all()
    
    def test_weather_data_generation(self):
        """Test weather data generation"""
        df = generate_weather_data()
        assert len(df) == 8  # 8 airports
        assert 'airport' in df.columns
        assert 'temperature' in df.columns
        assert df['airport'].is_unique

class TestETLPipeline:
    """Test ETL pipeline"""
    
    def test_pipeline_initialization(self, spark):
        """Test pipeline initialization"""
        pipeline = ETLPipeline()
        assert pipeline.spark is not None
    
    def test_csv_extraction(self, spark):
        """Test CSV extraction"""
        # Generate test data
        df = generate_flight_data(50)
        csv_path = os.path.join(PROJECT_PATH, "data", "test.csv")
        df.to_csv(csv_path, index=False)
        
        # Extract
        pipeline = ETLPipeline()
        extracted = pipeline.extract_csv(csv_path)
        
        assert extracted.count() == 50
        
        # Cleanup
        os.remove(csv_path)

class TestDataQuality:
    """Test data quality checks"""
    
    def test_no_null_delays(self):
        """Test that delays are not null"""
        df = generate_flight_data(100)
        assert df['dep_delay'].notna().all()
        assert df['arr_delay'].notna().all()
    
    def test_distance_positive(self):
        """Test that distance is positive"""
        df = generate_flight_data(100)
        assert (df['distance'] > 0).all()
    
    def test_airport_codes_valid(self):
        """Test that airport codes are valid"""
        df = generate_flight_data(100)
        valid_airports = {
            'JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'SFO', 'ATL', 'BOS'
        }
        assert df['origin'].isin(valid_airports).all()
        assert df['dest'].isin(valid_airports).all()
    
    def test_different_origin_dest(self):
        """Test that origin and destination are different"""
        df = generate_flight_data(100)
        assert (df['origin'] != df['dest']).all()

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
