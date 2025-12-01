"""
Weather Data Pipeline: API → JSON → Database
Fetches from OpenMeteo, saves JSON backup, stores in PostgreSQL
"""
import requests
import json
import os
import psycopg2
import sys
from datetime import datetime, timedelta
import logging
from dotenv import load_dotenv

load_dotenv()
# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DB_CONFIG

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Create database connection using config.settings"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        return None

def fetch_openmeteo_historical(start_date, end_date):
    """Fetch historical weather data from OpenMeteo API"""
    
    # Weather config DIREKT aus .env
    latitude = float(os.getenv('WEATHER_LATITUDE'))
    longitude = float(os.getenv('WEATHER_LONGITUDE'))

    params = {
        'latitude': latitude,  # Kiel
        'longitude': longitude,
        'start_date': start_date,
        'end_date': end_date,
        'daily': ','.join([
            'temperature_2m_max',
            'temperature_2m_min',
            'precipitation_sum',
            'precipitation_hours',
            'weathercode',
            'sunshine_duration',
            'windspeed_10m_max',
            'pressure_msl_mean',
            'cloudcover_mean',
            'relative_humidity_2m_mean'
        ]),
        'timezone': 'Europe/Berlin'
    }
    
    try:
        logger.info(f"Fetching weather: {start_date} to {end_date}")
        
        response = requests.get(
            url="https://archive-api.open-meteo.com/v1/era5",
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"Fetched {len(data['daily']['time'])} days from API")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API Request failed: {e}")
        return None

def save_json_backup(data, filename):
    """Save weather data as JSON backup"""
    os.makedirs('data', exist_ok=True)
    filepath = os.path.join('data', filename)
    
    backup_data = {
        'fetched_at': datetime.now().isoformat(),
        'api_data': data
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"JSON backup saved: {filepath}")
    return filepath

def save_weather_to_database(weather_data):
    """Save single day weather data to PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO weather_daily (
                    date, location, temp_max, temp_min, temp_mean,
                    precipitation_sum, precipitation_hours, humidity,
                    windspeed_max, pressure_msl, sunshine_duration,
                    cloudcover_mean, visibility, weathercode,
                    data_source, is_forecast, forecast_created_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) ON CONFLICT (date) DO UPDATE SET
                    temp_max = EXCLUDED.temp_max,
                    temp_min = EXCLUDED.temp_min,
                    precipitation_sum = EXCLUDED.precipitation_sum,
                    precipitation_hours = EXCLUDED.precipitation_hours,
                    humidity = EXCLUDED.humidity,
                    windspeed_max = EXCLUDED.windspeed_max,
                    pressure_msl = EXCLUDED.pressure_msl,
                    sunshine_duration = EXCLUDED.sunshine_duration,
                    cloudcover_mean = EXCLUDED.cloudcover_mean,
                    weathercode = EXCLUDED.weathercode,
                    updated_at = NOW()
            """, (
                weather_data['date'],
                'Kiel',
                weather_data.get('temp_max'),
                weather_data.get('temp_min'),
                weather_data.get('temp_mean'),
                weather_data.get('precipitation_sum', 0),
                weather_data.get('precipitation_hours', 0),
                weather_data.get('humidity', 70),
                weather_data.get('windspeed_max', 0),
                weather_data.get('pressure_msl', 1013),
                weather_data.get('sunshine_duration', 0),
                weather_data.get('cloudcover_mean', 50),
                15000,  # visibility default
                weather_data.get('weathercode', 1),
                'openmeteo',
                False,  # is_forecast
                None    # forecast_created_at
            ))
            
        conn.commit()
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database save failed for {weather_data.get('date')}: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def process_weather_data(api_data):
    """Process API JSON data and save to database"""
    if not api_data or 'daily' not in api_data:
        logger.error("Invalid API data")
        return 0
    
    daily = api_data['daily']
    dates = daily['time']
    
    successful_saves = 0
    
    for i, date_str in enumerate(dates):
        weather_entry = {
            'date': datetime.strptime(date_str, '%Y-%m-%d').date(),
            'temp_max': daily.get('temperature_2m_max', [None] * len(dates))[i],
            'temp_min': daily.get('temperature_2m_min', [None] * len(dates))[i],
            'temp_mean': None,  # Calculate if needed
            'precipitation_sum': daily.get('precipitation_sum', [0] * len(dates))[i],
            'precipitation_hours': daily.get('precipitation_hours', [0] * len(dates))[i],
            'weathercode': daily.get('weathercode', [1] * len(dates))[i],
            'sunshine_duration': daily.get('sunshine_duration', [0] * len(dates))[i] / 3600 if daily.get('sunshine_duration', [0] * len(dates))[i] else 0,
            'windspeed_max': daily.get('windspeed_10m_max', [0] * len(dates))[i],
            'pressure_msl': daily.get('pressure_msl_mean', [1013] * len(dates))[i],
            'cloudcover_mean': daily.get('cloudcover_mean', [50] * len(dates))[i],
            'humidity': daily.get('relative_humidity_2m_mean', [70] * len(dates))[i]
        }
        
        if save_weather_to_database(weather_entry):
            successful_saves += 1
        
        # Progress every 10 days
        if (i + 1) % 10 == 0:
            logger.info(f"Processed {i + 1}/{len(dates)} days...")
    
    return successful_saves

def import_weather_range(start_date, end_date):
    """
    Complete weather import pipeline: API → JSON → Database
    
    Args:
        start_date (str): "YYYY-MM-DD"
        end_date (str): "YYYY-MM-DD"
    """
    logger.info(f"Starting weather import pipeline: {start_date} to {end_date}")
    
    # Step 1: Fetch from API
    api_data = fetch_openmeteo_historical(start_date, end_date)
    if not api_data:
        logger.error("API fetch failed, stopping")
        return {"status": "error", "message": "API fetch failed"}
    
    # Step 2: Save JSON backup
    filename = f"weather_{start_date}_{end_date}.json"
    json_path = save_json_backup(api_data, filename)
    
    # Step 3: Process and save to database
    successful_saves = process_weather_data(api_data)
    total_days = len(api_data['daily']['time'])
    
    result = {
        "status": "success" if successful_saves > 0 else "error",
        "message": f"Imported {successful_saves}/{total_days} days",
        "json_backup": json_path,
        "successful_saves": successful_saves,
        "total_days": total_days
    }
    
    logger.info(f"Import completed: {result['message']}")
    return result

def import_monthly_batches(year, months=None):
    """Import weather data month by month for a year"""
    if months is None:
        months = range(1, 13)  # All months
    
    total_success = 0
    
    for month in months:
        start_date = f"{year}-{month:02d}-01"
        
        # Calculate end of month
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        end_date = (next_month - timedelta(days=1)).strftime('%Y-%m-%d')
        
        logger.info(f"Importing month {year}-{month:02d}")
        result = import_weather_range(start_date, end_date)
        
        if result['status'] == 'success':
            total_success += result['successful_saves']
        
        # Small delay between months
        import time
        time.sleep(1)
    
    logger.info(f"Year {year} import completed. Total saves: {total_success}")
    return total_success

def test_database_connection():
    """Test database connection and show current weather data count"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM weather_daily;")
            count = cur.fetchone()[0]
            logger.info(f"Database connected! Current weather records: {count}")
            
            if count > 0:
                cur.execute("SELECT MIN(date), MAX(date) FROM weather_daily;")
                min_date, max_date = cur.fetchone()
                logger.info(f"Date range: {min_date} to {max_date}")
        
        return True
    except psycopg2.Error as e:
        logger.error(f"Database test failed: {e}")
        return False
    finally:
        conn.close()

def main():
    """Main function for testing weather import pipeline"""
    logger.info("Weather Import Pipeline Test")
    
    # Test 1: Database connection
    if not test_database_connection():
        logger.error("Database connection failed")
        return
    
    # Test 2: Import recent month (November 2024)
    logger.info("Testing import for November 2024...")
    result = import_weather_range("2024-11-01", "2024-11-30")
    
    if result['status'] == 'success':
        logger.info("Pipeline test successful!")
        logger.info(f"JSON backup: {result['json_backup']}")
        logger.info(f"Database saves: {result['successful_saves']}")
        
        # Test database data
        test_database_connection()
    else:
        logger.error("Pipeline test failed!")

if __name__ == "__main__":
    main()