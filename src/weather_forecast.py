"""
Weather Sync - Fetches weather forecasts from Open Meteo API
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.database import save_weather_forecast, save_weather_forecast_batch
from config.logging_config import setup_logging

# Load environment variables
load_dotenv()

logger = setup_logging("weather-forecast")

def fetch_weather_forecast(forecast_date=None):
    """Fetch weather forecast from Open Meteo API"""
    
    if not forecast_date:
        forecast_date = datetime.now().date()
    
    latitude = float(os.getenv('WEATHER_LATITUDE'))
    longitude = float(os.getenv('WEATHER_LONGITUDE'))
    forecast_days = int(os.getenv('WEATHER_FORECAST_DAYS', '16'))
    
    logger.info(f"🌦️ Fetching {forecast_days}-day weather forecast for {forecast_date}")
    
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'daily': [
            'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum',
            'precipitation_probability_mean', 'sunshine_duration',
            'wind_speed_10m_max', 'cloud_cover_mean', 'weathercode',
            'apparent_temperature_max', 'apparent_temperature_min'
        ],
        'forecast_days': forecast_days,
        'timezone': 'Europe/Berlin'
    }
    
    try:
        response = requests.get(
            'https://api.open-meteo.com/v1/forecast',
            params=params,
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        
        daily = data['daily']
        dates = daily['time']
        
        logger.info(f"✅ Received {len(dates)} days from Open Meteo API")
        
        # Daten vorbereiten für Batch-Insert
        forecast_list = []
        for i, date_str in enumerate(dates):
            target_date = datetime.fromisoformat(date_str).date()
            days_ahead = (target_date - forecast_date).days + 1
            
            weather_data = {
                'forecast_created_at': forecast_date,
                'forecast_date': target_date,
                'days_ahead': days_ahead,
                'temperature_2m_max': daily['temperature_2m_max'][i],
                'temperature_2m_min': daily['temperature_2m_min'][i],
                'precipitation_sum': daily['precipitation_sum'][i],
                'precipitation_probability_mean': daily['precipitation_probability_mean'][i],
                'sunshine_duration': daily['sunshine_duration'][i],
                'wind_speed_10m_max': daily['wind_speed_10m_max'][i],
                'cloud_cover_mean': daily['cloud_cover_mean'][i],
                'weathercode': daily['weathercode'][i],
                'apparent_temperature_max': daily['apparent_temperature_max'][i],
                'apparent_temperature_min': daily['apparent_temperature_min'][i]
            }
            forecast_list.append(weather_data)
            
        # Batch Save
        logger.info(f"💾 Saving {len(forecast_list)} forecasts to database...")
        if save_weather_forecast_batch(forecast_list):
            logger.info(f"✅ Weather sync complete! Saved {len(forecast_list)} records.")
            return True
        else:
            logger.error("❌ Failed to save weather forecasts batch.")
            return False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Weather API request failed: {e}")
        return False

def sync_weather():
    """Wrapper for daily_sync.py compatibility"""
    logger.info("🌤️ Starting weather forecast sync")
    try:
        success = fetch_weather_forecast()
        if not success:
            return {"status": "error", "message": "Weather forecast failed"}
        return {
            "status": "success",
            "message": "Weather sync completed successfully",
            "forecasts": int(os.getenv('WEATHER_FORECAST_DAYS', '16'))
        }
    except Exception as e:
        logger.error(f"Weather sync exception: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    fetch_weather_forecast()


def sync_weather():
    """
    Wrapper function for daily_sync.py compatibility
    Fetches weather forecast and saves to database
    """
    print("🌤️ Starting weather forecast sync")
    
    try:
        # fetch_weather_forecast returns boolean success, not data!
        success = fetch_weather_forecast()
        
        if not success:
            return {"status": "error", "message": "Weather forecast failed"}
        
        return {
            "status": "success",
            "message": "Weather sync completed successfully",
            "forecasts": int(os.getenv('WEATHER_FORECAST_DAYS', '16'))
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Weather sync failed: {str(e)}"
        }

