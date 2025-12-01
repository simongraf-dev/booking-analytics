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
from src.database import save_weather_forecast

# Load environment variables
load_dotenv()

def fetch_weather_forecast(forecast_date=None):
    """Fetch weather forecast from Open Meteo API"""
    
    if not forecast_date:
        forecast_date = datetime.now().date()
    
    # Weather config DIREKT aus .env
    latitude = float(os.getenv('WEATHER_LATITUDE'))
    longitude = float(os.getenv('WEATHER_LONGITUDE'))
    forecast_days = int(os.getenv('WEATHER_FORECAST_DAYS', '16'))
    
    print(f"🌦️  Fetching {forecast_days}-day weather forecast for {forecast_date}")
    print(f"📍 Location: {latitude}, {longitude}")
    
    # Open Meteo API Parameters
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min', 
            'precipitation_sum',
            'precipitation_probability_mean',
            'sunshine_duration',
            'wind_speed_10m_max',
            'cloud_cover_mean',
            'weathercode',
            'apparent_temperature_max',
            'apparent_temperature_min'
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
        
        print(f"✅ Received weather data from Open Meteo")
        
        # Parse weather data
        daily = data['daily']
        dates = daily['time']
        
        successful_saves = 0
        failed_saves = 0
        
        print(f"💾 Saving {len(dates)} weather forecasts to database...")
        
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
            
            if save_weather_forecast(weather_data):
                successful_saves += 1
            else:
                failed_saves += 1
                print(f"❌ Failed to save weather for {target_date}")
        
        print(f"✅ Weather sync complete!")
        print(f"   💾 Successfully saved: {successful_saves} forecasts")
        print(f"   ❌ Failed saves: {failed_saves}")
        
        return successful_saves > 0
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Weather API request failed: {e}")
        return False

def main():
    """Test weather sync"""
    print("🧪 Testing weather forecast sync...")
    success = fetch_weather_forecast()
    
    if success:
        print("🎉 Weather sync test successful!")
    else:
        print("❌ Weather sync test failed")


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

if __name__ == "__main__":
    # Test both functions
    print("🧪 Testing weather forecast sync...")
    success = fetch_weather_forecast()
    
    if success:
        print("🎉 Direct test successful!")
    
    print("\n🧪 Testing sync_weather wrapper...")
    result = sync_weather()
    print(f"📊 Wrapper result: {result}")