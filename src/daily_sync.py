"""
Daily Sync - Orchestrates booking snapshots and weather forecasts
Run this daily to sync all forecast data
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from src.booking_sync import generate_booking_snapshot
from src.weather_sync import fetch_weather_forecast

def daily_sync():
    """
    Execute daily data synchronization
    - Booking snapshots (60 days)
    - Weather forecasts (16 days)
    """
    
    today = datetime.now().date()
    print(f"🚀 Starting daily sync for {today}")
    print("=" * 50)
    
    success_count = 0
    
    # 1. Booking Snapshots
    print("\n📊 BOOKING SNAPSHOTS")
    print("-" * 30)
    try:
        if generate_booking_snapshot(snapshot_date=today, forecast_days=60):
            print("✅ Booking snapshots completed successfully")
            success_count += 1
        else:
            print("❌ Booking snapshots failed")
    except Exception as e:
        print(f"❌ Booking snapshots error: {e}")
    
    # 2. Weather Forecasts
    print("\n🌦️  WEATHER FORECASTS")
    print("-" * 30)
    try:
        if fetch_weather_forecast(forecast_date=today):
            print("✅ Weather forecasts completed successfully")
            success_count += 1
        else:
            print("❌ Weather forecasts failed")
    except Exception as e:
        print(f"❌ Weather forecasts error: {e}")
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📋 DAILY SYNC SUMMARY")
    print(f"   Date: {today}")
    print(f"   Successful syncs: {success_count}/2")
    
    if success_count == 2:
        print("🎉 All syncs completed successfully!")
        return True
    else:
        print("⚠️  Some syncs failed - check logs above")
        return False

if __name__ == "__main__":
    daily_sync()