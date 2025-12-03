"""
Daily synchronization orchestrator for booking analytics
Coordinates booking sync, weather sync, snapshots AND historical weather updates
"""
import sys
import os
from datetime import datetime, timedelta

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import setup_logging, log_sync_start, log_sync_end, log_error

from src.booking_sync import sync_bookings, sync_booking_snapshots
from src.weather_forecast import sync_weather  
from src.weather_pipeline import import_weather_range  
from src.database import get_db_connection
from config.settings import validate_config

logger = setup_logging("daily-sync")

def get_comprehensive_stats():
    """Get statistics from all 3 tables + weather_daily"""
    conn = get_db_connection()
    if not conn:
        return {"error": "No DB Connection"}
    
    try:
        with conn.cursor() as cur:
            # 1. Main bookings table stats
            cur.execute("SELECT COUNT(*) FROM bookings")
            total_bookings = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM bookings 
                WHERE DATE(booking_date) = CURRENT_DATE
            """)
            today_bookings = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM bookings 
                WHERE booking_date > NOW() AND booking_date <= NOW() + INTERVAL '60 days'
            """)
            future_bookings = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM bookings 
                WHERE updated_at >= NOW() - INTERVAL '24 hours'
            """)
            recent_updates = cur.fetchone()[0]
            
            # 2. Booking snapshots table stats  
            cur.execute("SELECT COUNT(*) FROM booking_snapshots")
            total_snapshots = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(DISTINCT snapshot_created_at) FROM booking_snapshots
            """)
            snapshot_days = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM booking_snapshots 
                WHERE snapshot_created_at >= CURRENT_DATE
            """)
            todays_snapshots = cur.fetchone()[0]
            
            # 3. Weather forecasts table stats
            cur.execute("SELECT COUNT(*) FROM weather_forecasts")
            total_weather = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM weather_forecasts 
                WHERE forecast_created_at >= CURRENT_DATE
            """)
            todays_weather = cur.fetchone()[0]
            
            # 4. NEW: Historical weather daily stats
            cur.execute("SELECT COUNT(*) FROM weather_daily")
            total_weather_daily = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) FROM weather_daily 
                WHERE updated_at >= CURRENT_DATE
            """)
            todays_weather_daily = cur.fetchone()[0]
            
            return {
                # Bookings stats
                "total_bookings": total_bookings,
                "today_bookings": today_bookings,
                "future_bookings": future_bookings,
                "recent_updates": recent_updates,
                # Snapshots stats (BI)
                "total_snapshots": total_snapshots,
                "snapshot_days": snapshot_days,
                "todays_snapshots": todays_snapshots,
                # Weather forecasts stats
                "total_weather": total_weather,
                "todays_weather": todays_weather,
                # NEW: Historical weather stats
                "total_weather_daily": total_weather_daily,
                "todays_weather_daily": todays_weather_daily
            }
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return {"error": str(e)}
    finally:
        conn.close()

def sync_yesterday_weather():
    """Sync yesterday's weather data (historical data becomes available with 1-day delay)"""
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info(f"PHASE 4: Historical Weather Update for {yesterday}")
    return import_weather_range(yesterday, yesterday)

def main():
    """Main daily sync orchestrator"""
    validate_config()
    log_sync_start(logger, "4-Phase Daily Sync")
    
    try:
        # 1. Bookings
        logger.info("📚 PHASE 1: Main Bookings Table Sync")
        # Zeitzonen-Handling geschieht in booking_sync intern, hier reichen Strings
        start = (datetime.now() - timedelta(days=3)).isoformat()
        end = (datetime.now() + timedelta(days=60)).isoformat()
        sync_bookings(start, end)
        
        # 2. Weather Forecast
        logger.info("🌤️ PHASE 2: Weather Forecasts")
        sync_weather()
        
        # 3. Snapshots
        logger.info("📸 PHASE 3: Booking Snapshots")
        snapshot_end = (datetime.now() + timedelta(days=60)).isoformat()
        sync_booking_snapshots(snapshot_end)
        
        # 4. Historical Weather
        logger.info("🌡️ PHASE 4: Historical Weather Update")
        sync_yesterday_weather()
        
        # Stats
        final_stats = get_comprehensive_stats()
        log_sync_end(logger, "4-Phase Daily Sync", final_stats)
        
    except Exception as e:
        logger.error(f"FATAL ERROR in daily sync: {e}")
        raise

if __name__ == "__main__":
    main()

def health_check():
    """
    Quick health check for monitoring systems
    Returns True if everything is working
    """
    try:
        logger = setup_logging("health-check")
        
        # Test configuration
        validate_config()
        
        # Test database
        stats = get_sync_stats()
        if 'error' in stats:
            return False
        
        # Test basic functionality
        if stats.get('total_bookings', 0) == 0:
            logger.warning("No bookings found in database")
            return False
        
        logger.info("Health check passed")
        return True
        
    except Exception as e:
        if 'logger' in locals():
            log_error(logger, "Health check", e)
        return False

def manual_sync(start_date=None, end_date=None, include_weather=True):
    """
    Manual sync function for testing or one-off runs
    
    Args:
        start_date: Custom start date (ISO string)
        end_date: Custom end date (ISO string)
        include_weather: Include weather sync (default True)
    """
    logger = setup_logging("manual-sync")
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=7)).isoformat()
    if not end_date:
        end_date = (datetime.now() + timedelta(days=30)).isoformat()
    
    logger.info(f"Manual sync started: {start_date} to {end_date}")
    
    try:
        # Quick booking sync
        booking_result = sync_bookings(start_date, end_date)
        logger.info(f"Bookings: {booking_result}")
        
        if include_weather:
            # Weather forecast sync
            weather_result = sync_weather()
            logger.info(f"Weather forecasts: {weather_result}")
            
            # Yesterday's historical weather
            yesterday_result = sync_yesterday_weather()
            logger.info(f"Historical weather: {yesterday_result}")
        
        logger.info("Manual sync completed successfully")
        return True
        
    except Exception as e:
        log_error(logger, "Manual sync", e)
        return False

if __name__ == "__main__":
    # Support command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "health":
            success = health_check()
            exit(0 if success else 1)
        elif command == "manual":
            success = manual_sync()
            exit(0 if success else 1)
        elif command == "manual-no-weather":
            success = manual_sync(include_weather=False)
            exit(0 if success else 1)
        elif command == "test":
            # Test mode: shorter date range
            start = (datetime.now() - timedelta(days=1)).isoformat()
            end = (datetime.now() + timedelta(days=7)).isoformat()
            success = manual_sync(start, end)
            exit(0 if success else 1)
    
    # Default: full daily sync
    main()