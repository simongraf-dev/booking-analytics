"""
Daily synchronization orchestrator for booking analytics
Coordinates booking sync, weather sync, snapshots AND historical weather updates
"""
import sys
import os
from datetime import datetime, timedelta
import time

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import setup_logging, log_sync_start, log_sync_end, log_error, log_success
from src.booking_sync import sync_bookings, sync_booking_snapshots
from src.weather_forecast import sync_weather  
from src.weather_pipeline import import_weather_range  
from src.database import get_db_connection
from config.settings import validate_config

def get_comprehensive_stats():
    """Get statistics from all 3 tables + weather_daily"""
    conn = get_db_connection()
    if not conn:
        return {}
    
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
        return {"error": str(e)}
    finally:
        conn.close()

def sync_yesterday_weather():
    """
    Sync yesterday's weather data (historical data becomes available with 1-day delay)
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info("=" * 50)
    logger.info("PHASE 4: Historical Weather Update (Yesterday)")
    logger.info("Purpose: Backfill yesterday's actual weather data")
    logger.info("=" * 50)
    
    try:
        logger.info(f"Fetching historical weather for {yesterday}")
        
        result = import_weather_range(yesterday, yesterday)
        
        if result['status'] == 'success':
            logger.info(f"Historical weather update completed: {result['message']}")
            return result
        else:
            logger.warning(f"Historical weather update failed: {result['message']}")
            return result
            
    except Exception as e:
        log_error(logger, "Historical weather update", e)
        return {"status": "error", "message": f"Historical weather sync failed: {str(e)}"}

def main():
    """Main daily sync orchestrator for all 4 phases"""
    
    # Initialize logging
    logger = setup_logging("daily-sync")
    
    try:
        # Validate configuration
        validate_config()
        logger.info("✅ Configuration validated")
        
        # Get pre-sync stats
        pre_stats = get_comprehensive_stats()
        log_sync_start(logger, "4-Phase Daily Sync", f"Pre-sync: {pre_stats}")
        
        # PHASE 1: Main Bookings Table (Current state)
        logger.info("=" * 50)
        logger.info("📚 PHASE 1: Main Bookings Table Sync")
        logger.info("🎯 Purpose: Current state of all reservations")
        logger.info("=" * 50)
        
        try:
            # Sync 3 days back (updates) + 60 days forward (forecasts)
            start_date = datetime.now() - timedelta(days=3)
            end_date = datetime.now() + timedelta(days=60)
            
            logger.info(f"📅 Range: {start_date.date()} to {end_date.date()}")
            logger.info(f"🔄 3 days back (updates) + 60 days forward (forecasts)")
            
            booking_result = sync_bookings(
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )
            
            logger.info(f"✅ Main bookings sync completed: {booking_result}")
            
        except Exception as e:
            log_error(logger, "Main bookings sync", e)
            raise
        
        # PHASE 2: Weather Forecasts (16-day ahead) 
        logger.info("=" * 50)
        logger.info("🌤️ PHASE 2: Weather Forecasts")
        logger.info("🎯 Purpose: 16-day weather forecasts for planning")
        logger.info("=" * 50)
        
        try:
            logger.info(f"🌦️ Fetching 16-day weather forecast")
            logger.info(f"📊 For booking-weather correlation analysis")
            
            weather_result = sync_weather()
            logger.info(f"✅ Weather forecast sync completed: {weather_result}")
            
        except Exception as e:
            log_error(logger, "Weather forecast sync", e)
            logger.warning("🔄 Weather failure doesn't affect booking data")
        
        # PHASE 3: Booking Snapshots (Business Intelligence)
        logger.info("=" * 50)
        logger.info("📸 PHASE 3: Booking Snapshots for BI")
        logger.info("🎯 Purpose: Daily demand tracking & forecasting")
        logger.info("=" * 50)
        
        try:
            # Create snapshot of next 60 days for BI analysis
            snapshot_end = datetime.now() + timedelta(days=60)
            
            logger.info(f"📊 Creating 60-day demand snapshot")
            logger.info(f"🔍 For BI analysis: booking velocity, demand patterns")
            
            snapshot_result = sync_booking_snapshots(
                end_date=snapshot_end.isoformat()
            )
            
            logger.info(f"✅ BI snapshots sync completed: {snapshot_result}")
            
        except Exception as e:
            log_error(logger, "BI snapshots sync", e)
            # Continue even if snapshots fail - main bookings are more critical
            logger.warning("🔄 Continuing despite snapshot failure")
        
        # PHASE 4: Historical Weather Update (NEW!)
        logger.info("=" * 50)
        logger.info("🌡️ PHASE 4: Historical Weather Update")
        logger.info("🎯 Purpose: Backfill yesterday's actual weather data")
        logger.info("=" * 50)
        
        try:
            historical_weather_result = sync_yesterday_weather()
            logger.info(f"✅ Historical weather update completed: {historical_weather_result}")
        except Exception as e:
            log_error(logger, "Historical weather update", e)
            logger.warning("🔄 Historical weather failure doesn't affect other data")
        
        # Get post-sync stats and calculate differences
        post_stats = get_comprehensive_stats()
        
        stats_diff = {}
        for key in pre_stats:
            if key in post_stats and key != "error":
                diff = post_stats[key] - pre_stats.get(key, 0)
                if diff != 0:
                    stats_diff[f"new_{key}"] = diff
        
        # Final comprehensive stats
        final_stats = {**post_stats, **stats_diff}
        log_sync_end(logger, "4-Phase Daily Sync", final_stats)
        
        # Business Intelligence Summary
        logger.info("📊 BUSINESS INTELLIGENCE SUMMARY:")
        logger.info("=" * 40)
        logger.info(f"📈 Total reservations in system: {post_stats.get('total_bookings', 0):,}")
        logger.info(f"📅 Today's bookings: {post_stats.get('today_bookings', 0)}")
        logger.info(f"🔮 Future bookings (60d): {post_stats.get('future_bookings', 0)}")
        logger.info(f"🌦️ Weather forecasts: {post_stats.get('total_weather', 0)} entries")
        logger.info(f"🌡️ Historical weather: {post_stats.get('total_weather_daily', 0)} days")
        logger.info(f"📸 BI snapshots available: {post_stats.get('snapshot_days', 0)} days")
        
        if 'new_recent_updates' in stats_diff:
            logger.info(f"🆕 New/updated records: {stats_diff['new_recent_updates']}")
        
        if 'new_todays_snapshots' in stats_diff:
            logger.info(f"📊 Today's new snapshots: {stats_diff['new_todays_snapshots']}")
        
        if 'new_todays_weather_daily' in stats_diff:
            logger.info(f"🌡️ Today's weather updates: {stats_diff['new_todays_weather_daily']}")
        
        # Performance metrics
        total_new_data = sum([v for k, v in stats_diff.items() if k.startswith('new_') and isinstance(v, int)])
        
        logger.info("🎉 Complete 4-phase sync finished successfully!")
        logger.info("💡 Ready for: demand forecasting, weather correlation analysis, BI reporting!")
        
        
    except Exception as e:
        logger.error(f"FATAL ERROR in daily sync: {e}")
        logger.error("Check API connections and database status")
        logger.error("Tables affected: bookings, booking_snapshots, weather_forecasts, weather_daily")
        raise

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