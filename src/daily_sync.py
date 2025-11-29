"""
Daily synchronization for booking analytics
Syncs to 3 tables: bookings (main), booking_snapshots (BI), weather_forecasts
"""
import sys
import os
from datetime import datetime, timedelta
import time

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import setup_logging, log_sync_start, log_sync_end
from config.settings import validate_config
from src.booking_sync import sync_bookings, sync_booking_snapshots
from src.weather_sync import sync_weather
from src.database import get_db_connection

def get_comprehensive_stats():
    """Get statistics from all 3 tables"""
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
                # Weather stats
                "total_weather": total_weather,
                "todays_weather": todays_weather
            }
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()

def main():
    """Main daily sync orchestrator for all 3 tables"""
    
    # Initialize logging
    logger = setup_logging("daily-sync")
    
    try:
        # Validate configuration
        validate_config()
        logger.info("✅ Configuration validated")
        
        # Get pre-sync stats
        pre_stats = get_comprehensive_stats()
        log_sync_start(logger, "3-Table Daily Sync", f"Pre-sync: {pre_stats}")
        
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
            logger.error(f"❌ Main bookings sync failed: {e}")
            raise
        
        # PHASE 2: Booking Snapshots (Business Intelligence)
        logger.info("=" * 50)
        logger.info("📸 PHASE 2: Booking Snapshots for BI")
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
            logger.error(f"❌ BI snapshots sync failed: {e}")
            # Continue even if snapshots fail - main bookings are more critical
            logger.warning("🔄 Continuing despite snapshot failure")
        
        # PHASE 3: Weather Forecasts (Correlation Analysis)
        logger.info("=" * 50)
        logger.info("🌤️ PHASE 3: Weather Forecasts")
        logger.info("🎯 Purpose: Weather-demand correlation analysis")
        logger.info("=" * 50)
        
        try:
            logger.info(f"🌦️ Fetching 16-day weather forecast")
            logger.info(f"📊 For correlation with booking patterns")
            
            weather_result = sync_weather()
            logger.info(f"✅ Weather sync completed: {weather_result}")
            
        except Exception as e:
            logger.error(f"❌ Weather sync failed: {e}")
            logger.warning("🔄 Weather failure doesn't affect booking data")
        
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
        log_sync_end(logger, "3-Table Daily Sync", final_stats)
        
        # Business Intelligence Summary
        logger.info("📊 BUSINESS INTELLIGENCE SUMMARY:")
        logger.info("=" * 40)
        logger.info(f"📈 Total reservations in system: {post_stats.get('total_bookings', 0)}")
        logger.info(f"📅 Today's bookings: {post_stats.get('today_bookings', 0)}")
        logger.info(f"🔮 Future bookings (60d): {post_stats.get('future_bookings', 0)}")
        logger.info(f"📸 BI snapshots available: {post_stats.get('snapshot_days', 0)} days")
        logger.info(f"🌦️ Weather forecasts: {post_stats.get('total_weather', 0)} entries")
        
        if 'new_recent_updates' in stats_diff:
            logger.info(f"🆕 New/updated records: {stats_diff['new_recent_updates']}")
        
        if 'new_todays_snapshots' in stats_diff:
            logger.info(f"📊 Today's new snapshots: {stats_diff['new_todays_snapshots']}")
        
        logger.info("🎉 Complete 3-table sync finished successfully!")
        logger.info("💡 Ready for demand forecasting & weather correlation analysis!")
        
    except Exception as e:
        logger.error(f"💥 FATAL ERROR in daily sync: {e}")
        logger.error("🔍 Check API connections and database status")
        logger.error("📋 Tables affected: bookings, booking_snapshots, weather_forecasts")
        raise

if __name__ == "__main__":
    main()