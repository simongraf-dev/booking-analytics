"""
Booking Sync - Main script for fetching booking data
Handles API calls, caching, and data transformation
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
from pathlib import Path
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Fallback für alte Python Versionen, aber besser wäre Python 3.9+ 
    from datetime import timezone
    ZoneInfo = None 


from config.settings import API_CONFIG, DATE_CONFIG
from config.logging_config import setup_logging
from src.database import save_booking_snapshot, get_db_connection, save_booking, save_booking_snapshot, save_bookings_batch
from src.utils import parse_booking


from utils import parse_booking

logger = setup_logging("booking-sync")

def fetch_bookings(start_date=None, end_date=None, cache_file=None):
    """
    Fetch bookings from GraphQL API with optional caching
    
    Args:
        start_date (str): ISO format "2025-11-27T00:00:00+01:00"
        end_date (str): ISO format "2025-11-29T23:59:59+01:00"  
        cache_file (str): Path to cache file (optional)
    
    Returns:
        list: Booking data from API
    """
    
    # Use defaults if not provided
    start_date = start_date or DATE_CONFIG['default_start']
    end_date = end_date or DATE_CONFIG['default_end']
    
    # Check cache first
    if cache_file and Path(cache_file).exists():
        print(f"📁 Loading bookings from cache: {cache_file}")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            print(f"✅ Loaded {len(cached_data['bookings'])} bookings from cache")
            return cached_data['bookings']
        except Exception as e:
            print(f"❌ Cache loading failed: {e}")
            print("🔄 Falling back to API call...")
    
    # GraphQL Query
    query = """
    query bookingsAnalytics($locationId: String!, $date: Date!, $endDate: Date!, $startingAfter: Date) {
        bookingsAnalytics(locationId: $locationId, date: $date, endDate: $endDate, startingAfter: $startingAfter) {
            cursor
            hasMore
            count
            bookings {
                _id
                date
                endDate
                people
                cancelled
                noShow
                walkIn
                source
                host
                tracking {
                    source
                    medium
                    campaign
                    __typename
                }
                rating
                tagIds
                bookingTagsCount {
                    key
                    value
                    __typename
                }
                payment {
                    status
                    __typename
                }
                __typename
            }
            __typename
        }
    }
    """
    
    # Request payload
    payload = {
        "operationName": "bookingsAnalytics",
        "query": query,
        "variables": {
            "locationId": API_CONFIG['location_id'],
            "date": start_date,
            "endDate": end_date,
            "startingAfter": None
        }
    }
    
    # Headers
    headers = {
        "content-type": "application/json",
        "account_token": API_CONFIG['token']
    }
    
    try:
        print(f"🌐 Fetching bookings from {start_date} to {end_date}")
        response = requests.post(
            url=API_CONFIG['url'],
            json=payload,
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        data = response.json()
        
        # Check for GraphQL errors
        if 'errors' in data:
            print(f"❌ GraphQL Errors: {data['errors']}")
            return None
            
        analytics_data = data['data']['bookingsAnalytics']
        bookings = analytics_data['bookings']
        
        print(f"✅ Fetched {len(bookings)} bookings from API")
        
        # Save to cache if requested
        if cache_file:
            cache_data = {
                "fetched_at": datetime.now().isoformat(),
                "date_range": {"start": start_date, "end": end_date},
                "count": analytics_data['count'],
                "bookings": bookings
            }
            
            # Create directory if needed
            Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            print(f"💾 Saved cache to {cache_file}")
        
        return bookings
        
    except requests.exceptions.RequestException as e:
        print(f"❌ API Request failed: {e}")
        return None



def generate_booking_snapshot(snapshot_date=None, forecast_days=60):
    """
    Generate fresh booking snapshot by fetching current data from Teburio API
    """
    
    if not snapshot_date:
        snapshot_date = datetime.now().date()
    
    # Berechne Datumsbereich: heute + 60 Tage
    start_date = snapshot_date
    end_date = snapshot_date + timedelta(days=forecast_days)
    
    # KORREKTUR: Zeitzonen-Handling
    # Wir nutzen "Europe/Berlin", damit Sommer-/Winterzeit automatisch korrekt ist.
    if ZoneInfo:
        tz = ZoneInfo("Europe/Berlin")
        # Wir setzen die Zeit auf Anfang des Tages in Berlin
        start_dt = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=tz)
        # Und Ende des Tages in Berlin
        end_dt = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=tz)
        
        # Formatieren für API (ISO 8601 mit Offset)
        start_date_str = start_dt.isoformat()
        end_date_str = end_dt.isoformat()
    else:
        # Fallback (weniger genau bei Zeitumstellung)
        logger.warning("ZoneInfo nicht verfügbar, nutze Hardcoded Offset +01:00")
        start_date_str = start_date.strftime("%Y-%m-%dT00:00:00+01:00")
        end_date_str = end_date.strftime("%Y-%m-%dT23:59:59+01:00")
    
    logger.info(f"📊 Generating FRESH booking snapshot for {snapshot_date}")
    logger.info(f"📅 Fetching from Teburio: {start_date_str} to {end_date_str}")
    
    # Hole aktuelle Daten (OHNE Cache!)
    fresh_bookings = fetch_bookings_paginated(
        start_date=start_date_str,
        end_date=end_date_str,
        cache_file=None  # Kein Cache - immer fresh!
    )
    
    if not fresh_bookings:
        logger.error("❌ No fresh bookings received from API")
        return False
    
    logger.info(f"✅ Received {len(fresh_bookings)} fresh bookings from API")
    
    daily_stats = {}
    
    for booking in fresh_bookings:
        # Parse booking date
        booking_date = datetime.fromtimestamp(booking['date'] / 1000).date()
        
        if booking_date not in daily_stats:
            daily_stats[booking_date] = {
                'reservierungen': 0,
                'bestaetigt_personen': 0,
                'storniert_personen': 0,
                'online_personen': 0,
                'intern_personen': 0,
                'walk_in_personen': 0
            }
        
        # Analysiere Booking-Status
        people = booking['people']
        cancelled = booking.get('cancelled') or False
        no_show = booking.get('noShow') or False
        walk_in = booking.get('walkIn') or False
        source = booking.get('source')
        
        daily_stats[booking_date]['reservierungen'] += 1
        
        if not cancelled and not no_show:
            daily_stats[booking_date]['bestaetigt_personen'] += people
            
            if source == 'widget':
                daily_stats[booking_date]['online_personen'] += people
            elif walk_in:
                daily_stats[booking_date]['walk_in_personen'] += people
            elif source is None:
                daily_stats[booking_date]['intern_personen'] += people
        
        if cancelled:
            daily_stats[booking_date]['storniert_personen'] += people
            
    # Speichern...
    successful_saves = 0
    failed_saves = 0
    
    logger.info(f"💾 Saving {len(daily_stats)} daily forecasts to database...")
    
    for forecast_date, stats in daily_stats.items():
        snapshot_data = {
            'snapshot_created_at': snapshot_date,
            'forecast_date': forecast_date,
            **stats
        }
        
        if save_booking_snapshot(snapshot_data):
            successful_saves += 1
        else:
            failed_saves += 1
            logger.error(f"❌ Failed to save snapshot for {forecast_date}")
            
def fetch_bookings_paginated(start_date=None, end_date=None, cache_file=None):
    """
    Fetch ALL bookings with pagination support
    Uses cursor from API response to get next page
    """
    start_date = start_date or DATE_CONFIG['default_start']
    end_date = end_date or DATE_CONFIG['default_end']
    
    # Check cache first
    if cache_file and Path(cache_file).exists():
        logger.info(f"📁 Loading bookings from cache: {cache_file}")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            logger.info(f"✅ Loaded {len(cached_data['bookings'])} bookings from cache")
            return cached_data['bookings']
        except Exception as e:
            logger.error(f"❌ Cache loading failed: {e}")
            logger.info("🔄 Falling back to API call...")
    
    # Start pagination
    all_bookings = []
    cursor = None
    page = 1
    total_fetched = 0
    
    logger.info(f"🌐 Starting paginated fetch from {start_date} to {end_date}")
    
    while True:
        logger.info(f"📄 Fetching page {page}...")
        
        # GraphQL Query
        query = """
        query bookingsAnalytics($locationId: String!, $date: Date!, $endDate: Date!, $startingAfter: Date) {
            bookingsAnalytics(locationId: $locationId, date: $date, endDate: $endDate, startingAfter: $startingAfter) {
                cursor
                hasMore
                count
                bookings {
                    _id
                    date
                    endDate
                    people
                    cancelled
                    noShow
                    walkIn
                    source
                    host
                    tracking {
                        source
                        medium
                        campaign
                        __typename
                    }
                    rating
                    tagIds
                    bookingTagsCount {
                        key
                        value
                        __typename
                    }
                    payment {
                        status
                        __typename
                    }
                    __typename
                }
                __typename
            }
        }
        """
        
        # Request payload with cursor for pagination
        payload = {
            "operationName": "bookingsAnalytics",
            "query": query,
            "variables": {
                "locationId": API_CONFIG['location_id'],
                "date": start_date,
                "endDate": end_date,
                "startingAfter": cursor  # This is the pagination key!
            }
        }
        
        headers = {
            "content-type": "application/json",
            "account_token": API_CONFIG['token']
        }
        
        try:
            response = requests.post(
                url=API_CONFIG['url'],
                json=payload,
                headers=headers,
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Check for GraphQL errors
            if 'errors' in data:
                logger.error(f"❌ GraphQL Errors: {data['errors']}")
                break
                
            analytics_data = data['data']['bookingsAnalytics']
            bookings = analytics_data['bookings']
            
            # Add bookings to our collection
            all_bookings.extend(bookings)
            total_fetched += len(bookings)
            
            logger.info(f"✅ Page {page}: Got {len(bookings)} bookings (Total: {total_fetched})")
            
            # Check if more pages available
            has_more = analytics_data.get('hasMore', False)
            if not has_more:
                logger.info("🏁 No more pages available")
                break
            
            # Get cursor for next page
            cursor = analytics_data.get('cursor')
            if not cursor:
                logger.warning("🏁 No cursor for next page, stopping pagination")
                break
            
            page += 1
            
            if page > 50: 
                logger.warning("⚠️ Safety break: Too many pages")
                break
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API Request failed on page {page}: {e}")
            break
    
    logger.info(f"🎉 Pagination complete! Total bookings fetched: {len(all_bookings)}")
    
    # Save to cache if requested
    if cache_file and all_bookings:
        cache_data = {
            "fetched_at": datetime.now().isoformat(),
            "date_range": {"start": start_date, "end": end_date},
            "total_pages": page - 1,
            "count": len(all_bookings),
            "bookings": all_bookings
        }
        
        Path(cache_file).parent.mkdir(parents=True, exist_ok=True)
        
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Saved {len(all_bookings)} bookings to cache")
    
    return all_bookings


def sync_bookings(start_date, end_date):
    """
    Wrapper function for daily_sync.py compatibility
    Calls fetch_bookings_paginated and saves to database in a batch (single connection)
    """
    logger.info(f"🔄 Starting booking sync from {start_date} to {end_date}")
    
    bookings = fetch_bookings_paginated(
        start_date=start_date,
        end_date=end_date,
        cache_file=None
    )
    
    if not bookings:
        return {"status": "error", "message": "No bookings fetched"}
    
    # Bookings parsen VOR der Datenbank-Operation
    bookings_parsed = [parse_booking(b) for b in bookings]
    
    logger.info(f"💾 Saving {len(bookings_parsed)} parsed bookings to database in batch...")
    
    # Aufruf der Batch-Funktion (nutzt EINE Verbindung und Transaktion)
    saved_count = save_bookings_batch(bookings_parsed)

    failed_count = len(bookings_parsed) - saved_count
    
    logger.info(f"✅ Database save complete: {saved_count} saved, {failed_count} failed")
    
    return {
        "status": "success" if saved_count > 0 else "error",
        "message": f"Synced {len(bookings)} bookings, saved {saved_count}",
        "fetched": len(bookings),
        "saved": saved_count,
        "failed": failed_count
    }

def sync_booking_snapshots(end_date):
    """Wrapper for BI snapshots"""
    logger.info(f"📸 Creating booking snapshots up to {end_date}")
    
    success = generate_booking_snapshot(
        snapshot_date=datetime.now().date(),
        forecast_days=60
    )
    
    if success:
        return {"status": "success", "message": "Snapshots created successfully"}
    else:
        return {"status": "error", "message": "Snapshot creation failed"}

if __name__ == "__main__":
    # Test function for development
    import sys
    
    # Check für 'snapshot' Parameter
    if len(sys.argv) > 1 and sys.argv[1] == 'snapshot':
        # Ursprünglich test_snapshot(), jetzt verwenden wir die echte Funktion
        generate_booking_snapshot()
    else:
        # Deine bestehende Logik (angepasst auf logger):
        bookings = fetch_bookings(
            cache_file="data/bookings_cache.json"
        )
        
        if bookings:
            logger.info(f"🎉 Success! Got {len(bookings)} bookings")
            logger.info(f"📊 First booking: {bookings[0]['_id']}")
        else:
            logger.warning("❌ No bookings received")