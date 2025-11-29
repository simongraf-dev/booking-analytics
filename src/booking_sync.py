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

from config.settings import API_CONFIG, DATE_CONFIG
from src.database import save_booking_snapshot, get_db_connection

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


def fetch_bookings_paginated(start_date=None, end_date=None, cache_file=None):
    """
    Fetch ALL bookings with pagination support
    Uses cursor from API response to get next page
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
    
    # Start pagination
    all_bookings = []
    cursor = None
    page = 1
    total_fetched = 0
    
    print(f"🌐 Starting paginated fetch from {start_date} to {end_date}")
    
    while True:
        print(f"📄 Fetching page {page}...")
        
        # GraphQL Query (same as before)
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
        
        # Headers
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
                print(f"❌ GraphQL Errors: {data['errors']}")
                break
                
            analytics_data = data['data']['bookingsAnalytics']
            bookings = analytics_data['bookings']
            
            # Add bookings to our collection
            all_bookings.extend(bookings)
            total_fetched += len(bookings)
            
            print(f"✅ Page {page}: Got {len(bookings)} bookings (Total: {total_fetched})")
            
            # Check if more pages available
            has_more = analytics_data.get('hasMore', False)
            if not has_more:
                print("🏁 No more pages available")
                break
            
            # Get cursor for next page
            cursor = analytics_data.get('cursor')
            if not cursor:
                print("🏁 No cursor for next page")
                break
            
            page += 1
            
            # Safety break (prevent infinite loops)
            if page > 50:  # Adjust as needed
                print("⚠️ Safety break: Too many pages")
                break
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API Request failed on page {page}: {e}")
            break
    
    print(f"🎉 Pagination complete! Total bookings fetched: {len(all_bookings)}")
    
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
        
        print(f"💾 Saved {len(all_bookings)} bookings to cache")
    
    return all_bookings





def generate_booking_snapshot(snapshot_date=None, forecast_days=60):
    """
    Generate fresh booking snapshot by fetching current data from Teburio API
    
    Args:
        snapshot_date: Date when snapshot is created (default: today)
        forecast_days: How many days ahead to fetch (default: 60)
    """
    
    if not snapshot_date:
        snapshot_date = datetime.now().date()
    
    # Berechne Datumsbereich: heute + 60 Tage
    start_date = snapshot_date
    end_date = snapshot_date + timedelta(days=forecast_days)
    
    # Formatiere für Teburio API
    start_date_str = start_date.strftime("%Y-%m-%dT00:00:00+01:00")
    end_date_str = end_date.strftime("%Y-%m-%dT23:59:59+01:00")
    
    print(f"📊 Generating FRESH booking snapshot for {snapshot_date}")
    print(f"📅 Fetching from Teburio: {start_date_str} to {end_date_str}")
    
    # Hole aktuelle Daten (OHNE Cache!)
    fresh_bookings = fetch_bookings_paginated(
        start_date=start_date_str,
        end_date=end_date_str,
        cache_file=None  # Kein Cache - immer fresh!
    )
    
    if not fresh_bookings:
        print("❌ No fresh bookings received from API")
        return False
    
    print(f"✅ Received {len(fresh_bookings)} fresh bookings from API")
    
    # Gruppiere Bookings nach Datum und analysiere
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
    
    # Speichere alle Tage in booking_snapshots
    successful_saves = 0
    failed_saves = 0
    
    print(f"💾 Saving {len(daily_stats)} daily forecasts to database...")
    
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
            print(f"❌ Failed to save snapshot for {forecast_date}")
        
        # Progress alle 10 Tage
        if successful_saves % 10 == 0 and successful_saves > 0:
            print(f"   📈 Saved {successful_saves} snapshots...")
    
    print(f"✅ Snapshot generation complete!")
    print(f"   💾 Successfully saved: {successful_saves} daily forecasts")
    print(f"   ❌ Failed saves: {failed_saves}")
    
    return successful_saves > 0

def main():
    """Test function for development"""
    import sys
    
    # Check für 'snapshot' Parameter
    if len(sys.argv) > 1 and sys.argv[1] == 'snapshot':
        test_snapshot()
        return
    
    # Deine bestehende Logik:
    bookings = fetch_bookings(
        cache_file="data/bookings_cache.json"
    )
    
    if bookings:
        print(f"🎉 Success! Got {len(bookings)} bookings")
        print(f"📊 First booking: {bookings[0]['_id']}")
    else:
        print("❌ No bookings received")

if __name__ == "__main__":
    main()

def sync_bookings(start_date, end_date):
    """
    Wrapper function for daily_sync.py compatibility
    Calls fetch_bookings_paginated and saves to database
    """
    print(f"🔄 Starting booking sync from {start_date} to {end_date}")
    
    bookings = fetch_bookings_paginated(
        start_date=start_date,
        end_date=end_date,
        cache_file=None
    )
    
    if not bookings:
        return {"status": "error", "message": "No bookings fetched"}
    
    return {
        "status": "success", 
        "message": f"Synced {len(bookings)} bookings",
        "count": len(bookings)
    }

def sync_booking_snapshots(end_date):
    """Wrapper for BI snapshots"""
    print(f"📸 Creating booking snapshots up to {end_date}")
    
    success = generate_booking_snapshot(
        snapshot_date=datetime.now().date(),
        forecast_days=60
    )
    
    if success:
        return {"status": "success", "message": "Snapshots created successfully"}
    else:
        return {"status": "error", "message": "Snapshot creation failed"}