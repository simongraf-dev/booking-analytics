"""
Database operations for booking analytics
Handles PostgreSQL connections and data insertion
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import psycopg2
import json
from datetime import datetime
from config.settings import DB_CONFIG

def get_db_connection():
    """Create PostgreSQL connection to VPS"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection successful")
        return conn
    except psycopg2.Error as e:
        print(f"❌ Database connection failed: {e}")
        return None

def test_connection():
    """Quick test if database is reachable"""
    conn = get_db_connection()
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bookings;")
        count = cursor.fetchone()[0]
        print(f"📊 Current bookings in database: {count}")
        cursor.close()
        conn.close()
        return True
    return False

def save_booking(conn, booking):
    """Insert single booking with conflict handling"""
    try:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO bookings (
            id, booking_date, end_date, people, cancelled, no_show, walk_in,
            source, host, tracking, tag_ids, booking_tags_count, payment, rating
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s
        )
        ON CONFLICT (id) DO UPDATE SET
            updated_at = NOW()
        """
        
        cursor.execute(insert_query, (
            booking['id'],
            booking['booking_date'],
            booking['end_date'], 
            booking['people'],
            booking['cancelled'],
            booking['no_show'],
            booking['walk_in'],
            booking['source'],
            booking['host'],
            booking['tracking'],
            booking['tag_ids'],
            booking['booking_tags_count'],
            booking['payment'],
            booking['rating']
        ))
        
        conn.commit()  # ← FEHLT! Ohne das werden keine Daten gespeichert
        cursor.close()
        return True    # ← FEHLT! Ohne Return meldet sync_bookings "failed"
        
    except Exception as e:
        conn.rollback()
        print(f"Database error: {e}")
        return False

def save_bookings_batch(bookings_parsed):
    """Save multiple bookings to database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        saved_count = 0
        for booking in bookings_parsed:
            save_booking(conn, booking)
            saved_count += 1
        
        conn.commit()
        print(f"✅ Saved {saved_count} bookings to database")
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database save failed: {e}")
        conn.rollback()
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    # Test database connection
    test_connection()


def get_latest_booking_date():
    """Get most recent booking date for incremental updates"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(booking_date) FROM bookings;")
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result


def save_booking_snapshot(snapshot_data):
    """Save daily booking snapshot to database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO booking_snapshots (
            snapshot_created_at, forecast_date, reservierungen, 
            bestaetigt_personen, storniert_personen, online_personen, 
            intern_personen, walk_in_personen
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (snapshot_created_at, forecast_date) DO UPDATE SET
            reservierungen = EXCLUDED.reservierungen,
            bestaetigt_personen = EXCLUDED.bestaetigt_personen,
            storniert_personen = EXCLUDED.storniert_personen,
            online_personen = EXCLUDED.online_personen,
            intern_personen = EXCLUDED.intern_personen,
            walk_in_personen = EXCLUDED.walk_in_personen,
            created_at = NOW()
        """
        
        cursor.execute(insert_query, (
            snapshot_data['snapshot_created_at'],
            snapshot_data['forecast_date'],
            snapshot_data['reservierungen'],
            snapshot_data['bestaetigt_personen'],
            snapshot_data['storniert_personen'],
            snapshot_data['online_personen'],
            snapshot_data['intern_personen'],
            snapshot_data['walk_in_personen']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Snapshot save failed: {e}")
        conn.rollback()
        conn.close()
        return False

def save_weather_forecast(weather_data):
    """Save weather forecast data to database"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO weather_forecasts (
            forecast_created_at, forecast_date, days_ahead,
            temperature_2m_max, temperature_2m_min, precipitation_sum,
            precipitation_probability_mean, sunshine_duration, wind_speed_10m_max,
            cloud_cover_mean, weathercode, apparent_temperature_max,
            apparent_temperature_min
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (forecast_created_at, forecast_date) DO UPDATE SET
            temperature_2m_max = EXCLUDED.temperature_2m_max,
            temperature_2m_min = EXCLUDED.temperature_2m_min,
            precipitation_sum = EXCLUDED.precipitation_sum,
            precipitation_probability_mean = EXCLUDED.precipitation_probability_mean,
            sunshine_duration = EXCLUDED.sunshine_duration,
            wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
            cloud_cover_mean = EXCLUDED.cloud_cover_mean,
            weathercode = EXCLUDED.weathercode,
            apparent_temperature_max = EXCLUDED.apparent_temperature_max,
            apparent_temperature_min = EXCLUDED.apparent_temperature_min,
            created_at = NOW()
        """
        
        cursor.execute(insert_query, (
            weather_data['forecast_created_at'],
            weather_data['forecast_date'],
            weather_data['days_ahead'],
            weather_data['temperature_2m_max'],
            weather_data['temperature_2m_min'],
            weather_data['precipitation_sum'],
            weather_data['precipitation_probability_mean'],
            weather_data['sunshine_duration'],
            weather_data['wind_speed_10m_max'],
            weather_data['cloud_cover_mean'],
            weather_data['weathercode'],
            weather_data['apparent_temperature_max'],
            weather_data['apparent_temperature_min']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Weather save failed: {e}")
        conn.rollback()
        conn.close()
        return False
    

