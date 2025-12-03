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
from config.logging_config import setup_logging

def get_db_connection():
    """Create PostgreSQL connection to VPS"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # logger.info("Database connection successful")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
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

def save_bookings_batch(bookings_parsed):
    """Save multiple bookings to database using a single connection"""
    conn = get_db_connection()
    if not conn:
        return 0
    
    try:
        with conn.cursor() as cursor:
            # Query vorbereiten
            insert_query = """
            INSERT INTO bookings (
                id, booking_date, end_date, people, cancelled, no_show, walk_in,
                source, host, tracking, tag_ids, booking_tags_count, payment, rating
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                end_date = EXCLUDED.end_date,
                people = EXCLUDED.people,
                cancelled = EXCLUDED.cancelled,
                no_show = EXCLUDED.no_show,
                walk_in = EXCLUDED.walk_in,
                source = EXCLUDED.source,
                host = EXCLUDED.host,
                tracking = EXCLUDED.tracking,
                tag_ids = EXCLUDED.tag_ids,
                booking_tags_count = EXCLUDED.booking_tags_count,
                payment = EXCLUDED.payment,
                rating = EXCLUDED.rating,
                updated_at = NOW()
            """
            
            # Daten für executemany vorbereiten
            data_tuples = [
                (
                    b['id'], b['booking_date'], b['end_date'], b['people'],
                    b['cancelled'], b['no_show'], b['walk_in'], b['source'],
                    b['host'], b['tracking'], b['tag_ids'], b['booking_tags_count'],
                    b['payment'], b['rating']
                ) for b in bookings_parsed
            ]
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            return len(bookings_parsed)
            
    except psycopg2.Error as e:
        logger.error(f"Database batch save failed: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()
    
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

def save_weather_forecast_batch(forecasts):
    """Save multiple weather forecasts efficiently"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
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
            
            data_tuples = [
                (
                    w['forecast_created_at'], w['forecast_date'], w['days_ahead'],
                    w['temperature_2m_max'], w['temperature_2m_min'], w['precipitation_sum'],
                    w['precipitation_probability_mean'], w['sunshine_duration'], w['wind_speed_10m_max'],
                    w['cloud_cover_mean'], w['weathercode'], w['apparent_temperature_max'],
                    w['apparent_temperature_min']
                ) for w in forecasts
            ]
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            return True
            
    except psycopg2.Error as e:
        logger.error(f"Weather forecast batch save failed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def save_weather_forecast(weather_data):
    """Save weather forecast data to database"""
    conn = get_db_connection()
    if not conn:
        return False

def save_weather_daily_batch(weather_data_list):
    """Save historical/daily weather batch"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            insert_query = """
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
            """
            
            data_tuples = [
                (
                    w['date'], 'Kiel', w.get('temp_max'), w.get('temp_min'), w.get('temp_mean'),
                    w.get('precipitation_sum', 0), w.get('precipitation_hours', 0), w.get('humidity', 70),
                    w.get('windspeed_max', 0), w.get('pressure_msl', 1013), w.get('sunshine_duration', 0),
                    w.get('cloudcover_mean', 50), 15000, w.get('weathercode', 1),
                    'openmeteo', False, None
                ) for w in weather_data_list
            ]
            
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            return True
            
    except psycopg2.Error as e:
        logger.error(f"Weather daily batch save failed: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()
          
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
    

