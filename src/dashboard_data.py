import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Pfad-Setup
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import get_db_connection

def get_forecast_view(days_ahead=21):
    """
    Holt einen kompletten View für das Dashboard:
    - Vorhergesagte Walk-Ins
    - Bestehende Reservierungen
    - Wetterdaten
    """
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()

    today = datetime.now().date()
    end_date = today + timedelta(days=days_ahead)

    # Diese Query joint alles zusammen für den gewählten Zeitraum
    query = f"""
    WITH 
    -- 1. Vorhersagen (Neueste Version pro Tag)
    forecasts AS (
        SELECT target_date, pred_walkins, model_name
        FROM walkin_forecast
        WHERE target_date BETWEEN '{today}' AND '{end_date}'
    ),
    -- 2. Reservierungen (Summiert pro Tag)
    res_data AS (
        SELECT 
            DATE(booking_date) as date,
            COUNT(*) as res_count,
            SUM(people) as res_people
        FROM bookings
        WHERE booking_date >= '{today}' 
          AND booking_date <= '{end_date} 23:59:59'
          AND cancelled = false 
          AND no_show = false
        GROUP BY DATE(booking_date)
    ),
    -- 3. Wetter (Neueste Vorhersage pro Tag)
    weather AS (
        SELECT DISTINCT ON (forecast_date)
            forecast_date,
            temperature_2m_max as temp_max,
            precipitation_sum as rain,
            sunshine_duration as sun,
            weathercode
        FROM weather_forecasts
        WHERE forecast_date BETWEEN '{today}' AND '{end_date}'
        ORDER BY forecast_date, forecast_created_at DESC
    )
    
    -- Zusammenfügen: Basis ist das Datum
    SELECT 
        w.forecast_date as datum,
        COALESCE(f.pred_walkins, 0) as walkins_pred,
        COALESCE(r.res_people, 0) as reservations,
        COALESCE(r.res_count, 0) as res_count,
        COALESCE(w.temp_max, 0) as temp,
        COALESCE(w.rain, 0) as rain,
        COALESCE(w.sun, 0) / 3600.0 as sun_hours, -- Sekunden zu Stunden
        w.weathercode
    FROM weather w
    LEFT JOIN forecasts f ON w.forecast_date = f.target_date
    LEFT JOIN res_data r ON w.forecast_date = r.date
    ORDER BY w.forecast_date ASC;
    """
    
    try:
        df = pd.read_sql(query, conn)
        df['datum'] = pd.to_datetime(df['datum']).dt.date
        df['total_guests'] = df['walkins_pred'] + df['reservations']
        
        # Wochentag für Anzeige
        german_days = {0:'Mo', 1:'Di', 2:'Mi', 3:'Do', 4:'Fr', 5:'Sa', 6:'So'}
        df['wochentag'] = pd.to_datetime(df['datum']).apply(lambda x: german_days[x.weekday()])
        df['display_date'] = df['wochentag'] + " " + df['datum'].astype(str).str[8:10] + "." + df['datum'].astype(str).str[5:7] + "."
        
        return df
    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
        return pd.DataFrame()
    finally:
        conn.close()