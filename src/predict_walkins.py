import sys
import os
import joblib
import pandas as pd
import numpy as np
import holidays
from datetime import datetime, timedelta, date
import psycopg2
from psycopg2.extras import execute_values

# Pfad-Setup für Importe aus src/ und config/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.logging_config import setup_logging
from src.database import get_db_connection

logger = setup_logging("predict_walkins")

MODEL_PATH = "models/walkin_ridge_prod.pkl"

def load_model_artifact(path):
    """Lädt das trainierte Modell und die Feature-Liste."""
    if not os.path.exists(path):
        logger.error(f"Modell-Datei nicht gefunden: {path}")
        raise FileNotFoundError(f"Modell nicht gefunden: {path}")
    
    logger.info(f"Lade Modell von {path}...")
    artifact = joblib.load(path)
    return artifact

def get_data_for_prediction(conn, days_ahead=16):
    """
    Lädt Wettervorhersagen und Buchungsdaten.
    Holt auch historische Daten (letzte 7 Tage) für Rolling Averages.
    """
    # Wir brauchen Historie für Rolling Averages (z.B. walkin_7d_avg)
    # Startdatum für Datenabzug = Heute - 14 Tage (Puffer)
    today = datetime.now().date()
    history_start = today - timedelta(days=14)
    forecast_end = today + timedelta(days=days_ahead)
    
    logger.info(f"Lade Datenbasis: {history_start} bis {forecast_end}")

    # 1. Wetterdaten (Forecasts für Zukunft, ggf. Daily für Vergangenheit wäre genauer, 
    # aber wir nehmen vereinfacht Forecasts für alles oder füllen auf)
    weather_query = f"""
        SELECT DISTINCT ON (forecast_date)
            forecast_date as target_date,
            temperature_2m_max,
            temperature_2m_min,
            precipitation_sum,
            sunshine_duration,
            wind_speed_10m_max,
            cloud_cover_mean,
            weathercode
        FROM weather_forecasts
        WHERE forecast_date BETWEEN '{history_start}' AND '{forecast_end}'
        ORDER BY forecast_date, forecast_created_at DESC
    """
    df_weather = pd.read_sql(weather_query, conn)
    df_weather['target_date'] = pd.to_datetime(df_weather['target_date']).dt.date
    
    # 2. Alle Reservierungen (für reservations_count, people, avg_size)
    # Wir summieren alles, was NICHT Walk-In ist, als "Reservierung"
    booking_query = f"""
        SELECT 
            DATE(booking_date) as target_date,
            COUNT(*) as reservations_count,
            SUM(people) as reservations_people,
            AVG(people) as avg_reservation_size
        FROM bookings
        WHERE booking_date >= '{history_start}' 
          AND booking_date <= '{forecast_end} 23:59:59'
          AND cancelled = false 
          AND no_show = false
          AND walk_in = false
        GROUP BY DATE(booking_date)
    """
    df_bookings = pd.read_sql(booking_query, conn)
    df_bookings['target_date'] = pd.to_datetime(df_bookings['target_date']).dt.date

    # 3. Walk-Ins (Nur Historie, für walkin_7d_avg)
    walkin_query = f"""
        SELECT 
            DATE(booking_date) as target_date,
            SUM(people) as walkin_people
        FROM bookings
        WHERE booking_date >= '{history_start}' 
          AND booking_date < '{today}' -- Nur Vergangenheit
          AND walk_in = true
          AND cancelled = false
        GROUP BY DATE(booking_date)
    """
    df_walkins = pd.read_sql(walkin_query, conn)
    df_walkins['target_date'] = pd.to_datetime(df_walkins['target_date']).dt.date

    # 4. Merging
    # Wir erstellen ein Basis-Datumsgerüst
    all_dates = pd.date_range(start=history_start, end=forecast_end).date
    df_base = pd.DataFrame({'target_date': all_dates})
    
    df = pd.merge(df_base, df_weather, on='target_date', how='left')
    df = pd.merge(df, df_bookings, on='target_date', how='left')
    df = pd.merge(df, df_walkins, on='target_date', how='left')
    
    # Fill NAs
    df['reservations_count'] = df['reservations_count'].fillna(0)
    df['reservations_people'] = df['reservations_people'].fillna(0)
    df['walkin_people'] = df['walkin_people'].fillna(0)
    
    # Avg Reservation Size: Wenn 0 Reservierungen, nehmen wir den Durchschnitt der Spalte oder 0
    # Besser: 0, da keine Resos
    df['avg_reservation_size'] = df['avg_reservation_size'].fillna(0)

    # Wetter-NAs füllen (falls Forecasts fehlen)
    df['temperature_2m_max'] = df['temperature_2m_max'].fillna(method='ffill').fillna(15)
    df['temperature_2m_min'] = df['temperature_2m_min'].fillna(method='ffill').fillna(10)
    df['precipitation_sum'] = df['precipitation_sum'].fillna(0)
    df['sunshine_duration'] = df['sunshine_duration'].fillna(0)
    df['wind_speed_10m_max'] = df['wind_speed_10m_max'].fillna(10)
    df['cloud_cover_mean'] = df['cloud_cover_mean'].fillna(50)
    df['weathercode'] = df['weathercode'].fillna(0)

    return df

def calculate_weather_score(row):
    """Berechnet einen einfachen Wetter-Score (1-5) für Gastronomie."""
    score = 3 # Startwert Neutral
    temp = row.get('temp_max', 15)
    rain = row.get('precipitation_sum', 0)
    clouds = row.get('cloudcover_mean', 50)
    
    # Temperatur
    if 20 <= temp <= 26: score += 1
    elif temp < 10 or temp > 32: score -= 1
    
    # Regen
    if rain > 5: score -= 1
    if rain > 15: score -= 1
    if rain == 0: score += 0.5
    
    # Sonne/Wolken
    if clouds < 30: score += 0.5
    if clouds > 80: score -= 0.5
    
    return max(1, min(5, round(score)))

def feature_engineering(df):
    """
    Erstellt ALLE Features, die das Ridge-Modell erwartet.
    """
    logger.info("Führe Feature Engineering durch...")
    
    # Datums-Konvertierung
    df['target_date'] = pd.to_datetime(df['target_date'])
    
    # --- 1. Renaming & Basic Weather ---
    # Modell erwartet spezifische Namen
    df['temp_max'] = df['temperature_2m_max']
    df['temp_min'] = df['temperature_2m_min']
    df['windspeed_max'] = df['wind_speed_10m_max']
    df['cloudcover_mean'] = df['cloud_cover_mean']
    
    # Approximationen für fehlende DB-Spalten
    # Humidity ist nicht im Forecast Table -> Setze Standardwert Kiel (ca 75%)
    df['humidity'] = 75.0 
    # Precipitation Hours ist nicht im Forecast Table -> Schätzung aus Summe (grob)
    # Wenn Regen > 0, nehmen wir an es regnet ein paar Stunden. (Summe / 2 mm/h)
    df['precipitation_hours'] = df['precipitation_sum'].apply(lambda x: min(24.0, x * 2.0) if x > 0 else 0)

    # --- 2. Rolling Averages ---
    # Wir haben Daten von T-14. Wir berechnen Rolling auf dem ganzen DF.
    df = df.sort_values('target_date')
    
    # reservations_7d_avg (Durchschnitt der reservierten Personen letzte 7 Tage)
    df['reservations_7d_avg'] = df['reservations_people'].rolling(window=7, min_periods=1).mean()
    
    # walkin_7d_avg (Durchschnitt der Walkins letzte 7 Tage - ACHTUNG: Für Zukunft unbekannt)
    # Für die Zukunftstage müssen wir den *letzten bekannten* Wert nehmen (Shift).
    # Da wir walkin_people für Zukunft = 0 haben, würde der Rolling Average abstürzen.
    # Strategie: Wir berechnen den Rolling Average, und füllen Nullen in der Zukunft 
    # mit dem letzten gültigen Wert auf (ffill).
    df['walkin_7d_avg_raw'] = df['walkin_people'].rolling(window=7, min_periods=1).mean()
    # Maskiere Zukunftswerte (wo walkin_people 0 ist, weil es Zukunft ist - grobe Logik)
    today = pd.Timestamp(datetime.now().date())
    df.loc[df['target_date'] >= today, 'walkin_7d_avg_raw'] = np.nan
    df['walkin_7d_avg'] = df['walkin_7d_avg_raw'].ffill()
    
    # Fallback für ganz am Anfang
    df['walkin_7d_avg'] = df['walkin_7d_avg'].fillna(0)

    # --- 3. Zeitfeatures ---
    df['weekday'] = df['target_date'].dt.weekday
    df['month'] = df['target_date'].dt.month
    df['is_weekend'] = df['weekday'].apply(lambda x: 1 if x >= 5 else 0)
    
    df['month_sin'] = np.sin((df['month'] - 1) * (2. * np.pi / 12))
    df['month_cos'] = np.cos((df['month'] - 1) * (2. * np.pi / 12))
    
    # One-Hot Encoding für Weekdays (wd_1 bis wd_6, wd_0 ist Referenz/Drop)
    for i in range(1, 7):
        df[f'wd_{i}'] = (df['weekday'] == i).astype(int)

    # --- 4. Holidays (DE, SH, HH, DK) ---
    holidays_de = holidays.DE()
    holidays_sh = holidays.DE(subdiv='SH')
    holidays_hh = holidays.DE(subdiv='HH')
    holidays_dk = holidays.DK()
    
    df['is_holiday_de'] = df['target_date'].apply(lambda x: 1 if x in holidays_de else 0)
    df['is_holiday_sh'] = df['target_date'].apply(lambda x: 1 if x in holidays_sh else 0)
    df['is_holiday_hh'] = df['target_date'].apply(lambda x: 1 if x in holidays_hh else 0)
    df['is_holiday_dk'] = df['target_date'].apply(lambda x: 1 if x in holidays_dk else 0)
    
    # Ferien (Schulferien) - schwer exakt zu haben ohne extra Lib/API.
    # Setze Platzhalter 0 oder einfache Sommer/Winter-Logik
    # TODO: Echte Ferien-API anbinden.
    df['is_ferien_sh'] = 0 
    df['is_ferien_hh'] = 0
    
    # Brückentage
    # Einfache Logik: Wenn morgen Feiertag (Do->Fr frei) oder gestern Feiertag (Mo->Di frei)
    df['next_day_holiday'] = df['is_holiday_sh'].shift(-1).fillna(0)
    df['prev_day_holiday'] = df['is_holiday_sh'].shift(1).fillna(0)
    
    # Bridge Day: Heute ist Montag (0) UND gestern war Holiday? Nein.
    # Bridge Day klassisch: Heute ist Freitag (4) und Donnerstag war Holiday.
    # Oder Heute ist Montag (0) und Dienstag ist Holiday.
    df['bridge_day'] = 0
    df.loc[(df['weekday'] == 4) & (df['prev_day_holiday'] == 1), 'bridge_day'] = 1
    df.loc[(df['weekday'] == 0) & (df['next_day_holiday'] == 1), 'bridge_day'] = 1
    
    df['day_before_holiday'] = df['next_day_holiday']
    df['day_after_holiday'] = df['prev_day_holiday']

    # --- 5. Wetter Advanced & Squares ---
    df['weather_score'] = df.apply(calculate_weather_score, axis=1)
    
    # Cozy: Kalt und Regen oder Windig
    df['is_cozy_weather'] = ((df['temp_max'] < 10) & (df['precipitation_sum'] > 2)).astype(int)
    # Tourist: Warm und Sonnig
    df['is_tourist_weather'] = ((df['temp_max'] > 20) & (df['sunshine_duration'] > 5)).astype(int)
    
    # Squares
    features_to_square = [
        'temp_max', 'temp_min', 'precipitation_sum', 'precipitation_hours',
        'humidity', 'sunshine_duration', 'windspeed_max', 'cloudcover_mean'
    ]
    for col in features_to_square:
        df[f'{col}_sq'] = df[col] ** 2

    # --- 6. Interaktionen ---
    df['temp_x_weekend'] = df['temp_max'] * df['is_weekend']
    df['reservations_x_weekend'] = df['reservations_people'] * df['is_weekend']
    df['reservations_x_temp'] = df['reservations_people'] * df['temp_max']
    df['rain_x_clouds'] = df['precipitation_sum'] * df['cloudcover_mean']

    # --- Final: Filter auf ZUKUNFT (oder ab Heute) ---
    # Wir haben Historie mitgeschleppt für Rolling Avgs, brauchen aber nur Predictions ab heute
    df_future = df[df['target_date'] >= today].copy()
    
    return df_future

def save_predictions(conn, df_results, model_name="ridge_v1"):
    """Schreibt die Vorhersagen per Upsert in die Datenbank."""
    logger.info(f"Speichere {len(df_results)} Vorhersagen in DB...")
    
    data_tuples = []
    for _, row in df_results.iterrows():
        data_tuples.append((
            row['target_date'],
            float(row['pred_walkins']),
            model_name
        ))
    
    query = """
        INSERT INTO walkin_forecast (target_date, pred_walkins, model_name)
        VALUES %s
        ON CONFLICT (target_date, model_name)
        DO UPDATE SET
            pred_walkins = EXCLUDED.pred_walkins,
            run_at = NOW();
    """
    
    try:
        with conn.cursor() as cur:
            execute_values(cur, query, data_tuples)
        conn.commit()
        logger.info("✅ Vorhersagen erfolgreich gespeichert.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Fehler beim Speichern: {e}")
        raise

def main():
    conn = None
    try:
        # 1. Modell laden
        artifact = load_model_artifact(MODEL_PATH)
        model = artifact["model"]
        feature_cols = artifact["feature_cols"]
        
        # 2. DB Verbindung
        conn = get_db_connection()
        if not conn:
            raise ConnectionError("Konnte keine Verbindung zur Datenbank herstellen")

        # 3. Rohdaten holen (Wetter + Bookings)
        df = get_data_for_prediction(conn, days_ahead=16)
        
        if df.empty:
            logger.warning("Keine Daten für Vorhersage gefunden.")
            return

        # 4. Feature Engineering
        df_features = feature_engineering(df)
        
        # 5. Spaltenauswahl & Vorhersage
        # Sicherstellen, dass alle Spalten da sind
        missing_cols = set(feature_cols) - set(df_features.columns)
        if missing_cols:
            logger.warning(f"⚠️ Fehlende Spalten wurden mit 0 aufgefüllt: {missing_cols}")
            for col in missing_cols:
                df_features[col] = 0

        X_pred = df_features[feature_cols]

        logger.info("Berechne Vorhersagen...")
        preds = model.predict(X_pred)
        df_features['pred_walkins'] = np.maximum(0, np.round(preds)) # Keine negativen Walkins, runden
        
        # 6. Speichern
        save_predictions(conn, df_features, model_name="ridge_v1")
        
    except Exception as e:
        logger.error(f"Kritischer Fehler im Skript: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()