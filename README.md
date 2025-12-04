Booking Analytics & Smart Staffing OS

Operatives Betriebssystem für Gastronomie: KI-Gästeprognose, Personalplanung und Umsatzvorschau.

Gebaut für den realen Einsatz in Kiel.

🚀 Features

1. KI-Bedarfsprognose

Walk-In Vorhersage: Nutzt Machine Learning (Ridge Regression), um spontane Gäste basierend auf Wetter, Wochentag und Ferien vorherzusagen.

Wetter-Korrelation: Erkennt automatisch "Terrassen-Wetter" oder "Gemütliches Innen-Wetter".

Unsicherheits-Faktor: Visualisiert, wie sicher die Prognose für zukünftige Tage ist (je weiter weg, desto unsicherer).

2. Intelligente Personalplanung

Smart Shifts: Berechnet nicht nur Köpfe ("3 Kellner"), sondern schlägt effiziente Schichtmodelle vor (z.B. "2x Lang + 1x Peak 18-22 Uhr").

Rollen-Logik: Spezifische Regeln für Küche, Pizza-Station, Bar, Service und Runner.

Kosten-Effizienz: Spart aktiv Arbeitsstunden durch bedarfsgerechte Planung.

3. Dashboard & Operations

Streamlit UI: Modernes, sauberes Dashboard im Corporate Design.

User Login: Rollenbasierter Zugriff (Admin/User) mit sicherem Password-Hashing.

Echtzeit-Daten: Sync mit Reservierungssystem und Wetterdienst auf Knopfdruck.

🛠️ Technologie-Stack

Frontend: Streamlit, Plotly

Backend/Logic: Python 3.10+

Datenbank: PostgreSQL (Hetzner Cloud)

ML Engine: Scikit-Learn

APIs: Teburio (Reservierungen), OpenMeteo (Wetter)

🏁 Schnellstart

Installation

# Repository klonen
git clone [https://github.com/simongraf-dev/booking-analytics.git](https://github.com/simongraf-dev/booking-analytics.git)
cd booking-analytics

# Environment erstellen
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Abhängigkeiten installieren
pip install -r requirements.txt


Konfiguration (.env)

Erstelle eine .env Datei im Hauptverzeichnis:

# Datenbank
DB_HOST=deine-ip
DB_NAME=booking_analytics
DB_USER=...
DB_PASSWORD=...

# APIs
GRAPHQL_API_URL=...
ACCOUNT_TOKEN=...
LOCATION_ID=...
WEATHER_LATITUDE=54.32

# Business Logic
PROKOPFUMSATZ=30.0
PROKOPFUMSATZ_MONTAG=25.0


User anlegen

Da das Dashboard geschützt ist, musst du erst einen Admin anlegen:

python src/create_admin.py


Starten

streamlit run dashboard.py


📂 Projektstruktur

booking-analytics/
├── dashboard.py           # 🚀 Hauptanwendung (Streamlit)
├── src/
│   ├── auth.py            # Login & Sicherheit
│   ├── predict_walkins.py # KI-Modell Inferenz
│   ├── dashboard_data.py  # SQL Aggregationen
│   ├── booking_sync.py    # API Connector
│   └── create_admin.py    # Admin Tool
├── models/
│   └── walkin_ridge_v1.pkl # (Gitignored)
└── ...


Status: In Produktion.