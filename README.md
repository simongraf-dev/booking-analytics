Booking Analytics & AI Staffing OS

Production-grade restaurant intelligence system featuring AI-driven demand forecasting, smart staffing algorithms, and automated weather correlation.

🎯 Overview

This system transforms raw booking data into actionable operational insights. It combines historical data, real-time weather forecasts, and machine learning to predict walk-in guests and automatically generate efficient staff rosters for kitchen, service, and bar teams.

Built for real-world operations in Kiel, Germany.

✨ Key Features

🤖 AI Demand Forecasting

Ridge Regression Model: Predicts spontaneous "walk-in" guests based on weather, weekdays, and holidays.

7-Day Rolling Forecast: continuously updated with the latest weather forecasts.

Weather Context: Automtically detects "Perfect Patio Weather" or "Cozy Indoor Weather".

👨‍🍳 Smart Staffing Engine

Automated Rostering: Calculates required staff for Kitchen, Pizza Station, Bar, Service, and Runners.

Cost Efficiency Logic: Suggests "Split Shifts" (e.g., 4h Peak Support) instead of full shifts to save labor costs.

Role-Specific Rules:

Pizza: Scales based on expected pizza count (approx. 120 guests threshold).

Bar: Reacts to weekend high-volume pressure.

Service: Adjusts ratios based on total guest load (Reservations + Walk-ins).

📊 Operational Dashboard

Tech Stack: Built with Streamlit and Plotly.

Real-Time Control: Trigger data sync and re-calculations directly from the UI.

Visual Insights: Stacked bar charts for total load and card-based staffing plans.

🏗️ Architecture

Data Model (PostgreSQL)

bookings - Core reservation data (syncs via GraphQL).

weather_forecasts - 16-day forecasts from OpenMeteo.

weather_daily - Historical weather ground truth for training.

walkin_forecast - (NEW) ML-generated predictions per day.

booking_snapshots - Demand velocity tracking.

Infrastructure

Backend: Python 3.10+ on Ubuntu VPS.

ML Ops: scikit-learn model trained on 3+ years of history.

Frontend: Streamlit dashboard for daily usage.

Automation: 4-Phase Cronjob Pipeline.

🚀 Quick Start

1. Environment Setup

# Clone repository
git clone [https://github.com/simongraf-dev/booking-analytics.git](https://github.com/simongraf-dev/booking-analytics.git)
cd booking-analytics

# Create virtual environment
python3 -m venv booking-env
source booking-env/bin/activate  # Linux/Mac
# booking-env\Scripts\activate   # Windows

# Install dependencies (now includes streamlit & plotly)
pip install -r requirements.txt


2. Configuration

Create a .env file with your credentials (see .env.example):

DB_HOST=...
GRAPHQL_API_URL=...
WEATHER_LATITUDE=54.32  # Kiel


3. Run the Dashboard

The command center for daily operations:

streamlit run dashboard.py


Opens automatically in your browser at http://localhost:8501

🔧 Automation & Pipelines

The system runs a daily ETL pipeline to keep predictions fresh:

# Full manual sync (Bookings + Weather + AI Prediction)
python src/daily_sync.py

# Run AI prediction only
python src/predict_walkins.py


Production Crontab (Server)

0 10 * * * /root/booking-analytics/booking-env/bin/python src/daily_sync.py >> /var/log/booking-sync.log 2>&1


📁 Project Structure

booking-analytics/
├── dashboard.py           # 🚀 Main Operation Dashboard (Streamlit)
├── src/
│   ├── predict_walkins.py # 🧠 AI Inference Script (Ridge Model)
│   ├── dashboard_data.py  # 📊 SQL-Views for Dashboard
│   ├── daily_sync.py      # 🔄 Orchestrator (Phases 1-4)
│   ├── booking_sync.py    # Teburio GraphQL Wrapper
│   ├── weather_sync.py    # OpenMeteo Integration
│   └── ...
├── models/
│   └── walkin_ridge_v1.pkl # Trained Model Artifact (Gitignored!)
├── sql/                   # Database Schemas
├── logs/                  # Application Logs
└── ...


📈 Business Logic Examples

Why "Smart" Staffing?
Instead of static shifts ("We need 3 waiters"), the system calculates:

"Expected load is 220 guests. Instead of 3 full-time waiters (24h), schedule 2 Full-time + 1 Peak-Runner (18:00-22:00)."
-> Saves 4 labor hours per day.

Deployed on Hetzner Cloud VPS.