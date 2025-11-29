# Booking Analytics Pipeline

**Production-grade restaurant booking analytics with weather correlation and demand forecasting.**

## 🎯 Overview

Comprehensive data pipeline for restaurant booking intelligence featuring automated synchronization, business intelligence snapshots, and weather correlation analysis.

## 🏗️ Architecture

### 3-Table Data Model
- **`bookings`** - Main reservations (3 days back + 60 days forward)
- **`booking_snapshots`** - Daily BI snapshots for demand velocity tracking  
- **`weather_forecasts`** - 16-day weather data for correlation analysis

### Infrastructure
- **Backend:** PostgreSQL on Ubuntu VPS
- **APIs:** Teburio GraphQL + Open Meteo REST
- **Automation:** Daily cron jobs with comprehensive logging
- **Environment:** Python virtual environment with modular architecture

## ✨ Features

- 📊 **Business Intelligence:** Daily demand snapshots for forecasting models
- 🔄 **Automated Sync:** Production cronjobs with error handling
- 🌤️ **Weather Correlation:** 16-day forecasts for demand pattern analysis
- 📈 **Demand Velocity:** Track booking patterns over time
- 🔒 **Production Ready:** Comprehensive logging and monitoring
- 📝 **Professional Logging:** Centralized logs with performance metrics

## 🚀 Quick Start

### Environment Setup
```bash
# Clone repository
git clone https://github.com/simongraf-dev/booking-analytics.git
cd booking-analytics

# Create virtual environment
python3 -m venv booking-env
source booking-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your credentials
```

### Configuration
```bash
# Database Configuration
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=booking_analytics
DB_USER=bookings_user
DB_PASSWORD=your_secure_password

# API Configuration
GRAPHQL_API_URL=https://app.teburio.de/graphql
ACCOUNT_TOKEN=your_api_token
LOCATION_ID=your_location_id
```

## 🔧 Usage

### Manual Sync
```bash
# Complete daily sync (all 3 tables)
python src/daily_sync.py

# Individual components
python src/booking_sync.py
python src/weather_sync.py
```

### Production Automation
```bash
# Setup daily cronjob (10:00 AM)
crontab -e
# Add: 0 10 * * * cd /root/booking-analytics && /root/booking-analytics/booking-env/bin/python src/daily_sync.py >> /var/log/booking-sync.log 2>&1
```

### Monitoring
```bash
# Check sync logs
tail -f /var/log/booking-analytics/daily-sync.log

# View sync statistics
python config/settings.py  # Validate configuration
python src/database.py     # Test database connection
```

## 📊 Business Intelligence

### Demand Velocity Analysis
```sql
-- Track how bookings develop over time for specific date
SELECT snapshot_created_at::date, reservierungen 
FROM booking_snapshots 
WHERE forecast_date = '2025-12-31'
ORDER BY snapshot_created_at;
```

### Weather Correlation
```sql
-- Analyze weather impact on bookings
SELECT w.temperature_2m_max, AVG(b.people) as avg_party_size
FROM weather_forecasts w
JOIN bookings b ON DATE(w.forecast_date) = DATE(b.booking_date)
GROUP BY w.temperature_2m_max;
```

## 📁 Project Structure
```
booking-analytics/
├── README.md
├── requirements.txt
├── .env.example
├── config/
│   ├── settings.py          # Environment configuration
│   └── logging_config.py    # Centralized logging setup
├── src/
│   ├── daily_sync.py        # Main orchestrator
│   ├── booking_sync.py      # Teburio API integration
│   ├── weather_sync.py      # Weather API integration
│   ├── database.py          # PostgreSQL operations
│   └── utils.py            # Helper functions
├── sql/
│   ├── schema.sql          # Database schema
│   └── indexes.sql         # Performance indexes
├── notebooks/              # Analytics notebooks
└── scripts/               # Utility scripts
```

## 🔍 Monitoring & Logs

### Log Locations
- **Daily Sync:** `/var/log/booking-analytics/daily-sync.log`
- **Booking Sync:** `/var/log/booking-analytics/booking-sync.log`  
- **Weather Sync:** `/var/log/booking-analytics/weather-sync.log`

### Health Checks
```bash
# Quick status check
python config/settings.py

# Database connectivity
python src/database.py

# View recent sync activity
tail -20 /var/log/booking-analytics/daily-sync.log
```

## 🎯 Data Pipeline

### Daily Sync Process (10:00 AM)
1. **Phase 1:** Main bookings sync (3 days back + 60 days forward)
2. **Phase 2:** Create BI snapshot of next 60 days  
3. **Phase 3:** Sync 16-day weather forecasts
4. **Reporting:** Performance statistics and change tracking

### Business Intelligence Features
- **Demand Forecasting:** Predictive models based on historical patterns
- **Weather Impact Analysis:** Correlation between weather and bookings
- **Booking Velocity:** Track reservation patterns over time
- **Capacity Planning:** Optimize restaurant capacity based on demand

## 🛠️ Development

### Local Development
```bash
# Setup SSH tunnel for database access
ssh -L 5432:127.0.0.1:5432 root@your-vps-ip -N

# Run components locally
python src/daily_sync.py
```

### Deployment
```bash
# Update production
git pull origin main
source booking-env/bin/activate
pip install -r requirements.txt

# Restart services if needed
```

## 📈 Performance

- **45,000+ historical records** successfully processed
- **Automated daily updates** with comprehensive error handling  
- **Production-grade infrastructure** with monitoring and logging
- **Scalable architecture** ready for additional data sources

## 🤝 Contributing

This is a professional portfolio project demonstrating enterprise-grade data pipeline architecture.

---

**Built with Python, PostgreSQL, and professional DevOps practices.**