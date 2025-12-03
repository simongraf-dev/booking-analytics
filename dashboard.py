import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# Pfad-Setup für Importe (fügt das aktuelle Verzeichnis zum Pfad hinzu)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importe aus den src-Modulen
from src.dashboard_data import get_forecast_view
from src.booking_sync import sync_bookings
from src.weather_forecast import sync_weather
from src import predict_walkins

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Personal & Demand Forecast",
    page_icon="bar_chart",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- CSS STYLING (MODERN DASHBOARD LOOK) ---
st.markdown("""
    <style>
        /* General Layout */
        .block-container {padding-top: 1rem; padding-bottom: 3rem;}
        h1 {font-family: 'Helvetica Neue', sans-serif; font-weight: 700; color: #1e293b; letter-spacing: -0.5px;}
        h3 {font-weight: 600; color: #334155; margin-top: 2rem; margin-bottom: 1rem; border-bottom: 2px solid #f1f5f9; padding-bottom: 0.5rem;}
        
        /* Metric Cards */
        div[data-testid="metric-container"] {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
            border: 1px solid #e2e8f0;
            transition: all 0.2s ease;
        }
        div[data-testid="metric-container"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.05);
        }
        
        /* Staffing Card System */
        .staff-card-container {
            display: grid;
            grid-template-columns: repeat(5, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .staff-card {
            background-color: #ffffff;
            border-radius: 12px;
            padding: 0;
            text-align: center;
            height: 100%;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
            border: 1px solid #e2e8f0;
            transition: transform 0.2s, box-shadow 0.2s;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }
        
        .staff-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
        }
        
        /* Colored Top Borders for Roles */
        .border-top-kitchen { border-top: 4px solid #ef4444; } /* Red */
        .border-top-pizza { border-top: 4px solid #f97316; }   /* Orange */
        .border-top-bar { border-top: 4px solid #3b82f6; }     /* Blue */
        .border-top-service { border-top: 4px solid #8b5cf6; } /* Purple */
        .border-top-runner { border-top: 4px solid #10b981; }  /* Emerald */
        
        .staff-header {
            padding: 15px 10px 5px;
            background-color: #f8fafc;
            border-bottom: 1px solid #f1f5f9;
        }
        
        .staff-role { 
            font-size: 0.75rem; 
            color: #64748b; 
            text-transform: uppercase; 
            letter-spacing: 1.2px; 
            font-weight: 700;
        }
        
        .staff-body {
            padding: 10px;
            flex-grow: 1;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        .staff-count { 
            font-size: 2.5rem; 
            font-weight: 800; 
            color: #0f172a; 
        }
        
        .staff-footer {
            padding: 12px;
            background-color: #ffffff;
            border-top: 1px dashed #e2e8f0;
        }
        
        .shift-tag {
            display: inline-block;
            font-size: 0.75rem;
            padding: 4px 8px;
            border-radius: 9999px;
            font-weight: 600;
            background-color: #f1f5f9;
            color: #475569;
            width: 100%;
        }
        
        .shift-tag.peak {
            background-color: #fff7ed;
            color: #c2410c;
            border: 1px solid #ffedd5;
        }
        
        /* Custom Button */
        .stButton button {
            background-color: #ffffff;
            color: #0f172a;
            border: 1px solid #cbd5e1;
            font-weight: 600;
        }
        .stButton button:hover {
            border-color: #94a3b8;
            background-color: #f8fafc;
        }
    </style>
""", unsafe_allow_html=True)

# --- HELPER FUNCTIONS ---

def refresh_data():
    """Führt einen schnellen Sync der relevanten Daten durch."""
    with st.spinner('Synchronisiere Live-Daten...'):
        start_sync = datetime.now().date().isoformat()
        end_sync = (datetime.now().date() + timedelta(days=10)).isoformat()
        try:
            sync_bookings(start_sync, end_sync)
            sync_weather()
            predict_walkins.main()
            st.success("Daten aktuell!")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"Fehler: {e}")

def get_smart_shift_plan(role, count):
    """
    Simuliert einen intelligenten Algorithmus, der Stunden spart.
    """
    if count <= 1:
        return "1x Basis"
    
    if count == 2:
        if role == "Pizza":
            return "1x Lang + 1x Peak (18-21)"
        elif role == "Bar":
            return "1x Lang + 1x Support"
        else:
            return "1x Lang + 1x Peak (4h)"

    if count == 3:
        return "2x Lang + 1x Peak (18-22)"
        
    if count >= 4:
        return f"2x Station + {count-2}x Runner"
        
    return ""

def calculate_detailed_staffing(guests, is_weekend):
    """
    Berechnet den Personalbedarf basierend auf Gästezahlen.
    """
    kitchen = 4 if guests > 250 else 3
    pizza = 2 if guests >= 120 else 1
    
    if guests < 100:
        bar = 1
    elif guests > 200 and is_weekend:
        bar = 3
    else:
        bar = 2
        
    if guests >= 300:
        service = 4
    elif guests >= 200:
        service = 3
    else:
        service = 2
        
    if guests >= 300:
        runner = 3
    elif guests >= 200:
        runner = 2
    else:
        runner = 1
        
    return {
        "Küche": kitchen,
        "Pizza": pizza,
        "Bar": bar,
        "Service": service,
        "Runner": runner
    }

# --- HEADER ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("Forecast & Staffing")
    st.caption(f"Letztes Update: {datetime.now().strftime('%H:%M')} Uhr • Standort: Kiel")

with col2:
    st.write("")
    if st.button("↻ Daten aktualisieren"):
        refresh_data()

# --- DATA LOADING ---
df = get_forecast_view(days_ahead=7)

if df.empty:
    st.warning("Keine Daten gefunden. Bitte erst Daten synchronisieren.")
    st.stop()

# Helper Columns
df['is_weekend'] = pd.to_datetime(df['datum']).dt.dayofweek.isin([4, 5, 6]) 
df['staff_plan'] = df.apply(lambda x: calculate_detailed_staffing(x['total_guests'], x['is_weekend']), axis=1)

# --- KPI SECTION ---
st.markdown("### 📊 Tages-Status")
today = pd.Timestamp.now().date()
row_today = df[df['datum'] == today]

if not row_today.empty:
    guests_today = int(row_today['total_guests'].iloc[0])
    res_today = int(row_today['reservations'].iloc[0])
    walk_today = int(row_today['walkins_pred'].iloc[0])
    staff_today = row_today['staff_plan'].iloc[0]
    total_staff = sum(staff_today.values())
    temp_today = row_today['temp'].iloc[0]
else:
    guests_today = 0
    res_today = 0
    walk_today = 0
    total_staff = 0
    temp_today = 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("Gäste Erwartet", f"{guests_today}", f"{res_today} Res. • {walk_today} Lauf")
k2.metric("Personal Gesamt", f"{total_staff}", "Mitarbeiter heute")
k3.metric("Wetter", f"{temp_today:.1f}°C", "Prognose")

# Utilization Bar Custom
capacity = 350
utilization = min(1.0, guests_today / capacity)
with k4:
    st.metric("Auslastung", f"{int(utilization*100)}%", f"von {capacity}")
    st.progress(utilization)

st.write("") # Spacer

# --- PERSONAL-PLANER ---
st.markdown("### 🗓️ Einsatzplanung")

# Datumsauswahl
c_date, c_info = st.columns([2, 3])
with c_date:
    selected_date_str = st.select_slider(
        "Tag auswählen:",
        options=[d.strftime('%Y-%m-%d') for d in df['datum']],
        format_func=lambda x: f"{pd.to_datetime(x).strftime('%a, %d.%m.')}"
    )

# Daten filtern
selected_row = df[df['datum'].astype(str) == selected_date_str].iloc[0]
plan = selected_row['staff_plan']
guests_plan = selected_row['total_guests']

# Info Text
with c_info:
    st.info(f"Basis: **{int(guests_plan)} Gäste** ({'Wochenende' if selected_row['is_weekend'] else 'Wochentag'}). Intelligente Split-Schichten aktiviert.")

# Grid Layout Cards
c1, c2, c3, c4, c5 = st.columns(5)

def staff_card_html(role, count, color_class, icon_char):
    shift_text = get_smart_shift_plan(role, count)
    # Check if Peak shift is involved for highlighting
    tag_class = "peak" if "Peak" in shift_text or "Support" in shift_text else "standard"
    
    return f"""
    <div class="staff-card {color_class}">
        <div class="staff-header">
            <span class="staff-role">{icon_char} {role}</span>
        </div>
        <div class="staff-body">
            <span class="staff-count">{count}</span>
        </div>
        <div class="staff-footer">
            <span class="shift-tag {tag_class}">{shift_text}</span>
        </div>
    </div>
    """

with c1: st.markdown(staff_card_html("Küche", plan["Küche"], "border-top-kitchen", "🍳"), unsafe_allow_html=True)
with c2: st.markdown(staff_card_html("Pizza", plan["Pizza"], "border-top-pizza", "🍕"), unsafe_allow_html=True)
with c3: st.markdown(staff_card_html("Bar", plan["Bar"], "border-top-bar", "🍺"), unsafe_allow_html=True)
with c4: st.markdown(staff_card_html("Service", plan["Service"], "border-top-service", "🛎️"), unsafe_allow_html=True)
with c5: st.markdown(staff_card_html("Runner", plan["Runner"], "border-top-runner", "👟"), unsafe_allow_html=True)

st.write("") 

# --- CHART & TABLE ---
t1, t2 = st.tabs(["📈 Wochentrend Analyse", "📋 Detailliste Export"])

with t1:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['display_date'], 
        y=df['reservations'], 
        name='Reserviert', 
        marker_color='#334155', # Slate 700
        hovertemplate='%{y} Reservierungen<extra></extra>'
    ))
    fig.add_trace(go.Bar(
        x=df['display_date'], 
        y=df['walkins_pred'], 
        name='Walk-In (AI)', 
        marker_color='#94a3b8', # Slate 400
        hovertemplate='%{y} Walk-Ins<extra></extra>'
    ))
    fig.update_layout(
        barmode='stack', 
        height=350, 
        margin=dict(t=20,b=0,l=0,r=0), 
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        yaxis=dict(showgrid=True, gridcolor='#f1f5f9'),
        xaxis=dict(showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig, use_container_width=True)

with t2:
    table_data = df.copy()
    for role in ["Küche", "Pizza", "Bar", "Service", "Runner"]:
        table_data[role] = table_data['staff_plan'].apply(lambda x: x[role])
    
    st.dataframe(
        table_data[['datum', 'wochentag', 'total_guests', 'reservations', 'walkins_pred', 'Küche', 'Pizza', 'Bar', 'Service', 'Runner']],
        column_config={
            "datum": "Datum",
            "wochentag": "Tag",
            "total_guests": st.column_config.NumberColumn("Gäste", format="%d"),
            "reservations": st.column_config.NumberColumn("Res.", format="%d"),
            "walkins_pred": st.column_config.NumberColumn("Lauf.", format="%d"),
        },
        hide_index=True,
        use_container_width=True
    )