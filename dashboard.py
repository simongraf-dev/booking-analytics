import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Environment Variablen laden
load_dotenv()

# Pfad-Setup
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.dashboard_data import get_forecast_view
from src.booking_sync import sync_bookings
from src.weather_forecast import sync_weather
from src import predict_walkins
from src.auth import verify_user

# --- PAGE CONFIG ---
PRIMARY_COLOR = "#8B0000" # TraumGmbH Rot
SECONDARY_COLOR = "#2C3E50" # Dark Blue/Grey

st.set_page_config(
    page_title="TraumGmbH Analytics",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- AUTHENTICATION LOGIC ---
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None
if 'user_role' not in st.session_state:
    st.session_state['user_role'] = None

def login_screen():
    st.markdown(f"""
        <style>
            .login-container {{
                max-width: 400px;
                margin: 0 auto;
                padding-top: 100px;
                text-align: center;
            }}
            .stButton button {{
                width: 100%;
                background-color: {PRIMARY_COLOR};
                color: white;
            }}
        </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1,2,1])
    
    with col2:
        st.title("TraumGmbH Login")
        with st.form("login_form"):
            username = st.text_input("Benutzername")
            password = st.text_input("Passwort", type="password")
            submit = st.form_submit_button("Anmelden")
            
            if submit:
                valid, role = verify_user(username, password)
                if valid:
                    st.session_state['authentication_status'] = True
                    st.session_state['user_role'] = role
                    st.session_state['username'] = username
                    st.rerun()
                else:
                    st.error("Benutzername oder Passwort falsch")

def logout():
    st.session_state['authentication_status'] = None
    st.session_state['user_role'] = None
    st.rerun()

# --- MAIN APP FLOW ---
if st.session_state['authentication_status'] is not True:
    login_screen()
    st.stop() 

# ==========================================
# DASHBOARD START
# ==========================================

# --- CSS STYLING (CLEAN & CORPORATE) ---
st.markdown(f"""
    <style>
        /* General Fonts & Spacing */
        .block-container {{padding-top: 1rem; padding-bottom: 3rem;}}
        h1, h2, h3 {{font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #1a1a1a;}}
        h1 {{font-weight: 700; letter-spacing: -0.5px; margin-bottom: 0.5rem;}}
        h3 {{font-size: 1.1rem; font-weight: 600; text-transform: uppercase; color: #666; margin-top: 2rem; border-bottom: 2px solid {PRIMARY_COLOR}; padding-bottom: 5px; display: inline-block;}}
        
        /* Focus Day Cards */
        .focus-card {{
            background-color: white;
            padding: 15px 20px;
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            box-shadow: 0 1px 2px rgba(0,0,0,0.03);
            height: 100%;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }}
        .focus-label {{ font-size: 0.85rem; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
        .focus-value {{ font-size: 1.8rem; font-weight: 700; color: #1a1a1a; margin-bottom: 0px; }}
        .focus-sub {{ font-size: 0.85rem; color: #888; margin-top: 2px; }}
        
        /* Progress Bar Fix */
        .compact-progress {{
            background-color: #eee;
            border-radius: 4px;
            height: 10px;
            width: 100%;
            margin-top: 10px;
            overflow: hidden;
        }}
        .compact-progress-bar {{
            height: 100%;
            display: block; /* Wichtig */
        }}

        /* Staffing Cards */
        .staff-card {{
            background-color: white;
            border: 1px solid #eee;
            border-radius: 6px;
            padding: 12px;
            text-align: center;
            height: 100%;
            transition: all 0.2s;
        }}
        .staff-card:hover {{
            border-color: {PRIMARY_COLOR};
            transform: translateY(-2px);
            box-shadow: 0 4px 10px rgba(0,0,0,0.05);
        }}
        .staff-role {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: 1px; color: #888; font-weight: 600; }}
        .staff-count {{ font-size: 2rem; font-weight: 700; color: {PRIMARY_COLOR}; line-height: 1.1; }}
        .staff-detail {{ margin-top: 8px; font-size: 0.75rem; color: #444; background-color: #f8f8f8; padding: 3px 6px; border-radius: 4px; display: inline-block; }}
        
        /* 3-Day Focus Cards */
        .day-card {{
            background-color: white;
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #eee;
            text-align: center;
        }}
        .day-header {{ font-weight: bold; font-size: 1rem; color: {SECONDARY_COLOR}; margin-bottom: 5px; }}
        .day-date {{ color: #888; font-size: 0.8rem; margin-bottom: 8px; }}
        .confidence-badge {{
            font-size: 0.65rem; padding: 2px 6px; border-radius: 10px;
            display: inline-block; margin-top: 5px; font-weight: 600;
        }}
        .conf-high {{ background-color: #d1fae5; color: #065f46; }}
        .conf-med {{ background-color: #fef3c7; color: #92400e; }} 
        .conf-low {{ background-color: #f3f4f6; color: #6b7280; }} 
    </style>
""", unsafe_allow_html=True)

# --- BUSINESS LOGIC & HELPERS ---

def get_revenue_per_head(is_monday):
    try:
        if is_monday:
            return float(os.getenv("PROKOPFUMSATZ_MONTAG", 25.0))
        return float(os.getenv("PROKOPFUMSATZ", 30.0))
    except ValueError:
        return 30.0

def calculate_revenue(row):
    is_monday = row['datum'].weekday() == 0
    per_head = get_revenue_per_head(is_monday)
    return row['total_guests'] * per_head

def refresh_data():
    with st.spinner('Aktualisiere Daten...'):
        start_sync = datetime.now().date().isoformat()
        end_sync = (datetime.now().date() + timedelta(days=10)).isoformat()
        try:
            sync_bookings(start_sync, end_sync)
            sync_weather()
            predict_walkins.main()
            st.success("Aktualisiert")
            st.cache_data.clear()
        except Exception as e:
            st.error(f"Fehler: {e}")

def get_smart_shift_plan(role, count):
    if count <= 1: return "1x Basis"
    if count == 2:
        return "1x Lang + 1x Peak (4h)" if role != "Küche" else "2x Basis"
    if count == 3: return "2x Lang + 1x Peak"
    if count >= 4: return f"2x Station + {count-2}x Support"
    return ""

def calculate_staffing(guests, is_weekend):
    kitchen = 4 if guests > 250 else 3
    pizza = 2 if guests >= 120 else 1
    if guests < 100: bar = 1
    elif guests > 200 and is_weekend: bar = 3
    else: bar = 2
    if guests >= 300: service = 4
    elif guests >= 200: service = 3
    else: service = 2
    if guests >= 300: runner = 3
    elif guests >= 200: runner = 2
    else: runner = 1
    return {"Küche": kitchen, "Pizza": pizza, "Bar": bar, "Service": service, "Runner": runner}

# Helper Function für Staff Box
def staff_box(col, role, count):
    detail = get_smart_shift_plan(role, count)
    with col:
        st.markdown(f"""<div class="staff-card"><div class="staff-role">{role}</div><div class="staff-count">{count}</div><div class="staff-detail">{detail}</div></div>""", unsafe_allow_html=True)

# Helper for Uncertainty Calculation
today = datetime.now().date()
def get_confidence_badge(days):
    if days <= 1: return ('Hohe Sicherheit', 'conf-high')
    if days <= 3: return ('Mittlere Sicherheit', 'conf-med')
    return ('Geringe Sicherheit', 'conf-low')

# --- HEADER ---
c1, c2, c3 = st.columns([3, 1, 0.5])
with c1:
    st.title("TraumGmbH Analytics")
    st.caption(f"Angemeldet als: {st.session_state.get('username', 'Unknown')}")

with c2:
    if st.button("↻ Daten aktualisieren"):
        refresh_data()
with c3:
    if st.button("Logout"):
        logout()

# --- DATA PREP ---
df = get_forecast_view(days_ahead=7)

if df.empty:
    st.warning("Keine Daten verfügbar.")
    st.stop()

df['is_weekend'] = pd.to_datetime(df['datum']).dt.dayofweek.isin([4, 5, 6])
df['staff_plan'] = df.apply(lambda x: calculate_staffing(x['total_guests'], x['is_weekend']), axis=1)
df['revenue'] = df.apply(calculate_revenue, axis=1)
df['days_diff'] = df['datum'].apply(lambda x: (x - today).days)
df['opacity'] = df['days_diff'].apply(lambda d: max(0.4, 1.0 - (d * 0.12))) 


# --- 1. TAGES-FOKUS & PERSONAL ---
st.markdown("### 1. Tages-Fokus & Personalplanung")

# Default auf "Morgen" (Index 1) oder "Heute" (Index 0)
default_idx = 1 if len(df) > 1 else 0
selected_date_str = st.select_slider(
    "Tag auswählen:",
    options=[d.strftime('%Y-%m-%d') for d in df['datum']],
    value=df.iloc[default_idx]['datum'].strftime('%Y-%m-%d'), 
    format_func=lambda x: f"{pd.to_datetime(x).strftime('%A, %d.%m.')}"
)

# Filter Data for Selected Day
row = df[df['datum'].astype(str) == selected_date_str].iloc[0]
guests = int(row['total_guests'])
res = int(row['reservations'])
walk = int(row['walkins_pred'])
rev = row['revenue']
is_mon = row['datum'].weekday() == 0
avg_check = get_revenue_per_head(is_mon)

# KPI Cards
k1, k2, k3, k4 = st.columns(4)

# FIX: HTML string ohne Einrückung/Newline am Anfang bauen
def focus_metric_html(label, value, subtext, progress_val=None):
    progress_html = ""
    if progress_val is not None:
        width = min(100, int(progress_val * 100))
        progress_html = f'<div class="compact-progress"><div class="compact-progress-bar" style="width: {width}%; background-color: {PRIMARY_COLOR};"></div></div><div style="font-size: 0.75rem; color: #888; text-align: right; margin-top: 2px;">{width}% Kapazität</div>'
    
    return f'<div class="focus-card"><div class="focus-label">{label}</div><div class="focus-value">{value}</div><div class="focus-sub">{subtext}</div>{progress_html}</div>'

capacity = 350
load = min(1.0, guests/capacity)

with k1: st.markdown(focus_metric_html("Umsatz Prognose", f"{rev:,.0f} €", f"Ø {avg_check} €/Kopf"), unsafe_allow_html=True)
with k2: st.markdown(focus_metric_html("Gäste Gesamt", f"{guests}", f"{res} Res. | {walk} Lauf."), unsafe_allow_html=True)
with k3: st.markdown(focus_metric_html("Wetter", f"{row['temp']:.1f}°C", "Regen" if row['rain'] > 2 else "Trocken"), unsafe_allow_html=True)
with k4: st.markdown(focus_metric_html("Auslastung", f"{int(load*100)}%", f"{guests} von {capacity} Plätzen", progress_val=load), unsafe_allow_html=True)

# Personalplanung direkt darunter
st.markdown("##### Personalbedarf") 

plan = row['staff_plan']
c1, c2, c3, c4, c5 = st.columns(5)
staff_box(c1, "Küche", plan["Küche"])
staff_box(c2, "Pizza", plan["Pizza"])
staff_box(c3, "Bar", plan["Bar"])
staff_box(c4, "Service", plan["Service"])
staff_box(c5, "Runner", plan["Runner"])


# --- 2. TREND VORSCHAU ---
st.markdown("---")
st.markdown("### 2. Trend Vorschau")

selected_date_obj = row['datum']
next_days_mask = (df['datum'] > selected_date_obj) & (df['datum'] <= selected_date_obj + timedelta(days=3))
next_days = df[next_days_mask].copy()

if next_days.empty:
    st.info("Keine weiteren Zukunftsdaten verfügbar.")
else:
    cols = st.columns(3)
    for i, (index, r_next) in enumerate(next_days.head(3).iterrows()):
        conf_text, conf_class = get_confidence_badge(r_next['days_diff'])
        with cols[i]:
            st.markdown(f"""<div class="day-card"><div class="day-header">{r_next['wochentag']}</div><div class="day-date">{r_next['datum'].strftime('%d.%m.')}</div><div style="font-size: 1.5rem; font-weight: bold; color: #1a1a1a;">{int(r_next['total_guests'])} <span style="font-size: 0.8rem; color: #666;">Gäste</span></div><div style="margin: 5px 0; font-size: 0.8rem;">{r_next['revenue']:,.0f} €</div><div style="font-size: 0.8rem; color: #555;">{r_next['temp']:.0f}°C | {'🌧️' if r_next['rain'] > 2 else '☀️'}</div><span class="confidence-badge {conf_class}">{conf_text}</span></div>""", unsafe_allow_html=True)


# --- 3. WOCHENTREND (Charts) ---
st.markdown("---")
st.markdown("### 3. Wochentrend")

tab1, tab2 = st.tabs(["Chart", "Tabelle"])

with tab1:
    fig = go.Figure()
    
    res_colors = [f"rgba(44, 62, 80, {op})" for op in df['opacity']]
    walk_colors = [f"rgba(149, 165, 166, {op})" for op in df['opacity']]
    
    fig.add_trace(go.Bar(
        x=df['display_date'], y=df['reservations'], name='Reservierungen',
        marker_color=res_colors,
        hovertemplate='%{y} Reservierungen<extra></extra>'
    ))
    fig.add_trace(go.Bar(
        x=df['display_date'], y=df['walkins_pred'], name='Walk-Ins (Prognose)',
        marker_color=walk_colors,
        hovertemplate='%{y} Laufkundschaft<extra></extra>'
    ))
    fig.add_trace(go.Scatter(
        x=df['display_date'], y=df['revenue'], name='Umsatz (€)',
        yaxis='y2', line=dict(color=PRIMARY_COLOR, width=3, dash='dot')
    ))

    fig.update_layout(
        barmode='stack',
        height=400,
        margin=dict(t=20, b=0, l=0, r=0),
        paper_bgcolor='white',
        plot_bgcolor='white',
        yaxis=dict(title="Anzahl Gäste", showgrid=True, gridcolor='#eee'),
        yaxis2=dict(title="Umsatz €", overlaying='y', side='right', showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        hovermode="x unified"
    )
    
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    tbl = df.copy()
    for role in ["Küche", "Pizza", "Bar", "Service", "Runner"]:
        tbl[role] = tbl['staff_plan'].apply(lambda x: x[role])
    st.dataframe(
        tbl[['datum', 'wochentag', 'revenue', 'total_guests', 'reservations', 'walkins_pred', 'Küche', 'Bar', 'Service']],
        column_config={
            "datum": "Datum", "revenue": st.column_config.NumberColumn("Umsatz €", format="%.0f €"),
            "total_guests": "Gäste", "reservations": "Res.", "walkins_pred": "Lauf."
        }, hide_index=True, use_container_width=True
    )