import streamlit as st
import pandas as pd
import snowflake.connector
import pydeck as pdk
import time

st.set_page_config(page_title="Flight Tracker", layout="wide")

st.title("🛫 Real-Time Flight Tracker")

# Load credentials from Streamlit secrets
sf = st.secrets["snowflake"]

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf.user,
    password=sf.password,
    account=sf.account,
    warehouse=sf.warehouse,
    role=sf.role,
    database=sf.database,
    schema=sf.schema
)

# Query live data from Snowflake
query = """
SELECT
  RECORD_CONTENT:icao24::string AS icao24,
  RECORD_CONTENT:callsign::string AS callsign,
  RECORD_CONTENT:origin_country::string AS origin_country,
  RECORD_CONTENT:longitude::float AS lon,
  RECORD_CONTENT:latitude::float AS lat,
  RECORD_CONTENT:velocity::float AS velocity
FROM kafka_flight
WHERE RECORD_CONTENT:latitude IS NOT NULL AND RECORD_CONTENT:longitude IS NOT NULL
LIMIT 200;
"""

df = pd.read_sql(query, conn)

# Sidebar filters
with st.sidebar:
    st.header("✈️ Filter Flights")
    min_velocity = st.slider("Min Speed (m/s)", 0, 300, 0)
    country = st.text_input("Origin Country (optional)")

# Apply filters
df = df[df['VELOCITY'] >= min_velocity]
if country:
    df = df[df['origin_country'].str.lower() == country.lower()]

# Caption
st.caption(f"Currently tracking {len(df)} active flights.")

# Pydeck map with velocity-based coloring
if not df.empty:
    df['color'] = df['VELOCITY'].apply(lambda v: [255, 255 - min(v * 5, 255), min(v * 5, 255)])

    st.pydeck_chart(pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=df['LAT'].mean(),
            longitude=df['LON'].mean(),
            zoom=3,
            pitch=0,
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=df,
                get_position='[LON, LAT]',
                get_color='color',
                get_radius=20000,
                pickable=True,
                opacity=0.8
            ),
        ],
    ))
else:
    st.warning("No flights match your filters.")

# Optional table
st.subheader("Flight Data")
st.dataframe(df.drop(columns=['color'], errors='ignore'))

# Top 5 fastest flights
st.subheader("⚡ Top 5 Fastest Flights")
st.dataframe(
    df.nlargest(5, 'VELOCITY')[['CALLSIGN', 'ORIGIN_COUNTRY', 'VELOCITY']],
    use_container_width=True
)
