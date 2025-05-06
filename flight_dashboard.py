import streamlit as st
import pandas as pd
import snowflake.connector
import pydeck as pdk
import time

st.set_page_config(page_title="Flight Tracker", layout="wide")

st.title("🛫 Real-Time Flight Tracker")

sf = st.secrets["snowflake"]

conn = snowflake.connector.connect(
    user=sf.user,
    password=sf.password,
    account=sf.account,
    warehouse=sf.warehouse,
    role=sf.role,
    database=sf.database,
    schema=sf.schema
)

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

with st.sidebar:
    st.header("✈️ Filter Flights")
    min_velocity = st.slider("Min Speed (m/s)", 0, 300, 0)
    country = st.text_input("Origin Country (optional)")

df = df[df['VELOCITY'] >= min_velocity]
if country:
    df = df[df['ORIGIN_COUNTRY'].str.lower() == country.lower()]

st.caption(f"Currently tracking {len(df)} active flights.")

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

st.subheader("Flight Data")
st.dataframe(df.drop(columns=['color'], errors='ignore'))

st.subheader("⚡ Top 5 Fastest Flights")
st.dataframe(
    df.nlargest(5, 'VELOCITY')[['CALLSIGN', 'ORIGIN_COUNTRY', 'VELOCITY']],
    use_container_width=True
)
