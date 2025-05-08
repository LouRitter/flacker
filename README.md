# ✈️ Real-Time Flight Tracker

A real-time flight data streaming app powered by **OpenSky**, **Kafka**, **Confluent Cloud**, **Snowflake**, and **Streamlit**.

Built to demonstrate modern data architecture with live data
---

## 🧠 What It Does

- Pulls live aircraft data from the OpenSky API
- Streams flight records into Kafka using a Python producer
- Uses Confluent Cloud to deliver messages to Snowflake (via managed connector)
- Stores raw JSON data in Snowflake using `VARIANT` columns
- Visualizes flights in near real-time on an interactive Streamlit map

---

## 🛠️ Tech Stack

| Layer        | Tool                |
|--------------|---------------------|
| Source       | OpenSky API         |
| Ingestion    | Python + Kafka Producer |
| Transport    | Kafka (Confluent Cloud) |
| Storage      | Snowflake           |
| Visualization| Streamlit           |
| Hosting      | Fly.io (producer) + Streamlit Cloud |

---

## 🚀 How to Run Locally

### 1. Clone this repo

```bash
git clone https://github.com/your-user/flight-tracker.git
cd flight-tracker
