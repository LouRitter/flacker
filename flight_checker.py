from confluent_kafka import Producer
import requests, json, time
import os

def read_config():
    return {
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ['KAFKA_USERNAME'],
        'sasl.password': os.environ['KAFKA_PASSWORD']
    }

def produce(topic, config):
    producer = Producer(config)
    url = 'https://opensky-network.org/api/states/all'
    sent_ids = set()  # Track recently sent flight IDs

    while True:
        try:
            res = requests.get(url)
            if res.status_code == 200:
                data = res.json().get('states', [])
                buffer = []
                for flight in data[:50]:  # Consider up to 50 flights
                    if not flight or flight[0] in sent_ids:
                        continue
                    payload = {
                        'icao24': flight[0],
                        'callsign': flight[1],
                        'origin_country': flight[2],
                        'longitude': flight[5],
                        'latitude': flight[6],
                        'velocity': flight[9]
                    }
                    buffer.append(payload)
                    sent_ids.add(flight[0])
                    if len(sent_ids) > 500:
                        sent_ids = set(list(sent_ids)[-250:])  # Trim memory

                for record in buffer:
                    producer.produce(topic, value=json.dumps(record).encode('utf-8'))
                    producer.poll(0)
                    print(f"Produced flight: {record['icao24']}")
                producer.flush()

        except Exception as e:
            print("Error fetching or sending data:", e)

        print("Sleeping for 10 minutes...")
        time.sleep(600)

def main():
    config = read_config()
    topic = "kafka_flight"
    produce(topic, config)

if __name__ == "__main__":
    main()
