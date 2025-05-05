from confluent_kafka import Producer
import requests, json, time

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce(topic, config):
    producer = Producer(config)
    url = 'https://opensky-network.org/api/states/all'

    while True:
        try:
            res = requests.get(url)
            if res.status_code == 200:
                data = res.json().get('states', [])
                for flight in data[:10]:
                    payload = json.dumps({
                        'icao24': flight[0],
                        'callsign': flight[1],
                        'origin_country': flight[2],
                        'longitude': flight[5],
                        'latitude': flight[6],
                        'velocity': flight[9]
                    })
                    producer.produce(topic, value=payload.encode('utf-8'))
                    producer.poll(0)
                    print(f"Produced flight: {flight[0]}")
        except Exception as e:
            print("Error fetching or sending data:", e)
        time.sleep(10)

def main():
    config = read_config()
    topic = "kafka_flight"
    produce(topic, config)

if __name__ == "__main__":
    main()