from kafka import KafkaProducer
import argparse
import json
import time
from datetime import datetime
import random

# Function to simulate exchange rate fluctuation
def generate_exchange_rate(base_rate):
    fluctuation = random.uniform(-0.05, 0.05)  # ±5% fluctuation
    return round(base_rate * (1 + fluctuation), 2)

# Producer function
def run_producer(kafka_server, topic, base_rate):
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        print(f"Producing messages to Kafka topic: {topic}")
        while True:
            exchange_rate = generate_exchange_rate(base_rate)
            message = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'currency_pair': topic.split('-')[-1].upper(),
                'rate': exchange_rate
            }
            producer.send(topic, message)
            print(f"Sent: {message}")
            time.sleep(4)

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer for Exchange Rates")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    parser.add_argument('--kafka-topic', required=True, help="Kafka topic to publish")
    parser.add_argument('--base-rate', type=float, required=True, help="Base exchange rate")
    args = parser.parse_args()

    run_producer(args.kafka_bootstrap, args.kafka_topic, args.base_rate)
