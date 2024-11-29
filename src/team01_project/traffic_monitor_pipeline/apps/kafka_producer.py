from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import argparse

def generate_random_ip():
    return ".".join(str(random.randint(0, 255)) for _ in range(4))

def generate_random_network_event():
    protocols = ["TCP", "UDP", "ICMP"]
    event = {
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_ip": generate_random_ip(),
        "destination_ip": generate_random_ip(),
        "protocol": random.choice(protocols),
        "source_port": random.randint(1024, 65535),
        "destination_port": random.randint(1024, 65535),
        "data_transferred": random.randint(100, 100000),
    }
    return event

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Network Traffic Producer Kafka arguments")
    parser.add_argument(
        "--kafka-bootstrap", required=True, help="Kafka bootstrap server"
    )
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to publish")
    args = parser.parse_args()

    KAFKA_SERVER = f"{args.kafka_bootstrap}:9093"
    TOPIC = args.kafka_topic

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        print(f"Producing network traffic events to Kafka topic: {TOPIC}")
        while True:
            event_data = generate_random_network_event()
            producer.send(TOPIC, event_data)
            print(f"Sent to topic {TOPIC}: {event_data}")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Message production stopped.")

    finally:
        producer.close()
