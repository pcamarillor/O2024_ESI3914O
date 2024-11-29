# File: tweet_producer.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import argparse
import string


def generate_random_tweet():
    words = [
        "".join(random.choices(string.ascii_lowercase, k=random.randint(3, 10)))
        for _ in range(random.randint(5, 20))
    ]
    tweet = " ".join(words)
    return tweet[:255]  # Ensure the tweet is less than 256 characters


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tweet Producer Kafka arguments")
    parser.add_argument(
        "--kafka-bootstrap", required=True, help="Kafka bootstrap server"
    )
    parser.add_argument("--kafka-topic", required=True, help="Kafka topic to suscribe")
    args = parser.parse_args()

    KAFKA_SERVER = "{0}:9093".format(args.kafka_bootstrap)
    TOPIC = args.kafka_topic

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    try:
        print(f"Producing messages to Kafka topic: {TOPIC}")
        while True:
            message_data = {
                "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": random.randint(1, 100),
                "content": generate_random_tweet(),
            }

            producer.send(TOPIC, message_data)
            print(f"Sent to {TOPIC}: {message_data}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        producer.close()