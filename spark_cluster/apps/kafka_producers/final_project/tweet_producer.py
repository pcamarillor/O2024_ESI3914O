# File: tweet_producer.py
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random
import argparse
import string

common_words = [
    "the",
    "be",
    "to",
    "of",
    "and",
    "a",
    "in",
    "that",
    "have",
    "I",
    "it",
    "for",
    "not",
    "on",
    "with",
    "as",
    "you",
    "do",
    "at",
    "this",
    "but",
    "his",
    "by",
    "from",
    "they",
    "we",
    "say",
    "her",
    "she",
    "or",
    "an",
    "will",
    "my",
    "one",
    "all",
    "would",
    "there",
    "their",
    "what",
    "so",
    "up",
    "out",
    "if",
    "about",
    "who",
    "get",
    "which",
    "go",
    "me",
    "president",
]


def generate_random_tweet():
    words_count = random.randint(5, 20)
    words = random.choices(common_words, k=words_count)

    # Capitalize the first word
    words[0] = words[0].capitalize()

    # Randomly add punctuation
    if random.random() < 0.2:  # 20% chance to add a period
        words[-1] += "."

    tweet = " ".join(words)
    return tweet[:255]


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
                "user_id": random.randint(1, 10),
                "content": generate_random_tweet(),
            }

            producer.send(TOPIC, message_data)
            print(f"Sent to {TOPIC}: {message_data}")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        producer.close()
