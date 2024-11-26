from kafka import KafkaProducer
import argparse
import json
import time
from datetime import datetime
import random


import pandas as pd
import random
from faker import Faker

# Initialize Faker
fake = Faker()

# Define potential values for categorical data
playback_qualities = ['1080p', '720p', '480p', '360p']
devices = ['Smartphone', 'Desktop', 'Smart TV', 'Tablet']
recommendation_options = ['Yes', 'No']
locations = [fake.city() + ", " + fake.country() for _ in range(10)]

# Function to generate sensor data
def generate_video_stream_data():
    return {
        "video_id": fake.uuid4(),
        "video_title": fake.sentence(nb_words=3),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "playback_quality": random.choice(playback_qualities),
        "buffering_duration": round(random.uniform(0, 15), 2),  # Buffering in seconds
        "engagement_duration": random.randint(30, 7200),  # Engagement in seconds (30s to 2 hours)
        "device_type": random.choice(devices),
        "recommendation_clicked": random.choice(recommendation_options),
        "viewer_location": random.choice(locations)
    }



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Kafka arguments")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    parser.add_argument('--kafka-topic', required=True, help="Kafka topic to suscribe")
    
    args = parser.parse_args()

    # Define Kafka server and topic
    KAFKA_SERVER = '{0}:9093'.format(args.kafka_bootstrap)
    KAFKA_TOPIC = args.kafka_topic


    # Initialize the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize data as JSON
    )

    # Produce data to Kafka topic
    try:
        print(f"Producing messages to Kafka topic: {KAFKA_TOPIC}")
        while True:
            # Generate random sensor data
            sensor_data = generate_video_stream_data()
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, sensor_data)
            print(f"Sent: {sensor_data}")
            
            # Sleep for a few seconds before sending the next message
            time.sleep(2)

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        # Close the Kafka producer
        producer.close()