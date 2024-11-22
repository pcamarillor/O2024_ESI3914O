from kafka import KafkaProducer
import argparse
import json
import time
from datetime import datetime
import random

# Function to generate sensor data
def generate_sensor_data():
    sensor_ids = ['sensor1', 'sensor2', 'sensor3']
    return {
        'sensor_id': random.choice(sensor_ids),
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'temperature': round(random.uniform(18.0, 30.0), 2)
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
            sensor_data = generate_sensor_data()
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
