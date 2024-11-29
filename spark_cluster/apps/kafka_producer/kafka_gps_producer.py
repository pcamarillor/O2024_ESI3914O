import random 
from datetime import datetime
import time
import json
import argparse
from kafka import KafkaProducer


## Función para generar coordenadas GPS aleatorias
def generate_random_location():
    
    lat_min = 14.5  # Latitud mínima de México
    lat_max = 32.72  # Latitud máxima de México
    lon_min = -118.5  # Longitud mínima de México
    lon_max = -86.75  # Longitud máxima de México
    
    latitude = random.uniform(lat_min, lat_max)
    longitude = random.uniform(lon_min, lon_max)
    speed = random.uniform(0, 180)  # Velocidad aleatoria en km/h (ejemplo: 0-120 km/h)
    
    return {
        'latitude': latitude,
        'longitude': longitude,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'speed': speed
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
            # Generate random location data
            location_data = generate_random_location()
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, location_data)
            print(f"Sent: {location_data}")

            
            

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        # Close the Kafka producer
        producer.close()


