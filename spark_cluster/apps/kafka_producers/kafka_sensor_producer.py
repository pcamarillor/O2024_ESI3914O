from kafka import KafkaProducer
import argparse
import json
import time
from datetime import datetime
import random

# Function to generate network traffic data
def generate_network_traffic_data():
    protocols = ['TCP', 'UDP', 'ICMP']
    traffic_types = ['HTTP', 'HTTPS', 'DNS', 'FTP', 'SSH']
    
    return {
        'timestamp': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source_ip': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'destination_ip': f"10.0.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'source_port': random.randint(1024, 65535),
        'destination_port': random.randint(1, 65535),
        'protocol': random.choice(protocols),
        'packet_size_bytes': random.randint(64, 1500),
        'flow_id': f"flow-{random.randint(1000, 9999)}",
        'traffic_type': random.choice(traffic_types),
        'alerts': {
            'malicious_activity': random.choice([True, False]),
            'unusual_pattern': random.choice([True, False])
        }
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer for network traffic data")
    parser.add_argument('--kafka-bootstrap', required=True, help="Kafka bootstrap server")
    parser.add_argument('--kafka-topic', required=True, help="Kafka topic to publish to")
    
    args = parser.parse_args()

    # Define Kafka server and topic
    KAFKA_SERVER = f'{args.kafka_bootstrap}:9093'
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
            # Generate random network traffic data
            network_data = generate_network_traffic_data()
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, network_data)
            print(f"Sent: {network_data}")
            
            # Sleep for a few seconds before sending the next message
            time.sleep(2)

    except KeyboardInterrupt:
        print("Stopped producing messages.")

    finally:
        # Close the Kafka producer
        producer.close()
