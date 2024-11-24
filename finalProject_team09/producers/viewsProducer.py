# viewsProducer.py

from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_view_event():
    return {
        'event_type': 'view',
        'timestamp': datetime.now().isoformat(),
        'user_id': random.randint(1, 100),
        'page_id': random.choice(['home', 'product', 'checkout']),
        'view_duration': random.uniform(0.5, 10.0)
    }


with open('view_events_log.txt', 'a') as log_file:
    while True:
        event = generate_view_event()
        producer.send('topic_page_views', value=event)

        # Write the event in .txt file
        log_file.write(f"{json.dumps(event)}\n")

        log_file.flush()

        print(f"Sent and logged: {event}")
        time.sleep(1)