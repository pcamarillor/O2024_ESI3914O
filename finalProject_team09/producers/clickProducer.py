from kafka import KafkaProducer
from datetime import datetime
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_click_event():
    return {
        'event_type': 'click',
        'timestamp': datetime.now().isoformat(),
        'user_id': random.randint(1, 100),
        'page_id': random.choice(['home', 'product', 'checkout']),
        'click_count': random.randint(1, 10)
    }

while True:
    event = generate_click_event()
    producer.send('topic_user_clicks', value=event)
    print(f"Sent: {event}")
    time.sleep(1)