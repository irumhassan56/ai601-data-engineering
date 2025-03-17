import time
import random
import json
from kafka import KafkaProducer

# Kafka topic and producer setup
TOPIC = "music_events"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sample data
songs = [101, 202, 303, 404, 505]
regions = ["US", "EU", "APAC"]
actions = ["play", "skip", "like"]  

while True:
    event = {
        "song_id": random.choice(songs),
        "timestamp": time.time(),
        "region": random.choice(regions),
        "action": random.choice(actions), 
    }
    producer.send(TOPIC, event)
    print(f"Sent event: {event}")
    time.sleep(random.uniform(0.5, 2.0)) 
