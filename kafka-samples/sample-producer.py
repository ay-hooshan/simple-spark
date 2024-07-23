from confluent_kafka import Producer
import json

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

conf = {
    "bootstrap.servers": "localhost:9094",
}

producer = Producer(conf)

test_data = {
    "title": "new message", 
    "description": "something new for testing transformer"
}

for i in range(5):
    producer.produce("test1", json_serializer(test_data), callback=delivery_report)
    producer.flush()
