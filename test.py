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

test_data = [
    {
        "x": "1", 
        "y": "2",
        "z": "3"
    }, 
    {
        "x": "7", 
        "y": "2",
        "z": "4"
    }, 
    {
        "x": "9", 
        "y": "2",
        "z": "1"
    }, 
    {
        "x": "3", 
        "y": "7",
        "z": "8"
    }, 
    {
        "x": "5", 
        "y": "8",
        "z": "0"
    },
]

for i in range(len(test_data)):
    producer.produce("testnum", json_serializer(test_data[i]), callback=delivery_report)
    producer.flush()
