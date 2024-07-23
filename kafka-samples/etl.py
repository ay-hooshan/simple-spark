# your kafka topic should have some message at first of running

from confluent_kafka import Producer, Consumer, KafkaException
import sys
import json

broker = 'localhost:9094'
consume_topic_name = 'test1'
produce_topic_name = 'transformed'

producer_config = {
    "bootstrap.servers": broker,
}

consumer_config = {
    'bootstrap.servers': broker,
    'group.id': 'main',
    'auto.offset.reset': 'earliest',
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

def transform(messge: str):
    return messge.upper()

def load_transformed(message: str):
    producer.produce(produce_topic_name, message, callback=delivery_report)
    producer.flush()
    
def etl():
    consumer = Consumer(**consumer_config)

    consumer.subscribe([consume_topic_name])

    try:
        while True:
            response = consumer.poll(timeout=1.0)
            if response is None:
                continue
            if response.error():
                if response.error().code():
                    sys.stderr.write(f"Reached end of partition {response.partition()}\n")
                    break
                else:
                    raise KafkaException(response.error())
            else:
                message = transform(response.value())
                load_transformed(message)
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        consumer.close()
        
etl()
