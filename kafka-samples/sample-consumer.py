from confluent_kafka import Consumer, KafkaException
import sys

broker = 'localhost:9094'
topic_name = 'test1'

conf = {
    'bootstrap.servers': broker,
    'group.id': 'akbar',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(**conf)

consumer.subscribe([topic_name])

try:
    while True:

        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                sys.stderr.write(f"Reached end of partition {msg.partition()}\n")
                break
            else:
                raise KafkaException(msg.error())
        else:
            print(f"Key: {msg.key()}, Value: {msg.value()}")
except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')
finally:
    consumer.close()
