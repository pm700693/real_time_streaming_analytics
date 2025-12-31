# Simple Kafka producer (example)
from kafka import KafkaProducer
import json, time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

if __name__ == '__main__':
    for i in range(100):
        event = {'id': i, 'value': i*2}
        producer.send('events', event)
        print('sent', event)
        time.sleep(0.1)
