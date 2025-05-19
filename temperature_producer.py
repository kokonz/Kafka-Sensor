import json
import random
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    for gudang in gudangs:
        suhu = random.randint(70, 90)
        data = {"gudang_id": gudang, "suhu": suhu}
        producer.send('sensor-suhu-gudang', value=data)
        print(f"Sent temperature data: {data}")
    time.sleep(1)
