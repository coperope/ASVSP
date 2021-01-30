#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
import json
import csv
from datetime import datetime

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "chicago_live"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

f = csv.reader(open("ppm_seonsors_data.csv"), delimiter=",")
headers = next(f, None)
print("Loaded data")

dataList = list(f)
print("Initiating temperature sensors data")

for i, line in enumerate(dataList):
    producer.send(TOPIC, key=bytes(line[0], 'utf-8'), value=line)
    time.sleep(1)