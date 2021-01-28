#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = "chicago_live"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

# f = open("dataset.csv", "r")
# for line in f:
#   words = line.split('\t', 1)
#   producer.send(TOPIC, key=bytes(words[0], 'utf-8'), value=bytes(words[1], 'utf-8'))
#   time.sleep(0.5)