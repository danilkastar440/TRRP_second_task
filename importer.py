#!/usr/bin/env  python3
import normalize
from kafka import KafkaConsumer


consumer = KafkaConsumer('TutorialTopic', bootstrap_servers='10.8.0.1:9092')

#processing for db
normalize.prepare_db()
print("Database ready to insert")


for msg in consumer:
    item = []
    item.append(tuple(msg.value.decode("utf-8").split(',')))
    normalize.write_data_mysql(item)
    #print(item)
