#!/usr/bin/env  python3
from normalize import write_data_mysql
from kafka import KafkaConsumer


consumer = KafkaConsumer('TutorialTopic', bootstrap_servers='10.8.0.1:9092')
for msg in consumer:
    item = []
    item.append(tuple(msg.value.decode("utf-8").split(',')))
    write_data_mysql(item)
    #print(item)
