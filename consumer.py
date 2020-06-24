#!/usr/bin/env  python3
from normalize import write_data_mysql
from kafka import KafkaConsumer
import mysql.connector



consumer = KafkaConsumer('TutorialTopic', bootstrap_servers='10.8.0.1:9092')
for msg in consumer:
   write_data_mysql(msg.value.decode("utf-8"))
   print("msg.value.decode('utf-8')")
