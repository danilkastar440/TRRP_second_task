#!/usr/bin/env  python3
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='10.8.0.1:9092', api_version = (1,0,0))

f = open('baza.txt','r')

for item in f:
    #print(bytes(item[:-1], 'utf-8'))
    producer.send('TutorialTopic', bytes(item[:-1],'utf-8'))
producer.close()
