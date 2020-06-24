#!/usr/bin/env  python3
import asyncio
import sqlite3
import argparse
import normalize
import websockets
from kafka import KafkaProducer



def by_kafka(data):
    producer = KafkaProducer(bootstrap_servers='10.8.0.1:9092', api_version = (1,0,0))
    for item in data:
        #print(item)
        producer.send('TutorialTopic', bytes(str(item),'utf-8'))
    producer.close()

def by_socket(data):
    async def send_information():
        uri = "ws://10.8.0.1:9991"
        async with websockets.connect(uri) as websocket:
            for item in data:
                print(f'sent: {item}')
                #type(item) == tuple
                await websocket.send(str(item))
    asyncio.get_event_loop().run_until_complete(send_information())

if __name__=="__main__":
        parser = argparse.ArgumentParser(description='Import data to mysql')
        parser.add_argument('--kafka', help='Send by kafka', action="store_true")
        parser.add_argument("--socket", help="Send by socket", action="store_true")
        args = parser.parse_args()
        
        #list of tuples
        data = normalize.read_data_sqlite3("baza.db")
        if args.kafka:
            by_kafka(data)
        elif args.socket:
            by_socket(data)
        else:
            print("Add some startup options")
