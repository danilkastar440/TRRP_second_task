#!/usr/bin/env  python3
import asyncio
import argparse
import websockets
from kafka import KafkaProducer


f = open('baza.txt','r')

def by_kafka():
    producer = KafkaProducer(bootstrap_servers='10.8.0.1:9092', api_version = (1,0,0))
    for item in f:
        #print(bytes(item[:-1], 'utf-8'))
        producer.send('TutorialTopic', bytes(item[:-1],'utf-8'))
    producer.close()
    f.close()

def by_socket():
    async def send_information():
        uri = "ws://10.8.0.1:9991"
        async with websockets.connect(uri) as websocket:
            for item in f:
                print(f'sent: {item.rstrip()}')
                await websocket.send(item.rstrip())
    asyncio.get_event_loop().run_until_complete(send_information())

if __name__=="__main__":
        parser = argparse.ArgumentParser(description='Import data to mysql')
        parser.add_argument('--kafka', help='Send by kafka', action="store_true")
        parser.add_argument("--socket", help="Send by socket", action="store_true")
        args = parser.parse_args()

        if args.kafka:
                by_kafka()
        elif args.socket:
                by_socket()
        else:
                print("Add some startup options")
