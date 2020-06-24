#!/usr/bin/env  python3
import argparse
import asyncio
import normalize
import websockets
from kafka import KafkaConsumer

def by_kafka():
    consumer = KafkaConsumer('TutorialTopic', bootstrap_servers='10.8.0.1:9092')
    
    for msg in consumer:
        items = []
        item = msg.value.decode("utf-8")[1:-1].replace("'",'').replace(', ',',').split(',')
        #items should be list of tuples or list of lists
        items.append(item)
        #print(type(msg.value.decode("utf-8")))
        normalize.write_data_mysql(items)




def by_socket():
    #processing for db
    normalize.prepare_db()
    print("Database is ready, waiting for connections on ws://10.8.0.1:9991")
    
    start_server = websockets.serve(get_information, "10.8.0.1", 9991)
    asyncio.get_event_loop().run_until_complete(start_server)
    asyncio.get_event_loop().run_forever()

    

async def get_information(websocket, path):
    try:
        while True:
            items = []
            string = await websocket.recv()
            item = string[1:-1].replace("'",'').replace(', ',',').split(',')
            #items should be list of tuples or list of lists
            items.append(item)
            #print(items)
            normalize.write_data_mysql(items)
    except:
        pass

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Import data to mysql')
    parser.add_argument('--kafka', help='Import by kafka', action="store_true")
    parser.add_argument("--socket", help="Import by socket", action="store_true")
    args = parser.parse_args()
    
    #processing for db
    normalize.prepare_db()
    print("Database is ready, waiting for messages from kafka on 10.8.0.1")
    
    if args.kafka:
    	by_kafka()
    elif args.socket:
    	by_socket()
    else:
    	print("Add some startup options")
