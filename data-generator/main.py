import pandas as pd
import json
import datetime as dt
from time import sleep
from kafka import KafkaProducer
import os
import time

file = "online_retail_II.csv"

# Initialize Kafka Producer Client
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092']
    )

interval = 0.1

print('Initialized Kafka producer at {}'.format(dt.datetime.utcnow()))

# Set a basic counter as the message key
counter = 0

while True:
    print("Starting file restream...")
    for index, chunk in pd.read_csv(file,encoding='latin1').iterrows():

	    # For each chunk, convert the invoice date into the correct time format
        chunk["InvoiceDate"] = time.time()

        key = "invoice".encode()

        # Convert the data frame chunk into a dictionary including the index
        chunkd = chunk.to_dict()

        print(chunkd)

        # Encode the dictionary into a JSON Byte Array
        data = json.dumps(chunkd, default=str).encode('utf-8')

        # Send the data to Kafka
        producer.send(topic="data", key=key, value=data)
        counter = counter + 1

        # Sleep to simulate a real time interval
        sleep(interval)
        print(f'Sent record to topic at time {dt.datetime.utcnow()}')
