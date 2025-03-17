import json
import pandas as pd
from kafka import KafkaProducer
from time import time


# Read the csv file
cols = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']
df = pd.read_csv("green_tripdata_2019-10.csv", usecols=cols)


# Connect to Kafka Producer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

topic_name = "green-trips"


# Send data to Kafka and record the time
t0 = time()
for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)
    
    
# Ensure all data sent
producer.flush()
t1 = time()


# Calculate the time takes to send data
took = t1 - t0 
print(f"Took {took:.2f} seconds to send and flush the entire dataset.")
