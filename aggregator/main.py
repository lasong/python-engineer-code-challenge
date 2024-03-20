
from consumer import Consumer

if __name__ == "__main__":
    kafka_consumer = Consumer(server='localhost:9094', topic='data', group_id='my-personal-group', chunk_size=100)

    for message in kafka_consumer.messages_chunk():
        print(message)
