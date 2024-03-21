
from consumer import Consumer
from data_aggregator import DataAggregator

if __name__ == "__main__":
    kafka_consumer = Consumer(server='localhost:9094', topic='data')
    aggregator = DataAggregator(kafka_consumer.messages_chunk())

    for message in aggregator.aggregated_data():
        print(message)

    kafka_consumer.commit()
    kafka_consumer.close()
