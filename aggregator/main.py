import signal
import sys
from consumer import Consumer
from producer import Producer
from data_aggregator import DataAggregator
from application_state_db import ApplicationStateDB

consumer_topic = 'data'
producer_topic = 'agg'
kafka_server = 'localhost:9094'

# Signal handler function
def signal_handler(signal_received, frame):
    print('Signal received, shutting down gracefully.')
    kafka_consumer.close()
    state_db.close()
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handlers to catch SIGINT (Ctrl+C) and SIGTERM
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    state_db = ApplicationStateDB()
    kafka_consumer = Consumer(
        server=kafka_server,
        topic=consumer_topic,
        last_message=state_db.fetch_last_message(consumer_topic)
    )
    kafka_producer = Producer(topic=producer_topic, server=kafka_server)

    try:
        while True:
            kafka_consumer.set_offset()

            messages = kafka_consumer.messages_chunk()
            aggregator = DataAggregator(messages)

            for message in aggregator.aggregated_data():
                print(message)
                kafka_producer.send_message(message)

            state_db.add_message(consumer_topic, kafka_consumer.last_message)
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
        state_db.rollback()
    finally:
        kafka_consumer.close()
        state_db.close()
