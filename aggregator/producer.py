from kafka import KafkaProducer
import json
import time

class Producer:
    MAX_RETRIES = 3

    def __init__(self, topic: str, server: str) -> None:
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda mes: json.dumps(mes).encode('utf-8')
        )

    def send_message(self, message: dict) -> None:
        retries = self.MAX_RETRIES
        while retries > 0:
            try:
                self.producer.send(self.topic, value=message).get(timeout=10)  # Wait for send confirmation
                print(f'Producer: Sent message')
                break  # Exit retry loop on success
            except Exception as e:
                print(f'Producer: Error sending message, retries left: {retries - 1}. Error: {e}')
                retries -= 1
                time.sleep(2)
        if retries == 0:
            print(f'Producer: Failed to send message after retries. Message dropped.')
