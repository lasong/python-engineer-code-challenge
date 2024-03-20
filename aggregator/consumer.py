from kafka import KafkaConsumer
import time
import json

class Consumer:
    def __init__(self, server: str, topic: str, group_id: str, chunk_size: int) -> None:
        self.consumer = KafkaConsumer(
            topic,
            group_id=group_id,
            bootstrap_servers=[server],
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=False,  # We'll manually commit offsets
        )
        self.chunk_size = chunk_size
        self.chunk_timeout = 5  # Time in seconds to wait for filling the chunk

    def messages_chunk(self) -> list[dict]:
        messages = []
        start_time = time.time()

        for message in self.consumer:
            messages.append(json.loads(message.value))

            # Check if chunk is full or timeout has occurred
            if len(messages) >= self.chunk_size or (time.time() - start_time) >= self.chunk_timeout:
                self.consumer.commit()
                break

        return messages
