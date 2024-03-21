from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
import time
import json

class Consumer:
    def __init__(self, server: str, topic: str) -> None:
        self.topic = topic
        self.chunk_size = 50
        self.chunk_timeout = 5000 # in miliseconds
        self.last_message = None

        self.consumer = KafkaConsumer(
            self.topic,
            group_id='data-aggregator',
            bootstrap_servers=[server],
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=False,  # We'll manually commit offsets
            value_deserializer=lambda mes: json.loads(mes.decode('utf-8')),
            max_poll_records=self.chunk_size
        )

    def messages_chunk(self) -> list[dict]:
        batch = self.consumer.poll(timeout_ms=self.chunk_timeout)

        # Flatten the batch into a list of messages
        messages = [msg for _, msgs in batch.items() for msg in msgs]

        if messages:
            self.last_message = messages[-1]

        return [msg.value for msg in messages]

    def commit(self) -> None:
        if self.last_message:
            last_topic_partition = TopicPartition(self.last_message.topic, self.last_message.partition)
            last_offset_and_metadata = OffsetAndMetadata(self.last_message.offset + 1, None)
            self.consumer.commit({ last_topic_partition: last_offset_and_metadata })

    def close(self) -> None:
        self.consumer.close()
