from kafka import KafkaConsumer, OffsetAndMetadata
import json

class Consumer:
    def __init__(self, server: str, topic: str, last_message: dict = None) -> None:
        self.topic = topic
        self.chunk_size = 50
        self.chunk_timeout = 5000 # in miliseconds
        self.last_message = last_message

        self.consumer = KafkaConsumer(
            group_id='data-aggregator',
            bootstrap_servers=[server],
            auto_offset_reset='earliest',  # Start reading at the earliest message
            enable_auto_commit=False,  # We'll manually commit offsets
            value_deserializer=lambda mes: json.loads(mes.decode('utf-8')),
            max_poll_records=self.chunk_size
        )

        self.consumer.subscribe([self.topic])

    def messages_chunk(self) -> list[dict]:
        batch = self.consumer.poll(timeout_ms=self.chunk_timeout)

        # Flatten the batch into a list of messages
        messages = [msg for _, msgs in batch.items() for msg in msgs]

        if messages:
            last_message = messages[-1]
            self.last_message = {
                'topic': last_message.topic,
                'partition': last_message.partition,
                'offset': last_message.offset
            }

        return [msg.value for msg in messages]

    def set_offset(self) -> None:
        print(f'Consumer: Last message info - {self.last_message}')
        if self.last_message:

            # Check if there are assigned partitions that need offset adjustment before setting offset
            for topic_partition in self.consumer.assignment():
                topic = self.last_message['topic']
                partition = self.last_message['partition']
                if topic_partition.topic == topic and topic_partition.partition == partition:
                    self.consumer.commit({topic_partition: OffsetAndMetadata(self.last_message['offset'] + 1, None)})
                    break

    def close(self) -> None:
        self.consumer.close()
