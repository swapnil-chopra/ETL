from kafka import KafkaConsumer
from kafka.errors import KafkaError


class Consumer:
    def __init__(self, bootstrap_servers, group_id):
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            # enable_auto_commit=False,
            auto_offset_reset='earliest'
        )

    def process_message(self, msg):
        pass

    def consume_messages(self, topics):
        try:
            self.consumer.subscribe(topics)
            for msg in self.consumer:
                self.process_message(msg)

        except KafkaError as ke:
            print(f"error-: {ke}")

    def close_consumer(self):
        self.consumer.close()


class TransformationConsumer(Consumer):
    def __init__(self, bootstrap_servers):
        self.group_id = 'transform'
        super().__init__(bootstrap_servers, self.group_id)

    def process_message(self, msg):
        print(msg)
        return super().process_message(msg)
