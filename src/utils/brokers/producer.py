from kafka import KafkaProducer
from kafka.errors import KafkaError


class Producer:
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    def produce_message(self, topic, key, value):
        try:
            future = self.producer.send(topic, key=key, value=value)
            record_metadata = future.get(timeout=10)
            print(
                f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")

        except KafkaError as ke:
            print(f"Error sending message to Kafka: {ke}")

    def close_producer(self):
        self.producer.close()


# if __name__ == "__main__":
#     producer_app = Producer(bootstrap_servers='localhost:9092')

#     topic = 'test-topic'
#     key = 'key'
#     value = 'Hello, Kafka!'

#     producer_app.produce_message(topic, key, value)
#     producer_app.close_producer()
