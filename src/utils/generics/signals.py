from src.utils.brokers.producer import Producer
from src.utils.connectors.postgres import PostgreConnector
import json


def on_task_completed(data, table_name, kafka_conf):
    """
    This function will update the database and send a kafka message so that the transformation will be started.
    """
    connector = PostgreConnector()
    connector.update_data_in_db(data, table_name)
    kafka_producer = Producer(bootstrap_servers=[kafka_conf['ip']])
    # kafka_producer = Producer(bootstrap_servers=['192.168.56.101:9092'])

    kafka_producer.produce_message(
        topic=kafka_conf['topic'], key="id".encode('utf8'), value=json.dumps(data).encode('utf8'))


def save_df_to_parquet(df, data_location):
    df.to_parquet(data_location,  engine='fastparquet')
