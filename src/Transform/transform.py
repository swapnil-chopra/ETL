# import pyspark
# from pyspark.sql import SparkSession
# spark: SparkSession = SparkSession.builder.appName('parquetFile').getOrCreate()
# parDF = spark.read.parquet(
#     r'P:\Projects\ETL Projects\ETL\src\parquet_files\data.parquet')
# parDF.createOrReplaceTempView('orders')
# df = parDF.limit(150).toPandas()
# df.to_excel('./scrap/sample_data.xlsx')
# spark.sql('select * from orders').show(4)

import pandas as pd
from src.utils.brokers.consumer import TransformationConsumer
from src.utils.application.settings import BOOTSTRAP_SERVERS
k = TransformationConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
k.consume_messages('transform')
