from pyspark.sql import SparkSession, Row, Column
import math
from typing import Union
# from pyspark.sql.functions import regexp_replace, col, spark_partition_id, monotonically_increasing_id, rand, lit, expr
from pyspark.sql.types import TimestampType, DateType #StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.dataframe import DataFrame
import datetime
from dotenv import load_dotenv
import os 
from cassandra.cluster import Cluster
packages = """spark-cassandra-connector_2.13-3.5.1.jar:
            spark-sql-kafka-0-10_2.13-3.5.3.jar"""
            
spark = SparkSession.Builder().appName("Pyspark")\
    .config("spark.jars.packages", f"{packages}")\
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.files.maxPartitionBytes", "128MB")\
    .config("spark.executor.instances", "2")\
    .config('spark.cassandra.connection.host','localhost')\
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9090") \
    .getOrCreate()
    
class Streaming:
    
    def __init__(self):
        pass
    
    def connCassandra():
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        pass
