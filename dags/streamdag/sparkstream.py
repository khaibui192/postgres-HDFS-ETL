from pyspark.sql import SparkSession, Row, Column
import math
from typing import Union
from pyspark.sql.functions import col, spark_partition_id, monotonically_increasing_id, from_json
from pyspark.sql.types import TimestampType, DateType ,StructField, StringType, StructType
from pyspark.sql.dataframe import DataFrame
import datetime
from dotenv import load_dotenv
import logging 
from cassandra.cluster import Cluster
driver = """org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3,\
com.datastax.spark:spark-cassandra-connector_2.13:3.5.1"""
            
spark = SparkSession.Builder().appName("Pyspark")\
    .config("spark.jars.packages", f"{driver}")\
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.files.maxPartitionBytes", "128MB")\
    .config("spark.executor.instances", "2")\
    .config('spark.cassandra.connection.host','localhost')\
    .getOrCreate()

class Streaming:
    def __init__(self):
        pass
    
    # Define the schema of your Kafka JSON data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    def readKafka(self):
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
            
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")
        return parsed_df
    
    def create_cassandra_connection(self):
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        return session
    
    def create_keyspace(self, session):
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        
    def create_table(self, session):
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """)

    
    def write(self):
        session = self.create_cassandra_connection()
        self.create_keyspace(session)
        self.create_table(session)
        parsed_df = self.readKafka()
        logging.info("running")
        query = parsed_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "spark_streams") \
            .option("table", "created_users") \
            .outputMode("append") \
            .start()
        query.awaitTermination()
