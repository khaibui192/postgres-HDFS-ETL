import requests
from pyspark.sql import SparkSession, Row, Column
import yaml
from typing import Union
from pyspark.sql.dataframe import DataFrame
import datetime
from pyspark.sql.functions import udf, udtf, explode, lit, expr
from pyspark.sql.types import FloatType, StructType, StructField, \
                            ArrayType, TimestampType, StringType, IntegerType
import os
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("url")
host = os.getenv("host")
key = os.getenv("key")

with open('dags/apidag/config/config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.Loader)
    
driver = """/home/khai/Spark/spark-3.5.3-bin-hadoop3/jars/postgresql-42.7.4.jar:
            /home/khai/Spark/spark-3.5.3-bin-hadoop3/jars/mysql-connector-j-9.1.0.jar"""
            
spark = SparkSession.Builder().appName("Pyspark")\
    .config("spark.driver.extraClassPath", f"{driver}")\
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.files.maxPartitionBytes", "128MB")\
    .config("spark.executor.instances", "2")\
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9090") \
    .getOrCreate()


class transformData:
    
    schema = StructType({
        # StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("type", StringType(), True),
        StructField("last_update_utc", TimestampType(), True),
        StructField("price", FloatType(), True),
        StructField("change",FloatType(), True),
        StructField("change_percent",FloatType(), True),
        StructField("volume",IntegerType(), True) 
    })
    
    def __init__(self, url, symbol, period, key, host):
        self.url = url
        self.symbol = symbol
        self.period = period
        self.key = key
        self.host = host
    
    #fetch json data by API
    @udf(returnType=ArrayType(schema))
    def fetch_data(self, url, symbol, period, key, host):
        # endpoint = url
        params = {"symbol":f"{symbol}","period":f"{period}","language":"en"}
        header = {
            "x-rapidapi-key": f"{key}",
            "x-rapidapi-host": f"{host}"
        }
        response = requests.get(url, headers=header, params=params).json()
        dataset = self.process_data(response)
        return dataset
    
    #process the json and get relevant data
    def process_data(self, response)-> list:
        data = response["data"]
        timeseries = data['time_series']
        dataset = []
        # label = ["symbol","type","last_update_utc","price","change","change_percent","volume"]
        for item in timeseries:
            if 'volume' not in timeseries[item]:
                timeseries[item].update({
                    "volume": None
                })
            feature = {
                "symbol": data['symbol'],
                "type":   data['type'],
                "last_update_utc":  datetime.strptime(item, "%Y-%m-%d %H:%M:%S"),
                "price":            timeseries[item]['price'],
                "change":           timeseries[item]['change'],
                "change_percent":   timeseries[item]['change_percent'],
                "volume":           timeseries[item]['volume']
            }
            dataset.append(feature)
        return dataset
    
    def framing_data(self) -> DataFrame:
        data = []
        for i in config['symbol']:
            data.append((url, i, config['period'], key, host))
            
        columns = ["endpoint", "symbol", "period", "key", "host"]
        df = spark.createDataFrame(data, columns)
        response_df = df.withColumn("response", self.fetch_data("endpoint", "symbol", "period", "key", "host"))
        results_df = response_df.select(explode("response").alias("feature")).select("feature.*")
        return results_df
    
    def __checkFolderExist(self,hdfs_path: str)->bool:

        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        exist = fs.exists(path)
        if not exist:
            permission = spark._jvm.org.apache.hadoop.fs.permission.FsPermission("777")
            fs.mkdirs(path, permission)
            
        status = fs.listStatus(path)
        if len(status) == 0:
            return False
        else:
            return True
            
    
    def write(self):
        
        pass