# import pyspark
from pyspark.sql import SparkSession, Row, Column
import math
from typing import Union
# from pyspark.sql.functions import regexp_replace, col, spark_partition_id, monotonically_increasing_id, rand, lit, expr
from pyspark.sql.types import TimestampType, DateType #StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.dataframe import DataFrame
import datetime
from dotenv import load_dotenv
import os 

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

# load_dotenv("/home/khai/airflow/dags/.env")

# db = os.getenv("DB")
# password = os.getenv("PASSWORD")
# user = os.getenv("USR")
# hdfs = os.getenv("HDFS")

    
class transformData:
    
    def __init__(self, job:str, dbname:str, start_date: datetime, dbtable:str,
                    numPartitions, upperBound,lowerBound, partitionColumn:str, 
                    primaryKey_col:any, lastUpdate_col:str, url:str, 
                    user:str, password:str, hdfs:str):
        
        self.dbname = dbname
        self.job = job
        self.start_date = start_date
        self.dbtable = dbtable
        self.numPartitions = numPartitions
        self.lowerBound = lowerBound
        self.upperBound = upperBound
        self.partitionColumn = partitionColumn
        self.primaryKey_col = primaryKey_col
        self.lastUpdate_col = lastUpdate_col
        self.url = url
        self.user = user
        self.password = password
        self.hdfs = hdfs
        self.options = {
            "url": f"{self.url}{self.dbname}",
            "user": f"{self.user}",
            "password": f"{self.password}",
        }
    

    def getTotalRec(self):
        opts = self.options.copy()
        opts.update({
            "query": f"select count(*) from {self.dbtable}"
        })
        df = spark.read.format('jdbc').options(**opts).load()
        rec = df.collect()[0][0] # to take value cuz show is nonetype
        return rec
    
    # #BUILDING /////////////////////////////////////////// BUILDING
    # # auto check increment value
    # def checkAutoIncrement(self):
    #     if self.partitionColumn is not None: # ??
    #         return self.partitionColumn

    #     opts = self.options.copy()
    #     opts.update({
    #         "query": f"""
    #         select * from INFORMATION_SCHEMA.COLUMNS
    #         where TABLE_NAME = {self.dbtable}
    #         and 
    #         """
    #     })
    # #BUILDING /////////////////////////////////////////// BUILDING
    
    def read(self) -> DataFrame:
        totalRec = self.getTotalRec()
        step = int(1e4)
        if (isinstance(self.upperBound, str) and self.upperBound.lower() == 'max')\
            or self.upperBound is None:
                
            self.upperBound = totalRec
        
        if (isinstance(self.lowerBound, str) and self.lowerBound.lower() == 'min')\
            or self.lowerBound is None:
                
            self.lowerBound = 0
             
        if self.numPartitions is None:
            self.numPartitions = (self.upperBound-self.lowerBound)/step
            
        if self.lastUpdate_col is not None:
            opts = self.options.copy()
            opts.update({
                "query": f"""select * from {self.dbtable} 
                    where {self.lastUpdate_col} >= '{self.start_date}'""",
            })
            df = spark.read.format('jdbc').options(**opts).load()
            return df
        
        if self.partitionColumn is None :
            opts = self.options.copy()
            opts.update({
                # "dbtable": f"{self.dbtable}",
                "query":"""select mod(abs(hashtext(u1.id)),2) as modHashId, u2.* 
                            from uuiddata u1 full join uuiddata u2 on u1.id = u2.id""",
                "fetchsize": int(1e5)
            })
            df = spark.read.format('jdbc').options(**opts).load()
            return df
        
        opts = self.options.copy()
        opts.update({
            "dbtable": f"{self.dbtable}",
            "numPartitions": self.numPartitions,
            "partitionColumn":f"{self.partitionColumn}",
            "lowerBound": f"{self.lowerBound}",
            "upperBound": f"{self.upperBound}"
        })

        df = spark.read.format('jdbc').options(**opts).load()
        return df
  
    def getVol(self, opts:dict)-> int:
        """opts is options with query to count"""
        volume = spark.read.format('jdbc').options(**opts).load()
        volume = volume.collect()[0][0]
        files = math.ceil(volume/1e6)
        return files
    
    def filterDF(self, df: DataFrame) -> Union[DataFrame,list[DataFrame,str]]:
        if df is None:
            return None
        # no incremental column
        if self.lastUpdate_col is None and self.partitionColumn is None:
            return [df,'hashID']
        # no date 
        if self.lastUpdate_col is None:
            
            opts2 = self.options.copy()
            opts2.update({
            "query": f"""select count(*) from {self.dbtable}""",
            })
            files = self.getVol(opts2)
            filterdfs = df.repartition(files)
            # print(files, '-----------')
        # yes date
        else:
            opts = self.options.copy()
            opts.update({
                "query": f"""select count(*) from {self.dbtable} 
                    where {self.lastUpdate_col} >= '{self.start_date}'""",
            })
            # filterdf = df.filter(df[self.lastUpdate_col] >= self.start_date)
            files = self.getVol(opts)
            if files == 0:
                return None
            filterdfs = df.repartition(files)
            
        return filterdfs
    
    # if folder is created its always empty -> no need to return bool
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
 
    def write(self)-> bool:
        
        # check if folder exist, if not -> create
        path = f"{self.hdfs}{self.dbtable}"
        df = self.read()
        folder_exist = self.__checkFolderExist(path)
        start_date_str = self.start_date.strftime('%Y%m%d')
        hdfs_path = f"{self.hdfs}{self.dbtable}/{self.job}={start_date_str}"
        if df.rdd.isEmpty():
            return False
        
        if not folder_exist:
            opts = self.options.copy()
            opts.update({
            "query": f"""select count(*) from {self.dbtable}"""
            })
            files = self.getVol(opts)
            df = df.repartition(files)
            print("-----Extract all-----")
            df.write.mode("overwrite").parquet(hdfs_path)
            return True
        
        print("-----Extract date-----")
        print(f"-----{start_date_str}----")
        filterdf = self.filterDF(df)
        
        if filterdf is None:
            return False
        
        if isinstance(filterdf, list):
            filterdf[0].write.partitionBy("modHashId").mode("overwrite").parquet(hdfs_path)
            return True
        
        filterdf.write.mode("overwrite").parquet(hdfs_path)
        return True
