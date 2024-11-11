import yaml
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from functools import partial
from apidag.transform import transformData
from pyspark.sql.dataframe import DataFrame
import os
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("url")
host = os.getenv("host")
key = os.getenv("key")

with open('dags/apidag/config/config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.Loader)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='API-Nasdaq',
    default_args=default_args,
    description='test',
    # schedule="@daily",
)

#///////////////////
start = BashOperator(
    task_id='start',
    bash_command='echo Start process',
    dag=dag
)

end = BashOperator(
    task_id='end',
    bash_command='echo End process',
    dag=dag
)

start >> end