import yaml
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from functools import partial
from pyspark.sql.dataframe import DataFrame
import json
import uuid


with open('dags/streamdag/config/env.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.Loader)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='Streaming',
    default_args=default_args,
    description='test',
    schedule="None",#@daily
)
def get_data():
    res = requests.get(config['api'])
    res = res.json()
    res = res['results'][0]
    # print(json.dumps(res, indent=4))
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    from kafka import KafkaProducer
    import time
    import logging

    curr_time = time.time()
    res = get_data()
    res = format_data(res)
    data = json.dumps(res).encode('utf-8')
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    # call api to get and stream data for 1 min
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 60: #1 minute
            logging.info("-----DONE-----")
            break
        try:
            res = get_data()
            res = format_data(res)
            print(time.time())
            producer.send('users_created', data)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with TaskGroup(group_id="tasks", dag=dag) as group:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
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

start >> group >> end