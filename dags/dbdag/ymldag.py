import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup
from functools import partial
from dbdag.transform import transformData
from pyspark.sql.dataframe import DataFrame

with open('/home/khai/airflow/dags/dbdag/configs/dag-config.yml', 'r') as f:
    config = yaml.load(f, Loader=yaml.Loader)

with open('/home/khai/airflow/dags/dbdag/configs/env.yml', 'r') as f:
    env = yaml.load(f, Loader=yaml.Loader)

   
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='pipelineDB',
    default_args=default_args,
    description='test',
    schedule="@daily",
)


def transform_spark(job, db_name, table_name, numPartitions,lowerBound,
                    upperBound,partitionColumn,primaryKey_col,lastUpdate_col, url:str, 
                    user:str, password:str, hdfs:str, **kwargs)-> bool:
            
    start_datetime = datetime.fromtimestamp(kwargs['data_interval_start'].timestamp())
    # start_datetime = datetime(2024,10,21)
    # print(start_datetime)
    # print(type(start_datetime))
    transform = transformData(
        job = job,
        dbname=db_name,
        start_date=start_datetime,
        dbtable=table_name,
        numPartitions=numPartitions,
        lowerBound=lowerBound,
        upperBound=upperBound,
        partitionColumn=partitionColumn,
        primaryKey_col=primaryKey_col,
        lastUpdate_col=lastUpdate_col,
        url=url, user=user, password=password, hdfs=hdfs
    )
    return transform.write()

def python_task(job, db_name,table_name, numPartitions,lowerBound,
                upperBound,partitionColumn,primaryKey_col,lastUpdate_col,
                url:str,user:str, password:str, hdfs:str):
    
    return PythonOperator(
        task_id=f"Table_{table_name}_{user}",
        python_callable=transform_spark,
        op_kwargs={
            'job': job,
            'db_name': db_name,
            'table_name': table_name,
            'numPartitions': numPartitions,
            'lowerBound': lowerBound,
            'upperBound': upperBound,
            'partitionColumn': partitionColumn,
            'primaryKey_col': primaryKey_col,
            'lastUpdate_col': lastUpdate_col,
            'url':url,'user':user, 'password':password, 'hdfs':hdfs
        },
        # provide_context = True,
        dag=dag
    )


def child_group(detail, options):
    db_name = detail.get('db_name')
    with TaskGroup(group_id=f"DB_{db_name}_", dag=dag) as group: 
        process_table = partial(python_task, detail['job'], detail['db_name']) 
        if detail['tbl_list'] is None:
            process_table()
        for tbl in detail['tbl_list']:
            dt = tbl.values()
            # print(dt, '-----------------')
            # print(options, '-----------------')
            process_table(*dt,*options) 
    
    return group

def user_group(name, options:list):
    with TaskGroup(group_id=f"{name}", dag=dag) as group: 
        users = config.get('config')
        detail:any
        for user in users:
            if user.get('user') == name: #user name that contains databases ex. postgres, mysql
                item = user.get('databases')
                for detail in item:
                    task_groups = list(child_group(detail= detail, options=options))
            else:
                pass 

with TaskGroup(group_id="tasks", dag=dag) as group:
    
    hdfs = env["hdfs"]
    for DB in env["db"]:
        user = DB["user"]
        url = DB["url"]
        password = DB["password"]
        options = [url,user,password, hdfs]
        print(options)
        task_groups = user_group(name = user, options = options)

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
