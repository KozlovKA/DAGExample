from datetime import timedelta, datetime

import airflow
import os
import sys
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

os.environ['SPARK_HOME'] = '/opt/spark'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'ko3lof',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    # 'schedule_interval': '30 11 * * 4',
    'retries': 2
}
dag = DAG("DataLoad", default_args=default_args, schedule_interval=timedelta(days=1)
          )
t1 = BashOperator(
    task_id='DataLoad',
    bash_command='echo $PATH',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)

t1
