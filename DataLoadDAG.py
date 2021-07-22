from datetime import timedelta, datetime

import airflow
import os
import sys
from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

os.environ['SPARK_HOME'] = '/opt/spark'
os.environ['PATH'] += "/opt/spark/bin:/opt/spark/sbin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin"
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
    task_id='DataLoad1',
    bash_command='echo hello world ',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)
t2 = BashOperator(
    task_id='DataLoad2',
    bash_command='echo $PATH ',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)
t3 = BashOperator(
    task_id='DataLoad3',
    bash_command='spark-submit --class {{ params.class }} {{ params.jar }}',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)

t1 >> [t2, t3]
