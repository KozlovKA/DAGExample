from datetime import timedelta, datetime

import airflow
import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

now = datetime.now

os.environ['SPARK_HOME'] = '/opt/spark'
os.environ['PATH'] += "/opt/spark/bin:/opt/spark/sbin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin"
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'ko3lof',
    'depends_on_past': False,
    # 'schedule_interval': '30 11 * * 4',
}
dag = DAG("DataLoad",
          default_args=default_args,
          start_date=days_ago(2),
          schedule_interval="30 11 * * 4")
spark_home = Variable.get("SPARK_HOME")

t1 = SparkSubmitOperator(task_id='DataLoad',
                         application="/home/ko3lof/testing-assembly-0.1.jar",
                         name="DataLoad",
                         retries=2,
                         retry_delay=datetime(year=0, month=0, day=0, hour=0, minute=0, second=120),
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
