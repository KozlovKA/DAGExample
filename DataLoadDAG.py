from datetime import timedelta, datetime

import airflow
import os
import sys
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# os.environ['SPARK_HOME'] = '/opt/spark'
# os.environ['PATH'] += ":/opt/spark/bin:/opt/spark/sbin"
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'ko3lof',
    'depends_on_past': False,
    # 'schedule_interval': '30 11 * * 4',
}
dag = DAG("DataLoad",
          default_args=default_args,
          start_date=days_ago(2),
          schedule_interval="30 11 * * 4")
spark_config = {
    "master": "spark://spark-air-master-0.spark-air-headless.airflow.svc.cluster.local:7077"
}

t3 = SparkSubmitOperator(task_id='DataLoad',
                         name="DataLoad",
                         application="local:///home/ko3lof/testing-assembly-0.1.jar",
                         dag=dag,
                         conn_id="spark",
                         verbose=1,
                         java_class="DataLoad",
                         conf=spark_config
                         )
t1 = BashOperator(
    task_id='DataLoad2',
    bash_command='export -p',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)

t3