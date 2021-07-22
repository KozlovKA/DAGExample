from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ko3lof',
    'start_date': datetime(2021, 7, 22),
    'depends_on_past': False,
    'schedule_interval': '30 11 * * 4',
    'retries': 2
}
dag = DAG("DataLoad", default_args=default_args, schedule_interval=timedelta(days=1)
          )
t1 = BashOperator(
    task_id='DataLoad',
    bash_command='spark-submit --class {{ params.class }} {{ params.jar }}',
    params={'class': 'DataLoad', 'jar': '/home/ko3lof/testing-assembly-0.1.jar'},
    dag=dag
)

t1
