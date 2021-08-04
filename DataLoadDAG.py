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
# os.environ['PATH'] += ':/opt/spark/bin:/opt/spark/sbin'
# sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

default_args = {
    'owner': 'ko3lof',
    'depends_on_past': False,
    # 'schedule_interval': '30 11 * * 4',
}
dag = DAG('DataLoad',
          default_args=default_args,
          start_date=days_ago(2),
          schedule_interval='30 11 * * 4')

t2 = SparkSubmitOperator(task_id='DataLoad',
                         name='DataLoad',
                         application='local:///jar/ibaTask-assembly-0.1.jar',
                         dag=dag,
                         conf={
                             'spark.hadoop.fs.stocator.scheme.list': 'cos',
                             'spark.submit.deployMode': 'cluster',
                             'spark.hadoop.fs.stocator.cos.scheme': 'cos',
                             'spark.hadoop.fs.cos.sparkobject123.access.key': Variable.get('access.key'),
                             'spark.hadoop.fs.cos.sparkobject123.endpoint': Variable.get('endpoint'),
                             'spark.hadoop.fs.cos.sparkobject123.secret.key': Variable.get('secret.key'),
                             'spark.hadoop.fs.stocator.cos.impl': 'com.ibm.stocator.fs.cos.COSAPIClient',
                             'spark.hadoop.fs.cos.service.v2.signer.type': 'false',
                             'spark.hadoop.fs.cos.impl': 'com.ibm.stocator.fs.ObjectStoreFileSystem',
                             'spark.kubernetes.container.image': 'ko3lof/spark:check',
                             'spark.kubernetes.driver.pod.name': 'driver',
                             'spark.kubernetes.executor.lostCheck.maxAttempts': '2',
                             'spark.kubernetes.driver.request.cores': '1',
                             'spark.kubernetes.executor.request.cores': '0.5',
                             'spark.kubernetes.authenticate.driver.serviceAccountName': 'spark',
                             'spark.kubernetes.driverEnv.dbPassword': Variable.get('dbPassword'),
                             'spark.kubernetes.driverEnv.dbUsername': Variable.get('dbUsername'),
                             'spark.kubernetes.driverEnv.secret.key': Variable.get('secret.key'),
                             'spark.kubernetes.driverEnv.endpoint': Variable.get('endpoint'),
                             'spark.kubernetes.driverEnv.access.key': Variable.get('access.key'),
                             'spark.executor.instances': '11',
                             'spark.driver.memory': "15m",
                             'spark.executor.memory': '15m',
                             'spark.kubernetes.namespace': 'airflow',
                             'spark.kubernetes.allocation.batch.size': '10'
                             # 'spark.kubernetes.executor.request.cores': '1.0',
                             # 'spark.executor.cores': '2',

                         },
                         conn_id='spark',
                         verbose=1,
                         java_class='by.kozlov.iba.DataTransformation'
                         )
t1 = BashOperator(
    task_id='starTime',
    bash_command='date',
    dag=dag
)
t3 = BashOperator(
    task_id='endTime',
    bash_command='date',
    dag=dag
)
t1 >> t2 >> t3
