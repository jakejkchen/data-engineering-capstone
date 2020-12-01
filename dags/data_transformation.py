import lib.emr_lib as emr
import os
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import ClusterCheckSensor
from airflow.operators import ClusterCheckSensor
from airflow import AirflowException

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 1),
    'retries': 0,
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('transform_movielens', concurrency=3, schedule_interval=None, default_args=default_args)
region = 'us-east-1'
emr.client(region_name=region)

# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = emr.create_cluster(region_name=region, cluster_name='airflow_test', num_core_nodes=2)
    return cluster_id

# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)

# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)

def submit_spark_job(**kwargs):
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'spark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(session_url,
                                              kwargs['params']['file'])
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)

process_airport = PythonOperator(
    task_id='process_airport',
    python_callable = submit_spark_job,
    params={
        "file" : '/root/airflow/dags/transform/processing_airport.py',
        "log" : False
    },
    dag=dag
)

process_demo = PythonOperator(
    task_id='process_demo',
    python_callable = submit_spark_job,
    params={
        "file" : '/root/airflow/dags/transform/processing_demo.py',
        "log" : False
    },
    dag=dag
)

process_immigration = PythonOperator(
    task_id='process_immigration',
    python_callable = submit_spark_job,
    params={
        "file" : '/root/airflow/dags/transform/processing_immigration.py',
        "log" : False
    },
    dag=dag
)

process_temp = PythonOperator(
    task_id='process_temperature',
    python_callable = submit_spark_job,
    params={
        "file" : '/root/airflow/dags/transform/processing_temp.py',
        "log" : False
    },
    dag=dag
)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> process_airport >> terminate_cluster
wait_for_cluster_completion >> process_demo >> terminate_cluster
wait_for_cluster_completion >> process_immigration >> terminate_cluster
wait_for_cluster_completion >> process_temp >> terminate_cluster