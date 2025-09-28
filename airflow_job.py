from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 28),
}

# Function to get execution date (param or ds_nodash)
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date  # This automatically goes to XCom

# DAG definition
with DAG(
    dag_id="customer_filter_based_on_sex",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Pass date in yyyymmdd format')
    }
) as dag:

    # Fetch cluster config from Airflow Variables
    config = Variable.get("cluster_details", deserialize_json=True)
    CLUSTER_NAME = config['CLUSTER_NAME']
    PROJECT_ID = config['PROJECT_ID']
    REGION = config['REGION']

    # Task 1: Resolve execution date
    get_execution_date_task = PythonOperator(
        task_id='get_execution_date',
        python_callable=get_execution_date,
        provide_context=True,
        op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    )

    # Task 2: GCS File Sensor using XCom from get_execution_date_task
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket="airflow-project-dev",
        object="project-1/data/people_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv",
        google_cloud_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # Task 3: Submit PySpark job to Dataproc
    PYSPARK_JOB = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": "gs://airflow-project-dev/project-1/spark-job/transformation.py",
            "args": ["--date={{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        },
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Define task order
    get_execution_date_task >> file_sensor >> submit_pyspark_job
