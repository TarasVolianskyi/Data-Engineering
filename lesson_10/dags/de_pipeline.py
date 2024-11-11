from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'de_pipeline',
    default_args=default_args,
    description='Pipeline for loading data into GCS',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    catchup=False,
) as dag:

    # Step 1: Data extraction
    extract_data = DummyOperator(
        task_id='extract_data',
    )

    # Step 2: Upload data to GCS
    upload_to_gcs = DummyOperator(
        task_id='upload_to_gcs',
    )

    extract_data >> upload_to_gcs
