import os
import json
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

# Project path
PROJECT_PATH = "/Users/r/PycharmProjects/Data-Engineering/lesson_07"

# Function to fetch data from the API
def extract_data_from_api(**kwargs) -> None:
    ti: TaskInstance = kwargs.get('ti')
    execution_date = kwargs['ds']
    http_hook = HttpHook(
        method='GET',
        http_conn_id='airflow_db'  
    )
    response = http_hook.run(endpoint='v1/data/sales')  # replace with the actual endpoint
    if response.status_code == 201:
        data = response.json()
        raw_dir = f'{PROJECT_PATH}/raw/sales/{execution_date}'
        os.makedirs(raw_dir, exist_ok=True)
        file_path = os.path.join(raw_dir, 'data.json')
        
        # Save data
        with open(file_path, 'w') as file:
            json.dump(data, file)
        print(f"Data for {execution_date} saved successfully.")
    else:
        raise ValueError(f"Failed to fetch data for {execution_date}. Status code: {response.status_code}")

# Function to convert data to Avro format
def convert_to_avro(**kwargs) -> None:
    execution_date = kwargs['ds']
    raw_file = f'{PROJECT_PATH}/raw/sales/{execution_date}/data.json'
    stg_dir = f'{PROJECT_PATH}/stg/sales/{execution_date}'
    os.makedirs(stg_dir, exist_ok=True)
    avro_file_path = os.path.join(stg_dir, 'data.avro')
    
    # Convert data to Avro format
    with open(raw_file, 'r') as file:
        data = json.load(file)
    
    # Use fastavro for conversion to Avro format
    from fastavro import writer, parse_schema
    
    schema = {
        "type": "record",
        "name": "Sales",
        "fields": [{"name": "field1", "type": "string"}, {"name": "field2", "type": "string"}]
    }
    parsed_schema = parse_schema(schema)
    
    with open(avro_file_path, 'wb') as out_file:
        writer(out_file, parsed_schema, [data])  # Write in Avro format

    print(f"Data converted to Avro format for {execution_date}")

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_sales',
    default_args=default_args,
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True,
) as dag:

    # Initial task
    start = EmptyOperator(task_id='start')

    # Task to fetch data from the API
    extract_data_task = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api,
        provide_context=True
    )

    # Task to convert data to Avro format
    convert_to_avro_task = PythonOperator(
        task_id='convert_to_avro',
        python_callable=convert_to_avro,
        provide_context=True
    )

    # Final task
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ALL_DONE
    )

    # Task execution order
    start >> extract_data_task >> convert_to_avro_task >> end
