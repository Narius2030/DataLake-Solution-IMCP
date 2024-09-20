import datetime
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from bronze.load import load_parquet


# Static Function
def start():
    print('Starting to Integrate Data Warehouse...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))


with open("/opt/airflow/config/env.json", "r") as file:
    config = json.load(file)
    mongo_url = config['mongodb']['MONGO_ATLAS_PYTHON_GCP']


# DAGs
with DAG(
    'IMCP_Data_Integration',
    schedule_interval='0 23 * * *',
    default_args={'start_date': days_ago(1)},
    catchup=False
) as dag:
    print_start = PythonOperator(
        task_id = 'starting_integration',
        python_callable = start,
        dag = dag
    )
    
    bronze_huggface = PythonOperator(
        task_id = 'ingest_raw_data',
        params = {
            'file-path': Variable.get('huggingface_parquet_path'),
            'engine': 'pyarrow',
            'mongo-gcp-url': Variable.get('mongo_gcp_url')
        },
        python_callable = load_parquet,
        dag = dag
    )
    
    print_end = PythonOperator(
        task_id = 'ending_integration',
        python_callable = start,
        dag = dag
    )


# Workflow
print_start >> bronze_huggface >> print_end
    