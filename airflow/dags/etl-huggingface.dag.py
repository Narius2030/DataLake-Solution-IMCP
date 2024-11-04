import datetime
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from load_raw import load_raw_collection
from load_refined import load_refined_data
from load_business_data import load_encoded_data, load_image_storage

# Static Function
def start():
    print('Starting to Integrate Data Warehouse...')

def end():
    print('Finish at {}!'.format(datetime.today().date()))



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
    
    bronze_huggingface = PythonOperator(
        task_id = 'ingest_raw_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_data_path'),
            'engine': 'pyarrow',
            'mongo-url': Variable.get('MONGO_ATLAS_PYTHON')
        },
        python_callable = load_raw_collection,
        dag = dag
    )
    
    silver_huggingface = PythonOperator(
        task_id = 'refine_raw_data',
        python_callable = load_refined_data,
        dag = dag
    )
    
    gold_huggingface = PythonOperator(
        task_id = 'extract_image_feature',
        python_callable = load_encoded_data,
        dag = dag
    )
    
    upload_features = PythonOperator(
        task_id = 'extract_image_feature',
        python_callable = load_image_storage,
        dag = dag
    )
    
    print_end = PythonOperator(
        task_id = 'ending_integration',
        python_callable = start,
        dag = dag
    )


# Workflow
print_start >> bronze_huggingface >> silver_huggingface >> gold_huggingface >> upload_features >> print_end
    