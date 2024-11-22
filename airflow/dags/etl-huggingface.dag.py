import datetime
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_raw import load_raw_parquets, load_raw_image, load_raw_user_data #type: ignore
from load_refined import load_refined_data #type: ignore
from load_business_data import load_encoded_data, load_image_storage #type: ignore


# DAGs
with DAG(
    'IMCP_Data_Integration',
    schedule_interval='0 23 * * *',
    default_args={
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': True,
        'email': ['nhanbui15122003@gmail.com', 'dtptrieuphidtp@gmail.com', '159.thiennhan@gmail.com']
    },
    catchup=False
) as dag:
    # Start pipeline
    start = DummyOperator(task_id="start")
    
    with TaskGroup("ingest_raw_data", tooltip="Tasks for raw data ingestion") as bronze:
        common_params={
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_data_path'),
            'file_image_path': Variable.get('raw_image_path'),
            'engine': 'pyarrow',
            'mongo-url': Variable.get('MONGO_ATLAS_PYTHON')
        }
        # Bronze process
        bronze_data = PythonOperator(
            task_id = 'ingest_raw_parquet_data',
            params = common_params,
            python_callable = load_raw_parquets,
            dag = dag
        )
        
        bronze_user_data = PythonOperator(
            task_id = 'ingest_raw_user_data',
            python_callable = load_raw_user_data,
            dag = dag
        )
        
        bronze_image_data = PythonOperator(
            task_id = 'ingest_raw_image_data',
            params = common_params,
            python_callable = load_raw_image,
            trigger_rule='one_success',
            dag = dag
        )
        
        [bronze_data, bronze_user_data] >> bronze_image_data
        
    # Silver process
    silver_data = PythonOperator(
        task_id = 'refine_raw_data',
        python_callable = load_refined_data,
        trigger_rule='one_success',
        dag = dag
    )
    
    
    # Gold process
    with TaskGroup("extract_image_features", tooltip="Tasks for image feature extraction") as gold:
        gold_data = PythonOperator(
            task_id = 'extract_image_feature',
            python_callable = load_encoded_data,
            dag = dag
        )
        upload_features = PythonOperator(
            task_id = 'upload_s3_image_feature',
            python_callable = load_image_storage,
            dag = dag
        )
        gold_data >> upload_features
    
    # End pipeline
    end = DummyOperator(task_id="end")



# pipeline
start >> bronze >> Label("refine data") >> silver_data >> Label("extract feature") >> gold >> end

    