from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_raw import load_raw_parquets #type: ignore



# DAGs
with DAG(
    'IMCP_Raw_Data_Parquet_Integration',
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
    
    bronze_data = PythonOperator(
        task_id = 'ingest_raw_parquet_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_data_path'),
            'engine': 'pyarrow',
            'mongo-url': Variable.get('MONGO_ATLAS_PYTHON')
        },
        python_callable = load_raw_parquets,
        dag = dag
    )
    
    # End pipeline
    end = DummyOperator(task_id="end")
    
start >> bronze_data >> end