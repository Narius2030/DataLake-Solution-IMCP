import datetime
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from airflow.operators.email import EmailOperator #type: ignore
from load_raw import load_raw_collection, load_raw_image #type: ignore
from load_refined import load_refined_data #type: ignore
from load_business_data import load_encoded_data, load_image_storage #type: ignore

# Static Function
def send_failure_email(context):
    # Truy xuất thông tin về task và DAG từ context
    task_id = context['task_id']
    dag_id = context['dag_id']
    execution_date = context['execution_date']

    # Tạo nội dung email
    email_content = f"""
    DAG {dag_id} đã thất bại tại task {task_id}.
    Thời gian thực thi: {execution_date}
    """

    # Gửi email
    EmailOperator(
        task_id='send_failure_email',
        to='nhanbui15122003@gmail.com',
        subject='IMCP DAGs ARE FAILED',
        html_content=email_content
    ).execute(context)


# DAGs
with DAG(
    'IMCP_Data_Integration',
    schedule_interval='0 23 * * *',
    default_args={'start_date': days_ago(1)},
    catchup=False
) as dag:
    
    start = DummyOperator(task_id="start")
    
    bronze_data = PythonOperator(
        task_id = 'ingest_raw_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_data_path'),
            'engine': 'pyarrow',
            'mongo-url': Variable.get('MONGO_ATLAS_PYTHON')
        },
        python_callable = load_raw_collection,
        on_failure_callback = send_failure_email,
        dag = dag
    )
    
    bronze_image_data = PythonOperator(
        task_id = 'ingest_raw_image_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_image_path'),
        },
        python_callable = load_raw_image,
        on_failure_callback = send_failure_email,
        dag = dag
    )
    
    silver_data = PythonOperator(
        task_id = 'refine_raw_data',
        python_callable = load_refined_data,
        on_failure_callback = send_failure_email,
        trigger_rule='one_success',
        dag = dag
    )
    
    gold_data = PythonOperator(
        task_id = 'extract_image_feature',
        python_callable = load_encoded_data,
        on_failure_callback = send_failure_email,
        dag = dag
    )
    
    upload_features = PythonOperator(
        task_id = 'upload_s3_image_feature',
        python_callable = load_image_storage,
        on_failure_callback = send_failure_email,
        dag = dag
    )
    
    end = DummyOperator(task_id="end")


# Workflow
start >> [bronze_data, bronze_image_data] >> silver_data >> gold_data >> upload_features >> end
    