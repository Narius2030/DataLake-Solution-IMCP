import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or, And


# Function
db = firestore.Client()
data = {
    u'first': u'Brodo',
    u'last': u'Ficaso',
    u'born': 1998,
    'career': 'artist'
}
def create(params:dict):
    doc_ref = params["db"].collection(u'users').document(params["document_name"])
    doc_ref.set(params["data"])
    print("DocumentID: ", doc_ref.id)
    
def get_documents(params:dict):
    for doc in params["db"].collection(params["collname"]).stream():
        data = doc.to_dict()
        print(data)

def filter_document(params:dict):
    doc_ref = params["db"].collection(params["collname"])
    docs = doc_ref.order_by('last', direction=firestore.Query.DESCENDING).start_at({'last': 'Nguyen'}).stream()
    
    for doc in docs:
        print(doc.to_dict())


# DAGs
with DAG(
    'Handling_Firestore_Python',
    schedule_interval='0 23 * * *',
    default_args={'start_date': days_ago(1)},
    catchup=False
) as dag:
    create_document = PythonOperator(
        task_id = "create_document",
        params = {"db": db, "data": data, "document_name": None},
        python_callable = create,
        dag=dag
    )
    
    query_all = PythonOperator(
        task_id = "query_all_documents",
        params = {"db": db, "collname": "users"},
        python_callable = get_documents,
        dag=dag
    )
    
    filter_document = PythonOperator(
        task_id = "filter_documents",
        params = {"db": db, "collname": "users"},
        python_callable = filter_document,
        dag=dag
    )
    

# Workflow
create_document >> query_all >> filter_document
    