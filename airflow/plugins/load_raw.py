import sys
sys.path.append('./airflow')

import pandas as pd
import warnings
from core.config import get_settings
from utils.operators.database import MongoDBOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
        

def load_raw_collection(params):
    df = pd.read_parquet(params['file-path'], params['engine'])
    df['created_time'] = pd.to_datetime('now')
    df['publisher'] = 'HuggingFace'
    df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
    datasets = df.to_dict('records')
    
    # start to load
    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        if mongo_operator.is_has_data('huggingface') == False:
            warnings.warn("There is no documents in collection --> INGEST ALL")
            # If there is empty collection -> insert all
            affected_rows = mongo_operator.insert('huggingface', datasets)
        else:
            # If there are several documents -> check duplication -> insert one-by-one
            warnings.warn("There are documents in collection --> CHECK DUPLICATION")
            mongo_operator.insert_many_not_duplication('huggingface', datasets)
        # Write logs
        mongo_operator.write_log('audit', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)

    except Exception as ex:
        mongo_operator.write_log('audit', start_time=start_time, status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)

        

if __name__=='__main__':
    params = {
        'file-path': './airflow/data/lvis_caption_url.parquet',
        'engine': 'pyarrow',
        'mongo-gcp-url': settings.DATABASE_URL
    }
    load_raw_collection(params)