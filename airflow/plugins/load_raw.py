import sys
sys.path.append('/opt/airflow')

import pandas as pd
import warnings
import os
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.operators.storage import MinioStorageOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def dowload_raw_data(bucket_name, file_path, parquet_engine):
    datasets = {}
    file_name = os.path.basename(file_path)
    try:
        minio_operator.download_file(bucket_name, file_path, f'{settings.RAW_DATA_PATH}/{file_name}')
        df = pd.read_parquet(f'{settings.RAW_DATA_PATH}/{file_name}', parquet_engine)
        df['created_time'] = pd.to_datetime('now')
        df['publisher'] = 'HuggingFace'
        df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
        datasets = df.to_dict('records')
    except Exception as ex:
        raise FileNotFoundError(str(ex))
    finally:
        os.remove(f'{settings.RAW_DATA_PATH}/{file_name}')
    return datasets


def load_raw_collection(params):
    datasets = dowload_raw_data(params['bucket_name'], params['file_path'], params['engine'])
    print('=====>', len(datasets))
    # # start to load
    # start_time = pd.to_datetime('now')
    # affected_rows = 0
    # try:
    #     if mongo_operator.is_has_data('huggingface') == False:
    #         warnings.warn("There is no documents in collection --> INGEST ALL")
    #         # If there is empty collection -> insert all
    #         affected_rows = mongo_operator.insert('huggingface', datasets)
    #     else:
    #         # If there are several documents -> check duplication -> insert one-by-one
    #         warnings.warn("There are documents in collection --> CHECK DUPLICATION")
    #         mongo_operator.insert_many_not_duplication('huggingface', datasets)
    #     # Write logs
    #     mongo_operator.write_log('audit', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)

    # except Exception as ex:
    #     mongo_operator.write_log('audit', start_time=start_time, status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
        

if __name__=='__main__':
    params = {
        'bucket_name': 'mlflow',
        'file_path': '/raw_data/lvis_caption_url.parquet',
        'engine': 'pyarrow',
        'mongo-url': settings.DATABASE_URL
    }
    load_raw_collection(params)