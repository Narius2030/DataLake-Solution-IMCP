import sys
sys.path.append('./airflow')

import pymongo
import pandas as pd
from tqdm import tqdm
import json
import warnings
from core.config import get_settings
from functions.operators.database import MongoDBOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)


# def data_generator(db, datasets):
#     accepted_datas = []
#     count = 0
#     for data in tqdm(datasets):
#         query = {
#             "url": f"{data['url']}",
#             "$and": [
#                 {"caption": f"{data['caption']}"},
#                 {"short_caption": f"{data['short_caption']}"}
#             ]
#         }
#         docs = db['raw'].find(query, {"url":1,"caption":1,"short_caption":1})
#         if any(docs) == False:
#             accepted_datas.append(data)
#         count += 1
#         if count == 1000:
#             count = 0
#             yield accepted_datas

# def audit_log(start_time, end_time, status, error_message="", affected_rows=0, action=""):
#     with pymongo.MongoClient(settings.DATABASE_URL) as client:
#         db = client['imcp']
#         log = {
#             "layer": "bronze",
#             "table_name": "raw",
#             "start_time": start_time,
#             "end_time": end_time,
#             "status": status,
#             "error_message": error_message,
#             "affected_rows": affected_rows,
#             "action": action
#         }
#         db['audit'].insert_one(log)

# def load_raw_collection(params):
#     df = pd.read_parquet(params['file-path'], params['engine'])
#     df['created_time'] = pd.to_datetime('now')
#     df['publisher'] = 'HuggingFace'
#     df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
#     datasets = df.to_dict('records')
    
#     with pymongo.MongoClient(settings.DATABASE_URL) as client:
#         db = client["imcp"]
#         # start to load
#         start_time = pd.to_datetime('now')
#         affected_rows = 0
#         try:
#             docs = db['huggingface'].find()
#             if any(docs) == False:
#                 warnings.warn("There is no documents in collection --> INGEST ALL")
#                 # If there is empty collection -> insert all
#                 resp = db['huggingface'].insert_many(datasets)
#                 affected_rows = len(resp.inserted_ids)
#             else:
#                 # If there are several documents -> check duplication -> insert one-by-one
#                 warnings.warn("There are documents in collection --> CHECK DUPLICATION")
#                 for batch in data_generator(db, datasets):
#                     if batch != []:
#                         print("Loading...", len(batch))
#                         db['huggingface'].insert_many(batch)
#             # Write logs
#             audit_log(start_time, pd.to_datetime('now'), status="SUCCESS", action="insert", affected_rows=affected_rows)

#         except Exception as ex:
#             # Write logs
#             audit_log(start_time, pd.to_datetime('now'), status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
#             # Raise error
#             raise Exception(str(ex))
        
        

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