import sys
sys.path.append('./airflow')

import pymongo
import pandas as pd
from tqdm import tqdm
import json
import warnings
from core.config import get_settings


settings = get_settings()

def data_generator(db, datasets):
    accepted_datas = []
    count = 0
    for data in tqdm(datasets):
        query = {
            "url": f"{data['url']}",
            "$and": [
                {"caption": f"{data['caption']}"},
                {"short_caption": f"{data['short_caption']}"}
            ]
        }
        docs = db['raw'].find(query, {"url":1,"caption":1,"short_caption":1})
        if any(docs) == False:
            accepted_datas.append(data)
        count += 1
        if count == 1000:
            count = 0
            yield accepted_datas

def audit_log(start_time, end_time, status, error_message="", affected_rows=0, action=""):
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client['imcp']
        log = {
            "layer": "bronze",
            "table_name": "raw",
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "error_message": error_message,
            "affected_rows": affected_rows,
            "action": action
        }
        db['audit'].insert_one(log)

def load_parquet(params):
    df = pd.read_parquet(params['file-path'], params['engine'])
    df['created_time'] = pd.to_datetime('now')
    df['publisher'] = 'HuggingFace'
    df['year'] = '2023'
    df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
    datasets = df.to_dict('records')
    
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client["imcp"]
        # start to load
        start_time = pd.to_datetime('now')
        affected_rows = 0
        try:
            docs = db['raw'].find()
            if any(docs) == False:
                warnings.warn("There is no documents in collection --> INGEST ALL")
                # If there is empty collection -> insert all
                resp = db['raw'].insert_many(datasets)
                affected_rows = len(resp.inserted_ids)
            else:
                # If there are several documents -> check duplication -> insert one-by-one
                warnings.warn("There are documents in collection --> CHECK DUPLICATION")
                for batch in data_generator(db, datasets):
                    if batch != []:
                        print("Loading...", len(batch))
                        db['raw'].insert_many(batch)
            # Write logs
            audit_log(start_time, pd.to_datetime('now'), status="SUCCESS", action="insert", affected_rows=affected_rows)

        except Exception as ex:
            # Write logs
            audit_log(start_time, pd.to_datetime('now'), status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
            # Raise error
            raise Exception(str(ex))
        

if __name__=='__main__':
    pass