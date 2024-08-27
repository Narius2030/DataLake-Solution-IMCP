import pymongo
import pandas as pd
from tqdm import tqdm
import json


with open("./airflow/config.json", "r") as file:
    config = json.load(file)
    db_config = config["db_params"]
    src_config = config["source"]

def audit_log(start_time, end_time, status, error_message="", affected_rows=0, action=""):
    with pymongo.MongoClient(f"mongodb://{db_config['user']}:{db_config['password']}@{db_config['endpoint_local']}:27017/") as client:
        db = client['imcp']
        log = {
            "layer": "bronze",
            "table_name": "bronze",
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "error_message": error_message,
            "affected_rows": affected_rows,
            "action": action
        }
        db['audit'].insert_one(log)

def load_parquet(file_path, engine='pyarrow'):
    df = pd.read_parquet(file_path, engine=engine)
    df['created_time'] = pd.to_datetime('now')
    df['publisher'] = 'HuggingFace'
    df['year'] = '2023'
    df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
    datasets = df.to_dict('records')
    
    with pymongo.MongoClient(f"mongodb://{db_config['user']}:{db_config['password']}@{db_config['endpoint_local']}:27017/") as client:
        db = client["imcp"]
        # start to load
        start_time = pd.to_datetime('now')
        affected_rows = 0
        try:
            docs = db['bronze'].find()
            if any(docs) == False:
                # If there is empty collection -> insert all
                resp = db['bronze'].insert_many(datasets)
                affected_rows = len(resp.inserted_ids)
            else:
                # If there are several documents -> check duplication -> insert one-by-one
                for data in tqdm(datasets):
                    query = {
                        "url": f"{data['url']}",
                        "$and": [
                            {"caption": f"{data['caption']}"},
                            {"short_caption": f"{data['short_caption']}"}
                        ]
                    }
                    docs = db['bronze'].find(query, {"url":1,"caption":1,"short_caption":1})
                    if any(docs) == False:
                        db['bronze'].insert_one(data)
            # Write logs
            audit_log(start_time, pd.to_datetime('now'), status="SUCCESS", action="insert", affected_rows=affected_rows)

        except Exception as ex:
            # Write logs
            audit_log(start_time, pd.to_datetime('now'), status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
            # Raise error
            raise Exception(str(ex))
        

if __name__=='__main__':
    load_parquet('./airflow/data/HuggingFace/lvis_caption_url.parquet')