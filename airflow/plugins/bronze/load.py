import pymongo
import pandas as pd
from tqdm import tqdm


def load_parquet(file_path, engine='pyarrow'):
    df = pd.read_parquet(file_path, engine=engine)
    df['created_time'] = pd.to_datetime('now')
    df['publisher'] = 'HuggingFace'
    df['year'] = '2023'
    df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
    datasets = df.to_dict('records')
    
    with pymongo.MongoClient("mongodb://admin:nhanbui@localhost:27017/") as client:
        db = client["imcp"]
        docs = db['bronze'].find()
        try:
            if any(docs) == False:
                db['bronze'].insert_many(datasets)
            else:
                for data in tqdm(datasets):
                    query = {
                        "url": f"{data['url']}",
                        "$and": [
                            {"caption": f"{data['caption']}"},
                            {"short_caption": f"{data['short_caption']}"}
                        ]
                    }
                    docs = db['bronze'].find(query, {'url':1,'caption':1, 'short_caption':1})
                    if any(docs) == False:
                        db['bronze'].insert_one(data)
        except pymongo.errors.WriteError as ex:
            raise Exception(str(ex))
        

if __name__=='__main__':
    load_parquet('./data/HuggingFace/lvis_caption_url.parquet')