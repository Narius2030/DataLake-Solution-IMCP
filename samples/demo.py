from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or, And
import pandas as pd


def load_parquet(params):
    df = pd.read_parquet(params['file-path'], params['engine'])
    df['created_time'] = pd.to_datetime('now')
    df['publisher'] = 'HuggingFace'
    df['year'] = '2023'
    df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
    df['_id'] = df['url'].map(lambda x: x[-16:-4])
    print(df.head())
    

if __name__ == '__main__':
    params = {
        'file-path': './airflow/data/HuggingFace/lvis_caption_url.parquet',
        'engine': 'pyarrow'
    }
    load_parquet(params)
    
    