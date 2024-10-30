import sys
sys.path.append('./airflow')

import pymongo
import pandas as pd
import polars as pl
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.pre_proccess import tokenize, scaling_data, clean_text


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        for batch in mongo_operator.data_generator('huggingface'):
            data = list(batch)
            df = pl.DataFrame(data).drop('_id')
            lowered_df = df.with_columns(
                *[pl.col(col).str.to_lowercase().alias(col) for col in ['caption','short_caption']]
            )
            cleaned_df = lowered_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: clean_text(x), return_dtype=pl.String).alias(col) for col in ['caption','short_caption']]
            )
            tokenized_df = cleaned_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: tokenize(x), return_dtype=pl.List(pl.String)).alias(f'{col}_tokens') for col in ['caption','short_caption']]
            )
            refined_df = scaling_data(tokenized_df, ['url', 'caption', 'short_caption', 'caption_tokens', 'short_caption_tokens', 'publisher', 'created_time'])
            data = refined_df.to_dicts()
            mongo_operator.insert('refined', data)
            
            affected_rows += len(data)
            print('SUCCESS with', len(data))
    
        # Write logs
        mongo_operator.write_log('audit', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        aggregate = [{'$sort': {'created_time': -1}}, {'$project': {'_id': 1}}]
        data = mongo_operator.find_data_with_aggregate('refined', aggregate)
        affected_rows = len(data)
        # Write logs
        mongo_operator.write_log('audit', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        
        #Raise error
        raise Exception(str(exc))
    
    print('''
            ===========================================================
            Number of rows were inserted: {}
            ===========================================================
        ''', affected_rows)



if __name__=='__main__':
    load_refined_data()