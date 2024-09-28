import sys
sys.path.append('./airflow')

from functions.text.clean import lower_case, tokenize_caption, remove_punctuations, scaling
from pyspark.sql import SparkSession
import pymongo
import pandas as pd
import json
from core.config import get_settings


settings = get_settings()

def audit_log(start_time, end_time, status, error_message="", affected_rows=0, action=""):
    with pymongo.MongoClient(settings.DATABASE_URL) as client:
        db = client['imcp']
        log = {
            "layer": "silver",
            "table_name": "refined",
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "error_message": error_message,
            "affected_rows": affected_rows,
            "action": action
        }
        db['audit'].insert_one(log)


def normalize_caption():
    # create a local SparkSession
    spark = SparkSession.builder \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.network.timeout", "300s") \
                .config("spark.executor.heartbeatInterval", "120s") \
                .config("spark.executor.memory", "4g") \
                .config("spark.driver.memory", "2g") \
                .appName("Normalize data") \
                .getOrCreate()

    # define a batch query
    bronze_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option('spark.mongodb.input.uri', settings.DATABASE_URL) \
                        .option('spark.mongodb.input.database', 'imcp') \
                        .option('spark.mongodb.input.collection', 'raw') \
                        .load()

    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        # clean the data in RDD
        bronze_df = bronze_df.limit(8000)
        temp_lwc = lower_case(bronze_df)
        temp_rmp = remove_punctuations(temp_lwc)
        temp_tok = tokenize_caption(temp_rmp)
        silver_df = scaling(temp_tok)
        # insert to mongodb
        silver_df.write.format("com.mongodb.spark.sql.DefaultSource") \
                .option('spark.mongodb.output.uri', settings.DATABASE_URL) \
                .option('spark.mongodb.output.database', 'imcp') \
                .option('spark.mongodb.output.collection', 'refined') \
                .mode('append') \
                .save()
        
        affected_rows = silver_df.count()
        # Write logs
        audit_log(start_time, pd.to_datetime('now'), status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        with pymongo.MongoClient(settings.DATABASE_URL) as client:
            collection = client['imcp']['refined']
            documents = collection.aggregate([{
                '$sort': {
                    'created_time': -1
                }
            }, {
                '$project': {
                    '_id': 1   
                }
            }])
        affected_rows = len(list(documents))
        # Write logs
        audit_log(start_time, pd.to_datetime('now'), status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        
        #Raise error
        raise Exception(str(exc))
    
    print('''
            ===========================================================
            Number of rows were inserted: {}
            ===========================================================
        ''', affected_rows)
    spark.stop()



if __name__=='__main__':
    # normalize_caption()
    pass