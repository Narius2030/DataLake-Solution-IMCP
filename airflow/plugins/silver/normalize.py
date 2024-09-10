from pyspark.sql import SparkSession
from pyspark.sql import Row
import pymongo
import pandas as pd
import re
import json
from tqdm import tqdm


with open("./airflow/config/env.json", "r") as file:
    config = json.load(file)
    mongo_url = config['mongodb']['MONGO_ATLAS_PYTHON_GCP']


# lower case captions
def lower_case(row):
    lowered_caption = row['caption'].lower()
    lowered_shrtcaption = row['short_caption'].lower()
    return Row(caption=lowered_caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=lowered_shrtcaption, 
                url=row['url'], year=row['year'])

def remove_punctuations(row):
    caption = re.sub(r'[^\w\d\s]', '', row['caption'])
    shrt_caption = re.sub(r'[^\w\d\s]', '', row['short_caption'])
    return Row(caption=caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=shrt_caption, 
                url=row['url'], year=row['year'])
    
def tokenize(row):
    caption = row['caption'].split()
    shrt_caption = row['short_caption'].split()
    return Row(caption=caption, 
                created_time=row['created_time'], 
                howpublished=row['howpublished'],
                publisher=row['publisher'], 
                short_caption=shrt_caption, 
                url=row['url'], year=row['year'],
                text_technique='T01')


def audit_log(start_time, end_time, status, error_message="", affected_rows=0, action=""):
    with pymongo.MongoClient(mongo_url) as client:
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
                    .appName("readExample") \
                    .getOrCreate()

    # define a batch query
    bronze_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option('spark.mongodb.input.uri', mongo_url) \
                        .option('spark.mongodb.input.database', 'imcp') \
                        .option('spark.mongodb.input.collection', 'bronze_layer') \
                        .load()

    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        # clean the data in RDD
        bronze_rdd = bronze_df.rdd
        bronze_rdd = bronze_rdd.map(lower_case)
        bronze_rdd = bronze_rdd.map(remove_punctuations)
        silver_rdd = bronze_rdd.map(tokenize)
        
        # Convert to list of dicts
        datarows = silver_rdd.collect()
        refined_data = []
        for row in datarows:
            refined_data.append(row.asDict(recursive=False))
        print(refined_data[:1])
        
        # Stop spark instance
        spark.stop()
        
        # Insert data to MongoDB
        with pymongo.MongoClient(mongo_url) as client:
            db = client["imcp"]
            resp = db['refined'].insert_many(refined_data)
            affected_rows = len(resp.inserted_ids)
            print('''
                    ===========================================================
                    Number of rows were inserted: {}
                    ===========================================================
                ''', affected_rows)
        # Write logs
        audit_log(start_time, pd.to_datetime('now'), status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        # Write logs
        audit_log(start_time, pd.to_datetime('now'), status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        
        #Raise error
        raise Exception (str(exc))
    
if __name__=='__main__':
    normalize_caption()