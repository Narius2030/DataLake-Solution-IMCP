import pymongo
import pandas as pd
from pyspark.sql import SparkSession


# df = pd.read_parquet('./airflow/data/HuggingFace/lvis_caption_url.parquet', engine='pyarrow')
# df['created_time'] = pd.to_datetime('now')
# df['publisher'] = 'HuggingFace'
# df['year'] = '2023'
# df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
# datasets = df.to_dict('records')

# try:
#     with pymongo.MongoClient("mongodb+srv://nhanbuimongogcp:nhanbui@mongdb-gcp-cluster.eozg9.mongodb.net/?retryWrites=true&w=majority&appName=mongdb-gcp-cluster") as client:
#         db = client["imcp"]
#         # start to load
#         resp = db['bronze_layer'].insert_many(datasets)
#         affected_rows = len(resp.inserted_ids)
# except Exception as ex:
#     print(str(ex))
    

def connect_with_spark():
    spark = SparkSession.builder \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .appName("readExample") \
                .getOrCreate()
                
    bronze_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                    .option('spark.mongodb.input.uri', 'mongodb+srv://nhanbuimongogcp:nhanbui@mongdb-gcp-cluster.eozg9.mongodb.net/?retryWrites=true&w=majority&appName=mongdb-gcp-cluster') \
                    .option('spark.mongodb.input.database', 'imcp') \
                    .option('spark.mongodb.input.collection', 'bronze_layer') \
                    .load()
                    
    print(bronze_df.printSchema())
    
connect_with_spark()