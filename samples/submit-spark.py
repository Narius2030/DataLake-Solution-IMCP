from functions.text import clean
from pyspark.sql import SparkSession

def normalize_caption():
    # create a local SparkSession
    spark = SparkSession.builder \
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                    .appName("readExample") \
                    .getOrCreate()

    # define a streaming query
    bronze_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option('spark.mongodb.input.uri', 'mongodb+srv://nhanbuimongogcp:nhanbui@mongdb-gcp-cluster.eozg9.mongodb.net/?retryWrites=true&w=majority&appName=mongdb-gcp-cluster') \
                        .option('spark.mongodb.input.database', 'imcp') \
                        .option('spark.mongodb.input.collection', 'bronze_layer') \
                        .load()

    bronze_rdd = bronze_df.rdd
    
    bronze_rdd = bronze_rdd.map(clean.lower_case)
    bronze_rdd = bronze_rdd.map(clean.remove_punctuations)
    silver_rdd = bronze_rdd.map(clean.tokenize)
    print(silver_rdd.take(2))
    
    spark.stop()
    
if __name__=='__main__':
    # normalize_caption()
    clean.test()


