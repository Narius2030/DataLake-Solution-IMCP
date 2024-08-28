from pyspark.sql import SparkSession

# create a local SparkSession
spark = SparkSession.builder \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .appName("readExample") \
                .getOrCreate()

# define a streaming query
bronze_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                    .option('spark.mongodb.input.uri', 'mongodb://admin:nhanbui@localhost:27017/imcp.bronze?authSource=admin') \
                    .load()

print(bronze_df.printSchema())