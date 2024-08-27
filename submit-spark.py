from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("PythonWordCount").getOrCreate()

# text = "Vậy câu hỏi mà mình đặt ra ở đây là, làm sao để kết hợp thông tin của hai nguồn dữ liệu trên. Mình nên xây dựng một mạng nhận cả ảnh và caption vào một lúc, hay nên xử lí từng phần riêng biệt, xử lí ảnh riêng, xử lý caption riêng, rồi concat kết quả lại. Nhưng xử lý ảnh và caption cùng lúc hẳn sẽ cung cấp thêm thông tin cho nhau. Vậy lại phát sinh thêm câu hỏi khác, nên dùng cả ảnh để bổ trợ cho mỗi step, hay nên chia ảnh ra thành các step để fig với độ dài của caption. Liệu như thế có thể lấy hết thông tin của ảnh. Rốt cuộc làm như thế nào mới là tốt. Rất may, mình đã tìm được một bài báo chỉ rõ vấn đề này."
# words = spark.sparkContext.parallelize(text.split(" "))
# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# for wc in wordCounts.collect():
#     print(wc[0], wc[1])

# spark.stop()

from pyspark.sql import SparkSession

# create a local SparkSession
spark = SparkSession.builder \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .appName("readExample") \
                .getOrCreate()

# define a streaming query
dataStreamWriter = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                        .option('spark.mongodb.input.uri', 'mongodb://admin:nhanbui@localhost:27017/test.coffeeshop?authSource=admin') \
                        .load()

print(dataStreamWriter.printSchema())