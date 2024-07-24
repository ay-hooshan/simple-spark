from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, base64, udf, upper
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
import pyspark.pandas as ps

spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()
          
# this line is 
spark.conf.set("spark.sql.streaming.checkpointLocation", "./spark-storagee")
          
# input from kafka topics
df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9094") \
      .option("subscribe", "test1") \
      .option("startingOffsets", "earliest") \
      .load()

# see the schema
print(df.dtypes) # [('key', 'binary'), ('value', 'binary'), ('topic', 'string'), ('partition', 'int'), ('offset', 'bigint'), ('timestamp', 'timestamp'), ('timestampType', 'int')]

df = df.withColumn("value", upper(col("value")))  # Example string transformation

# output to console
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .start()
    
# output to another kakfa topic    
query = df.writeStream.outputMode("append").format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9094") \
        .option("topic", "transformed") \
        .start()
    

query.awaitTermination()
