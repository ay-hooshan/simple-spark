from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, expr, upper
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType
import pyspark.pandas as ps

# Create Spark session
spark = SparkSession.builder.appName("APP").getOrCreate()
          
# Set checkpoint location
spark.conf.set("spark.sql.streaming.checkpointLocation", "./spark-storagee")
          
# Define schema for the JSON data (example schema)
json_schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True)
])

number_test_schema = StructType([
    StructField("x", StringType(), True),
])

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "testnum") \
    .option("startingOffsets", "earliest") \
    .load()

# Print schema
print(df.dtypes)

# Transform the data
# 1. Decode the value from binary to string
df = df.withColumn("value", col("value").cast("string"))

raw_json_query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 2. Parse the JSON content
df = df.withColumn("jsonData", from_json(col("value"), number_test_schema))

# 3. Select the fields from the JSON data and add a new field by transforming the string
df = df.select(col("key").cast("string"), col("jsonData.*"))

parsed_json_query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

df = df.withColumn("XX", col('x').cast(IntegerType()) * 2)  # Example string transformation

# Output to console
query_console = df.writeStream \
    .format("console") \
    .start()

# Output to another Kafka topic
query_kafka = df.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("topic", "transformed") \
    .start()
    
raw_json_query.awaitTermination(10)
parsed_json_query.awaitTermination(10)
query_console.awaitTermination()
query_kafka.awaitTermination()
