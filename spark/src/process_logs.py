from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

dataSchema = (
    StructType()
    .add("timestamp", StringType())
    .add("host", StringType())
    .add("process", StringType())
    .add("message", StringType())
)

spark = SparkSession.builder.appName("LogFlowIDS").getOrCreate()

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "system_logs")
    .option("startingOffsets", "earliest")
    .load()
)

logs = df.selectExpr("CAST(value AS STRING) as value")

parsed = logs.select(from_json(col("value"), dataSchema).alias("data")).select("data.*")

query = (
    parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "/tmp/spark-checkpoints/logflowids")
    .start()
)

query.awaitTermination()