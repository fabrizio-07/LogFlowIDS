from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

spark = (
    SparkSession.builder
        .appName("LogFlowIDS")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

log_schema = StructType([
    StructField("traceID", StringType()),
    StructField("eventMessage", StringType()),
    StructField("eventType", StringType()),
    StructField("subsystem", StringType()),
    StructField("category", StringType()),
    StructField("processImagePath", StringType()),
    StructField("timestamp", StringType()),
    StructField("processID", LongType()),
    StructField("threadID", LongType()),
    StructField("senderImagePath", StringType()),
    StructField("messageType", StringType()),
])

raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "macos_logs")
        .option("startingOffsets", "latest")
        .load()
)

json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

parsed_stream = json_stream.select(from_json(col("json_str"), log_schema).alias("data")).select("data.*")

logs_selected = parsed_stream.select(
    "timestamp",
    "subsystem",
    "category",
    "eventType",
    "eventMessage",
    "processImagePath",
    "processID",
    "threadID"
)

query = (
    logs_selected.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
)

query.awaitTermination()