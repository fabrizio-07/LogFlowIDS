from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, concat_ws, to_timestamp, date_format, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import sys
import joblib
import pandas as pd

spark = (
    SparkSession.builder
        .appName("LogFlowIDS")
        .master("local[*]")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Loading ML models...")
MODEL_PATH = "/opt/spark-models/"
try:
    vectorizer = joblib.load(MODEL_PATH + "tfidf_model.joblib")
    model = joblib.load(MODEL_PATH + "isolation_forest_model.joblib")
    print("ML models loaded successfully.")
except Exception as e:
    print(f"FATAL ERROR: Could not load ML models from {MODEL_PATH}. {e}")
    print("This is a critical failure. The pipeline cannot run. Exiting.")
    sys.exit(1)

@pandas_udf(IntegerType())
def predict_udf(texts: pd.Series) -> pd.Series:

    try:
        texts_filled = texts.fillna("")

        text_vectors = vectorizer.transform(texts_filled)
        predictions = model.predict(text_vectors)

        is_suspicious = [1 if p == -1 else 0 for p in predictions]
        
        return pd.Series(is_suspicious)
    
    except Exception as e:
        print(f"FATAL ERROR during ML batch prediction: {e}")
        raise e

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
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
)

json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str")

parsed_stream = json_stream.select(from_json(col("json_str"), log_schema).alias("data")).select("data.*")
parsed_stream_with_date_obj = parsed_stream.withColumn(
    "timestamp_obj", 
    to_timestamp(col("timestamp"))
)
parsed_stream_with_date_str = parsed_stream_with_date_obj.withColumn(
    "timestamp", 
    date_format(col("timestamp_obj"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
)

logs_selected = parsed_stream_with_date_str.select(
    "traceID",
    "timestamp",
    "subsystem",
    "category",
    "eventType",
    "eventMessage",
    "processImagePath",
    "processID",
    "threadID"
)

logs_with_text = logs_selected.withColumn(
    "combined_text",
    concat_ws(" ", col("eventMessage"), col("processImagePath"))
)

rule_annotated = logs_with_text.withColumn(
    "rule_name",
    when(
        (
            col("processImagePath").rlike(r"/(bin|sbin)/launchctl$") & 
            col("eventMessage").rlike(r"\b(load|enable)\b")
        ), 
        lit("Rule 1: LaunchAgent/Daemon loading")
    ).when(
        col("processImagePath").rlike(r"/usr/bin/crontab$"), 
        lit("Rule 2: Crontab modification")
    ).when(
        col("eventMessage").contains("DYLD_INSERT_LIBRARIES"), 
        lit("Rule 3: Process injection")
    ).when(
        (
            col("processImagePath").rlike(r"/usr/sbin/spctl$") & 
            col("eventMessage").contains("--master-disable")
        ), 
        lit("Rule 4: Gatekeeper disabled")
    ).when(
        (
            col("processImagePath").rlike(r"/usr/bin/xattr$") & 
            col("eventMessage").rlike(r"(-d|-c).*com\.apple\.quarantine")
        ), 
        lit("Rule 5: Quarantine attribute cleared")
    ).when(
        (
            col("processImagePath").rlike(r"/usr/bin/log$") & 
            col("eventMessage").rlike(r"\b(erase|config)\b")
        ), 
        lit("Rule 6: Log clearing/tampering")
    ).when(
        col("processImagePath").rlike(r"/usr/bin/osascript$"), 
        lit("Rule 7: AppleScript execution")
    ).when(
        (
            col("processImagePath").rlike(r"/(sh|bash|zsh)$") & 
            col("eventMessage").rlike(r"base64.*(-d|-D|--decode)|(python|ruby|perl) -c")
        ), 
        lit("Rule 8: Obfuscated shell command")
    ).when(
        col("processImagePath").rlike(r"/usr/bin/(nc|netcat)$"), 
        lit("Rule 9: Netcat usage")
    ).when(
        col("processImagePath").rlike(r"/usr/sbin/system_profiler$"), 
        lit("Rule 10: System profiling")
    ).otherwise(lit("N/A"))
)

rule_flagged = rule_annotated.withColumn(
    "rule_is_suspicious",
    when(col("rule_name") != "N/A", lit(1)).otherwise(lit(0))
)

ml_flagged = rule_flagged.withColumn("ml_is_suspicious", predict_udf(col("combined_text")))
final_flagged = ml_flagged.withColumn(
    "is_suspicious",
    when(
        (col("rule_is_suspicious") == 1) | (col("ml_is_suspicious") == 1),
        lit(1)
    ).otherwise(lit(0))
)

output_df = final_flagged.select(
    "traceID",
    "timestamp",
    "subsystem",
    "category",
    "eventType",
    "eventMessage",
    "processImagePath",
    "processID",
    "threadID",
    "is_suspicious",        
    "rule_is_suspicious",   
    "ml_is_suspicious",
    "rule_name" 
)

def write_to_es(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    (batch_df
    .write
    .format("org.elasticsearch.spark.sql")
    .option("es.nodes", "elasticsearch")      
    .option("es.port", "9200")            
    .option("es.nodes.wan.only", "false")  
    .option("es.resource", "logs-enriched")  
    .option("es.mapping.id", "traceID")
    .option("es.batch.size.entries", "5000")
    .option("es.batch.size.bytes", "10485760")
    .option("es.write.operation", "upsert")
    .mode("append")
    .save()
    )

query = (
    output_df.writeStream 
        .outputMode("append")
        .foreachBatch(write_to_es)
        .option("checkpointLocation", "/tmp/spark-checkpoints/logflowids")
        .start()
)

query.awaitTermination()