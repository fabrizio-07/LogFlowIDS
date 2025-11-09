from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, concat_ws, udf, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import re
import joblib

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
    print(f"ERROR: Could not load ML models from {MODEL_PATH}. {e}")
    vectorizer = None
    model = None

def predict_anomaly(text):

    if not vectorizer or not model:
        return 0
        
    try:
        if not text:
            return 0
        
        text_vector = vectorizer.transform([text])
        prediction = model.predict(text_vector)
        
        is_suspicious = 1 if prediction[0] == -1 else 0
        return is_suspicious
    except Exception as e:
        print(f"Error during ML prediction: {e}")
        return 0

predict_udf = udf(predict_anomaly, IntegerType())

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

suspicious_keywords = [
# privilege / system control
"sudo", "launchctl", "launchd", "csrutil", "spctl", "codesign",
# kernel / kext / injection
"kextload", "DYLD_INSERT_LIBRARIES", "task_for_pid",
# persistence / scheduling
"cron", "launchagents", "launchdaemons", 
# remote / network / transfer
"curl", "wget", "nc", "netcat", "ssh", "scp", "rsync", "ftp", "tftp",
# scripting / one-liners often used by attackers
"python -c", "ruby -e", "perl -e", "base64 -d", "bash -c", "zsh -c", 
# obfuscation / installer / package managers
"brew install", "installer", "open -a",
# destructive / commands used by attackers
"rm -rf", "chmod +x", "chown", "dd",
# macOS specific APIs / automation
"osascript", "screen sharing", 
# generic malware keywords
"malware", "suspicious", "reverse shell", "unauthorized", "attack", "exploit"
]

escaped = [re.escape(k) for k in suspicious_keywords]
pattern = r"(?i)\b(" + "|".join(escaped) + r")\b"

logs_with_text = logs_selected.withColumn(
    "combined_text",
    concat_ws(" ", col("eventMessage"), col("processImagePath"))
)

rule_flagged = logs_with_text.withColumn(
    "rule_is_suspicious",
    when(col("combined_text").rlike(pattern), lit(1)).otherwise(lit(0))
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
    "ml_is_suspicious"      
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