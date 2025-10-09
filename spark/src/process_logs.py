from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, regexp_extract, when, lit, coalesce, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import re

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

logs_safe = (
    logs_selected
    .withColumn("eventMessage_safe", coalesce(col("eventMessage"), lit("")))
    .withColumn("processImagePath_safe", coalesce(col("processImagePath"), lit("")))
    .withColumn("combined_text", col("eventMessage_safe") + lit(" ") + col("processImagePath_safe"))
)

flagged = logs_safe.withColumn(
    "matched_keyword", regexp_extract(col("combined_text"), pattern, 1)
)

score_expr = " + ".join([
    f"(CASE WHEN combined_text RLIKE '(?i)\\\\b{re.escape(k)}\\\\b' THEN 1 ELSE 0 END)"
    for k in suspicious_keywords
])
flagged = flagged.withColumn("suspicion_score", expr(score_expr))

flagged = flagged.withColumn(
    "is_suspicious",
    when(col("suspicion_score") > 0, lit(1)).otherwise(lit(0))
)

output_df = flagged.select(
"timestamp",
"subsystem",
"category",
"eventType",
"eventMessage",
"processImagePath",
"processID",
"threadID",
"matched_keyword",
"suspicion_score",
"is_suspicious"
)

query = (
    output_df.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/spark-checkpoints/logflowids")
        .start()
)

query.awaitTermination()