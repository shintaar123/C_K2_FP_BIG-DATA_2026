"""
bronze_ingest_once.py
Versi BATCH SEKALI JALAN dari bronze_ingest.py.

Bedanya: pakai trigger(availableNow=True) -> Spark memproses SEMUA data yang sudah
ada di Kafka topic lalu OTOMATIS BERHENTI (tidak nunggu 5 menit, tidak perlu Ctrl+C).
Cocok untuk demo / run end-to-end yang lancar tanpa job streaming yang menggantung.

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/bronze_ingest_once.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP = "kafka:9094"
TOPICS = "raw-rss,raw-x,raw-reddit,raw-yt,raw-threads"
CHECKPOINT_PATH = "s3a://bronze/_checkpoints/news_raw"
BRONZE_PATH = "s3a://bronze/news_raw"

BRONZE_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("source_type", StringType()),
    StructField("source_name", StringType()),
    StructField("raw_text", StringType()),
    StructField("author", StringType()),
    StructField("url", StringType()),
    StructField("likes", IntegerType()),
    StructField("shares", IntegerType()),
    StructField("published_at", StringType()),
    StructField("scraped_at", StringType()),
])

spark = (
    SparkSession.builder.appName("bronze-ingest-once")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f">>> bronze_ingest_once: proses semua data Kafka({TOPICS}) -> {BRONZE_PATH} lalu berhenti...")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPICS)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_value", "topic AS kafka_topic")
    .select(from_json(col("json_value"), BRONZE_SCHEMA).alias("data"), col("kafka_topic"))
    .select("data.*", "kafka_topic")
    .withColumn("ingested_at", current_timestamp())
)

query = (
    parsed.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)   # proses semua yang tersedia lalu stop
    .start(BRONZE_PATH)
)
query.awaitTermination()

total = spark.read.format("delta").load(BRONZE_PATH).count()
print(f">>> SELESAI. Total record di Bronze sekarang: {total}")
spark.stop()
