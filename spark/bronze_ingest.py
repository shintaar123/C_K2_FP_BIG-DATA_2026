"""
bronze_ingest.py
Spark Structured Streaming: Kafka (raw-rss, raw-x, raw-reddit, raw-yt, raw-threads) -> Bronze Delta Lake (MinIO).
Ini bagian terakhir tanggung jawab Ingestion Engineer: scraper -> Kafka -> Bronze.
Setelah ini, tanggung jawab pindah ke ML/Pipeline Engineer (Bronze -> Silver -> Gold).

CATATAN: ini versi STREAMING (jalan terus, hentikan dengan Ctrl+C setelah data masuk).
Untuk batch sekali jalan yang otomatis berhenti, pakai `bronze_ingest_once.py`.

Cara jalanin (di dalam container spark-master, file ini ke-mount otomatis ke
/opt/spark/work-dir/app/bronze_ingest.py karena folder ./spark di-mount di docker-compose):

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-aws:3.3.4 \\
      --conf spark.jars.ivy=/tmp/.ivy \\
      /opt/spark/work-dir/app/bronze_ingest.py

NOTE: versi spark-sql-kafka HARUS sama dengan versi image Spark (3.5.3). Kalau tidak cocok,
error-nya jelas ("provider org.apache.spark.sql.kafka010... not found") -> ganti versi paketnya.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP = "kafka:9094"  # listener INTERNAL, dipakai antar-container (bukan localhost)
TOPICS = "raw-rss,raw-x,raw-reddit,raw-yt,raw-threads"  # subscribe semua sumber
CHECKPOINT_PATH = "s3a://bronze/_checkpoints/news_raw"
BRONZE_PATH = "s3a://bronze/news_raw"

# Skema HARUS sama persis dengan skema yang dihasilkan base_scraper.py (lihat docstring di sana).
# Kalau scraper nambah field baru, update skema ini juga, kalau enggak field baru itu akan ke-drop.
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


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-ingest-kafka-to-delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")  # sprint awal: ambil semua histori topic
        .load()
    )

    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value", "topic AS kafka_topic")
        .select(
            from_json(col("json_value"), BRONZE_SCHEMA).alias("data"),
            col("kafka_topic"),
        )
        .select("data.*", "kafka_topic")
        .withColumn("ingested_at", current_timestamp())
    )

    query = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="5 minutes")  # batch tiap 5 menit, cukup buat skala harian
        .start(BRONZE_PATH)
    )

    print(f">>> Streaming Kafka({TOPICS}) -> Bronze Delta ({BRONZE_PATH}) berjalan...")
    print(">>> Tekan Ctrl+C / stop container untuk berhenti. Cek progress di Spark UI :8080")
    query.awaitTermination()


if __name__ == "__main__":
    main()
