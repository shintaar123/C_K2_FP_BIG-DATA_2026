"""
predict_batch.py
Terapkan 3 model terlatih (category / importance / urgency) ke Silver layer.

Ini menambal gap penting: silver_transform.py menulis kolom category /
importance_label / urgency_label sebagai NULL (placeholder). Tanpa langkah ini
Gold layer tidak punya apa pun untuk diagregasi.

Alur:
  s3a://silver/news_silver  (category NULL)
        -> apply category_classifier  -> category
        -> apply importance_classifier -> importance_label
        -> apply urgency_classifier    -> urgency_label
  s3a://silver/news_scored  (Silver lengkap, siap diagregasi Gold)

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/ml/predict_batch.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString

SILVER_IN  = "s3a://silver/news_silver"
SILVER_OUT = "s3a://silver/news_scored"

CATEGORY_MODEL   = "s3a://mlflow/models/category_classifier"
IMPORTANCE_MODEL = "s3a://mlflow/models/importance_classifier"
URGENCY_MODEL    = "s3a://mlflow/models/urgency_classifier"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

spark = (
    SparkSession.builder.appName("predict-batch")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(">>> predict_batch: scoring Silver dengan model terlatih...")

silver = spark.read.format("delta").load(SILVER_IN)
total = silver.count()
print(f">>> Total record Silver: {total}")

# Buang kolom placeholder NULL supaya bisa diisi hasil prediksi
base = silver.drop("category", "importance_label", "urgency_label")


def apply_model(df, model_path, indexer_stage_input, out_col):
    """Load PipelineModel, transform, konversi index prediksi -> label asli.
    Mengembalikan df berisi (id, out_col)."""
    model = PipelineModel.load(model_path)
    labels = model.stages[0].labels  # StringIndexerModel ada di stage 0
    # StringIndexer butuh input col-nya ada; sediakan dummy agar transform tidak gagal.
    # Pakai label valid pertama supaya tidak ke-skip oleh handleInvalid="skip".
    dummy = labels[0] if labels else "rendah"
    tmp = df.withColumn(indexer_stage_input, F.lit(dummy).cast("string"))
    scored = model.transform(tmp)
    # output ke nama sementara supaya tidak bentrok dgn kolom dummy bernama sama
    i2s = IndexToString(inputCol="prediction", outputCol="__pred_out", labels=labels)
    return i2s.transform(scored).select("id", F.col("__pred_out").alias(out_col))


print(">>> Apply category_classifier...")
cat = apply_model(base, CATEGORY_MODEL, "category", "category")

print(">>> Apply importance_classifier...")
imp = apply_model(base, IMPORTANCE_MODEL, "importance_label", "importance_label")

print(">>> Apply urgency_classifier...")
urg = apply_model(base, URGENCY_MODEL, "urgency_label", "urgency_label")

scored = (
    base
    .join(cat, on="id", how="left")
    .join(imp, on="id", how="left")
    .join(urg, on="id", how="left")
    .withColumn("scored_at", F.current_timestamp())
)

print("\n>>> Preview hasil scoring:")
scored.select(
    F.col("clean_text").substr(1, 50).alias("text"),
    "category", "importance_label", "urgency_label",
    "importance_score", "urgency_score",
).show(10, truncate=False)

print("\n>>> Distribusi kategori hasil prediksi:")
scored.groupBy("category").count().orderBy("count", ascending=False).show()

print(f">>> Menulis Silver ter-skor ke {SILVER_OUT}...")
(
    scored.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .save(SILVER_OUT)
)

print(">>> SELESAI. news_scored siap dipakai gold_aggregate.py")
spark.stop()
