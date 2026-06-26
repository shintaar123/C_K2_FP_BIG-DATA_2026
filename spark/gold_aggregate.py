"""
gold_aggregate.py   (Pipeline + LLM Engineer — Angga)
Silver (ter-skor) -> Gold: agregasi harian per kecamatan & kategori + label 4-kuadran.

Input  : s3a://silver/news_scored   (hasil predict_batch.py)
Output : tabel Hive `gold.complaint_daily` (Delta di s3a://gold/warehouse)
         supaya bisa dibaca Trino -> Superset/Grafana.

Field Gold (sesuai Section 4 implementation plan):
  date, kecamatan, category, complaint_count, avg_importance, avg_urgency,
  quadrant (Q1/Q2/Q3/Q4), is_anomaly, complaint_growth_rate_3day,
  importance_high_ratio, urgency_high_ratio, priority_score_base

Quadrant (matriks Eisenhower):
  Q1 = penting & mendesak       (importance tinggi, urgency tinggi)  -> PRIORITAS UTAMA
  Q2 = penting, tidak mendesak  (importance tinggi, urgency rendah)
  Q3 = tidak penting, mendesak  (importance rendah, urgency tinggi)
  Q4 = tidak penting, tidak mendesak

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/gold_aggregate.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_IN  = "s3a://silver/news_scored"
GOLD_TABLE = "gold.complaint_daily"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# Ambang label tinggi (rasio record berlabel "tinggi" dalam satu grup)
IMPORTANCE_RATIO_THRESHOLD = 0.5
URGENCY_RATIO_THRESHOLD    = 0.5
# Ambang z-score untuk anomaly flag (butuh histori >1 hari per grup)
ANOMALY_ZSCORE = 2.0

spark = (
    SparkSession.builder.appName("gold-aggregate")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", "s3a://gold/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(">>> gold_aggregate: Silver -> Gold dimulai...")

silver = spark.read.format("delta").load(SILVER_IN)
print(f">>> Total record Silver ter-skor: {silver.count()}")

# ─── Normalisasi dimensi ─────────────────────────────────────────────────────
prepared = (
    silver
    # tanggal keluhan: utamakan published_at, fallback scraped_at, lalu hari ini
    .withColumn(
        "date",
        F.coalesce(
            F.to_date(F.col("published_at")),
            F.to_date(F.col("scraped_at")),
            F.current_date(),
        ),
    )
    # kecamatan kosong -> "tidak diketahui" supaya tetap teragregasi
    .withColumn(
        "kecamatan",
        F.when(
            (F.col("kecamatan").isNull()) | (F.trim(F.col("kecamatan")) == ""),
            F.lit("Tidak Diketahui"),
        ).otherwise(F.col("kecamatan")),
    )
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Lainnya")))
    .withColumn("imp_high", F.when(F.col("importance_label") == "tinggi", 1).otherwise(0))
    .withColumn("urg_high", F.when(F.col("urgency_label") == "tinggi", 1).otherwise(0))
)

# ─── Agregasi harian per (date, kecamatan, category) ─────────────────────────
agg = (
    prepared.groupBy("date", "kecamatan", "category")
    .agg(
        F.count("*").alias("complaint_count"),
        F.round(F.avg("importance_score"), 4).alias("avg_importance"),
        F.round(F.avg("urgency_score"), 4).alias("avg_urgency"),
        F.round(F.avg("imp_high"), 4).alias("importance_high_ratio"),
        F.round(F.avg("urg_high"), 4).alias("urgency_high_ratio"),
    )
)

# ─── Label 4-kuadran ─────────────────────────────────────────────────────────
imp_high = F.col("importance_high_ratio") >= IMPORTANCE_RATIO_THRESHOLD
urg_high = F.col("urgency_high_ratio") >= URGENCY_RATIO_THRESHOLD

agg = agg.withColumn(
    "quadrant",
    F.when(imp_high & urg_high, "Q1")
     .when(imp_high & ~urg_high, "Q2")
     .when(~imp_high & urg_high, "Q3")
     .otherwise("Q4"),
)

# ─── Growth rate 3 hari + anomaly flag (per kecamatan+category) ───────────────
w_hist = (
    Window.partitionBy("kecamatan", "category")
    .orderBy("date")
    .rowsBetween(-3, -1)  # 3 hari sebelumnya
)
w_all = Window.partitionBy("kecamatan", "category")

agg = (
    agg
    .withColumn("prev_avg_3day", F.avg("complaint_count").over(w_hist))
    .withColumn(
        "complaint_growth_rate_3day",
        F.when(
            F.col("prev_avg_3day").isNotNull() & (F.col("prev_avg_3day") > 0),
            F.round((F.col("complaint_count") - F.col("prev_avg_3day")) / F.col("prev_avg_3day"), 4),
        ).otherwise(F.lit(0.0)),
    )
    # z-score sederhana untuk anomaly (placeholder sebelum Isolation Forest ML Engineer)
    .withColumn("grp_mean", F.avg("complaint_count").over(w_all))
    .withColumn("grp_std", F.stddev_pop("complaint_count").over(w_all))
    .withColumn(
        "is_anomaly",
        F.when(
            (F.col("grp_std").isNotNull()) & (F.col("grp_std") > 0) &
            (((F.col("complaint_count") - F.col("grp_mean")) / F.col("grp_std")) >= ANOMALY_ZSCORE),
            True,
        ).otherwise(False),
    )
)

# ─── Priority score baseline (sebelum LLM) ───────────────────────────────────
# normalisasi count global supaya skala 0-1, gabung dengan importance & urgency
max_count = agg.agg(F.max("complaint_count")).collect()[0][0] or 1
agg = agg.withColumn(
    "priority_score_base",
    F.round(
        0.4 * F.col("avg_importance")
        + 0.4 * F.col("avg_urgency")
        + 0.2 * (F.col("complaint_count") / F.lit(float(max_count))),
        4,
    ),
)

gold = agg.select(
    "date", "kecamatan", "category", "complaint_count",
    "avg_importance", "avg_urgency",
    "importance_high_ratio", "urgency_high_ratio",
    "quadrant", "complaint_growth_rate_3day", "is_anomaly",
    "priority_score_base",
).orderBy(F.col("priority_score_base").desc())

print("\n>>> Preview Gold (urut prioritas):")
gold.show(20, truncate=False)
print(f">>> Total baris Gold: {gold.count()}")

print("\n>>> Distribusi kuadran:")
gold.groupBy("quadrant").count().orderBy("quadrant").show()

# ─── Tulis + register ke Hive (Delta) ─────────────────────────────────────────
# CATATAN BUG SCHEMA HIVE (sebelum fix ini):
#   `saveAsTable(..., mode="overwrite", overwriteSchema=true)` di atas tabel
#   yang sudah ada dengan tipe kolom berbeda (mis. kolom `date` pernah ditulis
#   sebagai STRING di run sebelumnya, lalu DATE di run ini) memicu:
#     "HiveExternalCatalog: Could not alter schema of table gold.complaint_daily
#      in a Hive compatible way"
#   Spark tetap menulis, tetapi sebagai format Spark-SQL-specific -> Trino TIDAK
#   bisa SELECT tabel tersebut (atau membaca dengan tipe salah).
#
# FIX (paling robust & idempoten):
#   1) DROP TABLE IF EXISTS  -> bersihkan entri metastore
#   2) Hapus path warehouse-nya  -> bersihkan _delta_log + parquet sisa run lama
#   3) saveAsTable  -> tabel baru selalu Hive/Trino-compatible
GOLD_DB        = "gold"
TABLE_NAME     = "complaint_daily"
TABLE_PATH_S3A = f"s3a://gold/warehouse/{GOLD_DB}.db/{TABLE_NAME}"

print(f"\n>>> Menulis & register tabel {GOLD_TABLE}...")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

print(f">>> Idempotent reset: DROP {GOLD_TABLE} + bersihkan {TABLE_PATH_S3A}")
try:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")
except Exception as e:
    print(f"    (warning) DROP TABLE gagal (lanjut): {e}")

# Bersihkan sisa file di MinIO supaya tidak ada konflik _delta_log lama
try:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(TABLE_PATH_S3A), hadoop_conf
    )
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(TABLE_PATH_S3A)
    if fs.exists(path_obj):
        fs.delete(path_obj, True)
        print(f"    Path {TABLE_PATH_S3A} dihapus.")
    else:
        print(f"    Path {TABLE_PATH_S3A} belum ada (run pertama), skip.")
except Exception as e:
    print(f"    (warning) bersih-bersih path gagal (lanjut tetap): {e}")

# Pastikan kolom `date` ber-tipe DATE (bukan STRING) supaya konsisten antar run
# dan Trino bisa langsung melakukan filter range tanggal.
gold = gold.withColumn("date", F.col("date").cast("date"))

(
    gold.write.format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

print(">>> SELESAI. Gold terdaftar di Hive Metastore, siap dibaca Trino/Superset.")
print(">>> Cek: SELECT * FROM delta.gold.complaint_daily; (di Trino)")
spark.stop()
