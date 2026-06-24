"""
train_anomaly.py  (ML Engineer — Anomaly Detection)
Isolation Forest untuk deteksi lonjakan keluhan tidak wajar pada Gold layer.

Sesuai Section 5 implementation plan:
- Anomaly Detection — Isolation Forest: deteksi lonjakan tidak wajar pada
  time-series Gold layer.
- Metrik: Precision@K (K = anomali yang dikonfirmasi manual), target >= 0.70

Model ini bekerja di atas data Gold (agregasi harian per kecamatan & kategori),
membuat fitur time-series sederhana, lalu mendeteksi baris yang "tidak wajar"
dibandingkan distribusi normalnya.

Karena Spark MLlib tidak menyediakan Isolation Forest bawaan, kita menggunakan
scikit-learn IsolationForest yang di-broadcast ke executor Spark.
Hasilnya di-log ke MLflow dan model disimpan sebagai artifact.

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/ml/train_anomaly.py
"""

import os
import pickle
import tempfile

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─── Konfigurasi ──────────────────────────────────────────────────────────────

GOLD_TABLE     = "gold.complaint_daily"
GOLD_PATH      = "s3a://gold/warehouse"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

MLFLOW_URI       = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")

# Isolation Forest hyperparameters
CONTAMINATION = 0.15      # estimasi 15% data adalah anomali
N_ESTIMATORS  = 100       # jumlah tree
RANDOM_STATE  = 42

# ─── Spark Session ────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder.appName("train-anomaly-isolation-forest")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.sql.warehouse.dir", GOLD_PATH)
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

print(">>> train_anomaly: Memulai training Isolation Forest...")

# ─── Baca Gold Layer ──────────────────────────────────────────────────────────

try:
    gold = spark.sql(f"SELECT * FROM {GOLD_TABLE}")
    total = gold.count()
    print(f">>> Total record Gold: {total}")
except Exception:
    print(">>> Tabel Gold belum ada, coba baca dari path langsung...")
    gold = spark.read.format("delta").load(f"{GOLD_PATH}/{GOLD_TABLE.replace('.', '/')}")
    total = gold.count()
    print(f">>> Total record Gold (dari path): {total}")

if total == 0:
    print(">>> SKIP: Gold layer kosong, tidak ada data untuk training anomaly.")
    spark.stop()
    exit(0)

# ─── Feature Engineering ─────────────────────────────────────────────────────
# Fitur yang digunakan untuk mendeteksi anomali:
# 1. complaint_count          : jumlah keluhan hari itu
# 2. avg_importance           : rata-rata skor kepentingan
# 3. avg_urgency              : rata-rata skor kemendesakan
# 4. complaint_growth_rate_3day: laju pertumbuhan 3 hari
# 5. importance_high_ratio    : rasio keluhan ber-importance tinggi
# 6. urgency_high_ratio       : rasio keluhan ber-urgency tinggi

FEATURE_COLS = [
    "complaint_count",
    "avg_importance",
    "avg_urgency",
    "complaint_growth_rate_3day",
    "importance_high_ratio",
    "urgency_high_ratio",
]

# Pastikan semua kolom fitur ada dan isi null dengan 0
features_df = gold.select("date", "kecamatan", "category", *FEATURE_COLS)
for col in FEATURE_COLS:
    features_df = features_df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

# Kumpulkan ke driver (Gold layer relatif kecil: puluhan-ratusan baris per hari)
pdf = features_df.toPandas()
X = pdf[FEATURE_COLS].values.astype(np.float64)

print(f">>> Shape fitur untuk Isolation Forest: {X.shape}")
print(f">>> Kolom fitur: {FEATURE_COLS}")

# ─── Train Isolation Forest ──────────────────────────────────────────────────

from sklearn.ensemble import IsolationForest

model = IsolationForest(
    n_estimators=N_ESTIMATORS,
    contamination=CONTAMINATION,
    random_state=RANDOM_STATE,
    n_jobs=-1,
)

model.fit(X)

# Prediksi: -1 = anomali, 1 = normal
predictions = model.predict(X)
scores = model.decision_function(X)  # skor anomali (semakin rendah = semakin anomali)

n_anomalies = int((predictions == -1).sum())
n_normal    = int((predictions == 1).sum())
anomaly_ratio = n_anomalies / len(predictions) if len(predictions) > 0 else 0

print(f"\n>>> Hasil Deteksi:")
print(f"    Normal   : {n_normal}")
print(f"    Anomali  : {n_anomalies}")
print(f"    Rasio    : {anomaly_ratio:.2%}")

# ─── Evaluasi: Precision@K ───────────────────────────────────────────────────
# Karena ini unsupervised, kita hitung Precision@K terhadap z-score anomaly
# yang sudah ada di Gold layer (kolom is_anomaly dari gold_aggregate.py).
# Ini berfungsi sebagai "pseudo ground-truth".

pdf["iso_anomaly"]   = (predictions == -1)
pdf["iso_score"]     = scores

try:
    gold_with_flag = gold.select("date", "kecamatan", "category", "is_anomaly").toPandas()
    pdf_merged = pdf.merge(gold_with_flag, on=["date", "kecamatan", "category"], how="left")
    
    # Ground truth dari z-score
    zscore_anomalies = pdf_merged["is_anomaly"].fillna(False).astype(bool)
    iso_anomalies    = pdf_merged["iso_anomaly"]
    
    # Precision@K: dari semua yang IF deteksi sebagai anomali, berapa yang cocok dengan z-score
    if iso_anomalies.sum() > 0:
        true_positives = (iso_anomalies & zscore_anomalies).sum()
        precision_at_k = true_positives / iso_anomalies.sum()
    else:
        precision_at_k = 0.0
    
    # Recall: dari semua z-score anomali, berapa yang IF juga deteksi
    if zscore_anomalies.sum() > 0:
        recall = (iso_anomalies & zscore_anomalies).sum() / zscore_anomalies.sum()
    else:
        recall = 0.0
    
    print(f"\n>>> Evaluasi vs z-score baseline:")
    print(f"    Precision@K : {precision_at_k:.4f}")
    print(f"    Recall      : {recall:.4f}")
    
except Exception as e:
    print(f">>> Tidak bisa menghitung Precision@K (is_anomaly mungkin belum ada): {e}")
    precision_at_k = 0.0
    recall = 0.0

# ─── Log ke MLflow ────────────────────────────────────────────────────────────

try:
    import mlflow
    import mlflow.sklearn
    
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("anomaly-detection")
    
    with mlflow.start_run(run_name="isolation-forest-gold") as run:
        # Log parameters
        mlflow.log_param("model_type", "IsolationForest")
        mlflow.log_param("n_estimators", N_ESTIMATORS)
        mlflow.log_param("contamination", CONTAMINATION)
        mlflow.log_param("random_state", RANDOM_STATE)
        mlflow.log_param("feature_cols", ",".join(FEATURE_COLS))
        mlflow.log_param("training_samples", len(X))
        
        # Log metrics
        mlflow.log_metric("n_anomalies", n_anomalies)
        mlflow.log_metric("n_normal", n_normal)
        mlflow.log_metric("anomaly_ratio", anomaly_ratio)
        mlflow.log_metric("precision_at_k", precision_at_k)
        mlflow.log_metric("recall", recall)
        
        # Log model
        mlflow.sklearn.log_model(model, artifact_path="isolation_forest_model")
        
        # Juga simpan model sebagai pickle ke S3 agar predict_batch bisa menggunakannya
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
            pickle.dump(model, f)
            tmp_path = f.name
        mlflow.log_artifact(tmp_path, artifact_path="model_pkl")
        os.unlink(tmp_path)
        
        print(f"\n>>> MLflow Run ID: {run.info.run_id}")
        print(f">>> Experiment: anomaly-detection")
        print(f">>> Model berhasil di-log ke MLflow!")

except Exception as e:
    print(f">>> MLflow logging gagal (non-fatal): {e}")
    # Simpan model lokal sebagai fallback
    fallback_path = "/tmp/isolation_forest_model.pkl"
    with open(fallback_path, "wb") as f:
        pickle.dump(model, f)
    print(f">>> Model disimpan lokal di: {fallback_path}")

# ─── Update Gold table dengan kolom is_anomaly dari Isolation Forest ──────────

print("\n>>> Mengupdate Gold layer dengan hasil Isolation Forest...")

# Buat Spark DataFrame dari hasil prediksi
import pandas as pd

result_pdf = pdf[["date", "kecamatan", "category", "iso_anomaly", "iso_score"]].copy()
result_pdf["date"] = pd.to_datetime(result_pdf["date"])

result_spark = spark.createDataFrame(result_pdf)
result_spark = (
    result_spark
    .withColumnRenamed("iso_anomaly", "is_anomaly_if")
    .withColumnRenamed("iso_score", "anomaly_score_if")
)

# Join kembali ke Gold dan update is_anomaly
gold_updated = (
    gold.drop("is_anomaly")  # Hapus is_anomaly z-score lama
    .join(
        result_spark.select("date", "kecamatan", "category", "is_anomaly_if", "anomaly_score_if"),
        on=["date", "kecamatan", "category"],
        how="left",
    )
    .withColumn("is_anomaly", F.coalesce(F.col("is_anomaly_if"), F.lit(False)))
    .withColumn("anomaly_score", F.coalesce(F.col("anomaly_score_if"), F.lit(0.0)))
    .drop("is_anomaly_if", "anomaly_score_if")
)

# Tulis ulang ke Gold table
gold_updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)

print(">>> Gold layer berhasil diperbarui dengan deteksi anomali Isolation Forest!")
print(f">>> Total anomali terdeteksi: {n_anomalies} dari {total} baris")

# Preview
print("\n>>> Preview data ANOMALI (is_anomaly = true):")
gold_updated.filter(F.col("is_anomaly") == True).show(10, truncate=False)

print("\n>>> SELESAI. Isolation Forest telah dilatih dan Gold layer diperbarui.")
spark.stop()
