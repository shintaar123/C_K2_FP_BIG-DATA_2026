"""
Train Category Classifier
Pipeline: TF-IDF (HashingTF + IDF) -> Random Forest -> 7 kategori
Dataset: data/labeled_samples_final.csv

Jalankan dari root repo:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/train_classifier.py
"""

import os
import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, IndexToString
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

LABELED_CSV   = "/opt/spark/work-dir/data/labeled_samples_final.csv"
MODEL_OUTPUT  = "s3a://mlflow/models/category_classifier"
MLFLOW_URI    = "http://mlflow:5000"
EXPERIMENT    = "category-classifier"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# ─── Spark Session ────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("train-category-classifier")
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

print(">>> Training Category Classifier dimulai...")

# ─── Load Dataset ─────────────────────────────────────────────────────────────

print(f">>> Load dataset dari {LABELED_CSV}...")
df = spark.read.csv(LABELED_CSV, header=True, inferSchema=True)

# Filter hanya baris yang punya label
df = df.filter(
    F.col("category").isNotNull() &
    F.col("clean_text").isNotNull() &
    (F.length(F.col("clean_text")) > 5)
)

total = df.count()
print(f">>> Total sampel berlabel: {total}")

# Distribusi per kategori
print("\n>>> Distribusi kategori:")
df.groupBy("category").count().orderBy("count", ascending=False).show()

# ─── Split Train/Test ─────────────────────────────────────────────────────────

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f">>> Train: {train_df.count()} | Test: {test_df.count()}")

# ─── Pipeline ML ─────────────────────────────────────────────────────────────

# 1. Label encoder: category string -> index angka
label_indexer = StringIndexer(
    inputCol="category",
    outputCol="label",
    handleInvalid="keep"
)

# 2. Tokenizer: split teks jadi kata-kata
tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")

# 3. Stop words remover (bahasa Indonesia + Inggris)
stop_words_id = [
    "yang", "dan", "di", "ke", "dari", "untuk", "dengan", "ini", "itu",
    "ada", "juga", "sudah", "akan", "saya", "kami", "kita", "mereka",
    "bisa", "tidak", "tak", "belum", "telah", "oleh", "pada", "dalam",
    "adalah", "atau", "karena", "jika", "maka", "tapi", "namun", "saat",
    "seperti", "lebih", "hanya", "agar", "atas", "bawah", "lagi", "pun",
    "ya", "dong", "nih", "sih", "deh", "kok", "lho", "wah", "ah",
    "tolong", "mohon", "minta", "harap",
]
remover = StopWordsRemover(
    inputCol="words",
    outputCol="filtered_words",
    stopWords=StopWordsRemover.loadDefaultStopWords("english") + stop_words_id
)

# 4. HashingTF: words -> term frequency vector
hashing_tf = HashingTF(
    inputCol="filtered_words",
    outputCol="raw_features",
    numFeatures=10000
)

# 5. IDF: bobot TF dengan inverse document frequency
idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=1)

# 6. Random Forest Classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=10,
    seed=42
)

# 7. Converter balik: index -> nama kategori
label_converter = IndexToString(
    inputCol="prediction",
    outputCol="predicted_category",
    labels=[]  # diisi dari label_indexer.labels setelah fit
)

pipeline = Pipeline(stages=[
    label_indexer,
    tokenizer,
    remover,
    hashing_tf,
    idf,
    rf,
])

# ─── MLflow Tracking ─────────────────────────────────────────────────────────

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT)

print(f"\n>>> Tracking MLflow di {MLFLOW_URI}, experiment: {EXPERIMENT}")

with mlflow.start_run(run_name="rf-tfidf-classifier-v1") as run:
    run_id = run.info.run_id
    print(f">>> MLflow Run ID: {run_id}")

    # Log parameter
    mlflow.log_param("num_trees", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("num_features_hashing", 10000)
    mlflow.log_param("train_size", train_df.count())
    mlflow.log_param("test_size", test_df.count())
    mlflow.log_param("total_categories", 7)

    # ─── Training ────────────────────────────────────────────────────────────

    print("\n>>> Training model...")
    model = pipeline.fit(train_df)

    # ─── Evaluasi ────────────────────────────────────────────────────────────

    print(">>> Evaluasi pada test set...")
    predictions = model.transform(test_df)

    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
    )
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall"
    )

    f1        = evaluator_f1.evaluate(predictions)
    accuracy  = evaluator_acc.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall    = evaluator_recall.evaluate(predictions)

    print(f"\n>>> HASIL EVALUASI:")
    print(f"    Accuracy  : {accuracy:.4f}")
    print(f"    F1 (macro): {f1:.4f}")
    print(f"    Precision : {precision:.4f}")
    print(f"    Recall    : {recall:.4f}")

    # Log metrik ke MLflow
    mlflow.log_metric("accuracy",  accuracy)
    mlflow.log_metric("f1_macro",  f1)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall",    recall)

    # Preview prediksi
    print("\n>>> Contoh prediksi (5 baris):")
    rf_model = model.stages[-1]
    label_model = model.stages[0]
    labels = label_model.labels

    predictions.select(
        "clean_text",
        "category",
        F.array(*[labels[i] for i in range(len(labels))]
            if len(labels) > 0 else F.lit("unknown")
        ).alias("label_map") if False else F.col("prediction"),
        F.col("prediction").cast("int"),
    ).show(5, truncate=50)

    # Tampilkan prediksi kategori yang lebih readable
    pred_with_label = predictions.withColumn(
        "predicted_category",
        F.when(F.col("prediction") == 0, labels[0] if len(labels) > 0 else "?")
    )
    for i, lbl in enumerate(labels):
        pred_with_label = pred_with_label.withColumn(
            "predicted_category",
            F.when(F.col("prediction") == i, lbl).otherwise(F.col("predicted_category"))
        )

    print("\n>>> Prediksi vs Aktual (10 contoh):")
    pred_with_label.select(
        F.col("clean_text").substr(1, 60).alias("text"),
        F.col("category").alias("aktual"),
        F.col("predicted_category").alias("prediksi")
    ).show(10, truncate=False)

    # Distribusi prediksi per kategori
    print("\n>>> Distribusi prediksi:")
    pred_with_label.groupBy("predicted_category").count().orderBy("count", ascending=False).show()

    # ─── Simpan Model ────────────────────────────────────────────────────────

    print(f"\n>>> Menyimpan model ke {MODEL_OUTPUT}...")
    model.write().overwrite().save(MODEL_OUTPUT)
    mlflow.log_param("model_path", MODEL_OUTPUT)

    print(f"\n>>> SELESAI! Training category classifier berhasil.")
    print(f">>> MLflow Run ID : {run_id}")
    print(f">>> Cek metrik di : {MLFLOW_URI}")
    print(f">>> Model tersimpan di bucket mlflow/models/category_classifier")

spark.stop()
