"""
Train Category Classifier
Pipeline: TF-IDF (HashingTF + IDF) -> Random Forest -> 7 kategori
Dataset: data/labeled_samples_final.csv

Sesuai Section 5b implementation plan:
- 5-fold CrossValidator untuk tuning numTrees & maxDepth
- Metrik utama: F1-macro (dihitung manual via fMeasureByLabel, BUKAN weighted)
- Confusion matrix di-log ke MLflow setiap run
- Semua metrik & confusion matrix tercatat di MLflow Tracking

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/ml/train_classifier.py
"""

import json
import tempfile

import mlflow
import mlflow.spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, IndexToString
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ─── Konfigurasi ─────────────────────────────────────────────────────────────

LABELED_CSV  = "/opt/spark/work-dir/data/labeled_samples_final.csv"
MODEL_OUTPUT = "s3a://mlflow/models/category_classifier"
MLFLOW_URI   = "http://mlflow:5000"
EXPERIMENT   = "category-classifier"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

NUM_FOLDS = 5  # Section 5b: 5-fold cross-validation

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
# multiLine + escape supaya teks berkoma dalam tanda kutip tidak terpotong
df = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("escape", '"')
    .csv(LABELED_CSV)
)

df = df.filter(
    F.col("category").isNotNull() &
    F.col("clean_text").isNotNull() &
    (F.length(F.col("clean_text")) > 5)
)

total = df.count()
print(f">>> Total sampel berlabel: {total}")

print("\n>>> Distribusi kategori:")
df.groupBy("category").count().orderBy("count", ascending=False).show()

# ─── Split Train/Test ─────────────────────────────────────────────────────────

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
print(f">>> Train: {train_df.count()} | Test: {test_df.count()}")

# ─── Definisi Pipeline ────────────────────────────────────────────────────────

label_indexer = StringIndexer(inputCol="category", outputCol="label", handleInvalid="keep")
tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")

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
    inputCol="words", outputCol="filtered_words",
    stopWords=StopWordsRemover.loadDefaultStopWords("english") + stop_words_id,
)
hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=10000)
idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=1)
rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

pipeline = Pipeline(stages=[label_indexer, tokenizer, remover, hashing_tf, idf, rf])

# ─── Hyperparameter tuning (5-fold CV) ──────────────────────────────────────

# F1 weighted dipakai sebagai objective CV (Spark tak punya macro langsung);
# F1-macro final dihitung manual di test set untuk pelaporan (Section 5b).
cv_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)
param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 150])
    .addGrid(rf.maxDepth, [5, 10, 15])
    .build()
)
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=cv_evaluator,
    numFolds=NUM_FOLDS,
    parallelism=2,
    seed=42,
)


# ─── Helper: F1-macro manual ─────────────────────────────────────────────────

def macro_f1(predictions, n_labels):
    """Rata-rata F1 per kelas (unweighted) = F1-macro sesungguhnya."""
    scores = []
    for i in range(n_labels):
        ev = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction",
            metricName="fMeasureByLabel", metricLabel=float(i),
        )
        try:
            scores.append(ev.evaluate(predictions))
        except Exception:
            pass
    return sum(scores) / len(scores) if scores else 0.0


# ─── MLflow Tracking ─────────────────────────────────────────────────────────

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT)
print(f"\n>>> Tracking MLflow di {MLFLOW_URI}, experiment: {EXPERIMENT}")

with mlflow.start_run(run_name="rf-tfidf-classifier-v1") as run:
    run_id = run.info.run_id
    print(f">>> MLflow Run ID: {run_id}")

    print("\n>>> Training dengan 5-fold CrossValidator...")
    cv_model = cv.fit(train_df)
    model = cv_model.bestModel
    best_rf = model.stages[-1]

    best_num_trees = best_rf.getNumTrees
    best_max_depth = best_rf.getMaxDepth()
    print(f">>> Best params -> numTrees={best_num_trees}, maxDepth={best_max_depth}")

    mlflow.log_param("num_trees", best_num_trees)
    mlflow.log_param("max_depth", best_max_depth)
    mlflow.log_param("num_features_hashing", 10000)
    mlflow.log_param("cv_folds", NUM_FOLDS)
    mlflow.log_param("train_size", train_df.count())
    mlflow.log_param("test_size", test_df.count())

    labels = model.stages[0].labels
    mlflow.log_param("total_categories", len(labels))

    # ─── Evaluasi test set ────────────────────────────────────────────────────
    print(">>> Evaluasi pada test set...")
    predictions = model.transform(test_df)

    f1_weighted = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
    f1_macro = macro_f1(predictions, len(labels))
    accuracy = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    precision = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    recall = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)

    print("\n>>> HASIL EVALUASI:")
    print(f"    Accuracy     : {accuracy:.4f}")
    print(f"    F1 (macro)   : {f1_macro:.4f}   <- metrik utama (target >= 0.75)")
    print(f"    F1 (weighted): {f1_weighted:.4f}")
    print(f"    Precision    : {precision:.4f}")
    print(f"    Recall       : {recall:.4f}")

    mlflow.log_metric("accuracy",    accuracy)
    mlflow.log_metric("f1_macro",    f1_macro)
    mlflow.log_metric("f1_weighted", f1_weighted)
    mlflow.log_metric("precision",   precision)
    mlflow.log_metric("recall",      recall)

    # ─── Prediksi readable + confusion matrix ──────────────────────────────────
    i2s = IndexToString(inputCol="prediction", outputCol="predicted_category", labels=labels)
    pred_readable = i2s.transform(predictions)

    print("\n>>> Prediksi vs Aktual (10 contoh):")
    pred_readable.select(
        F.col("clean_text").substr(1, 60).alias("text"),
        F.col("category").alias("aktual"),
        F.col("predicted_category").alias("prediksi"),
    ).show(10, truncate=False)

    print("\n>>> Confusion Matrix (aktual x prediksi):")
    cm = (
        pred_readable.groupBy("category")
        .pivot("predicted_category")
        .count()
        .fillna(0)
        .orderBy("category")
    )
    cm.show(truncate=False)

    # Log confusion matrix sebagai artifact JSON
    cm_rows = [r.asDict() for r in cm.collect()]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(cm_rows, fh, indent=2, ensure_ascii=False)
        cm_path = fh.name
    mlflow.log_artifact(cm_path, artifact_path="confusion_matrix")

    # ─── Simpan Model ──────────────────────────────────────────────────────────
    print(f"\n>>> Menyimpan model ke {MODEL_OUTPUT}...")
    model.write().overwrite().save(MODEL_OUTPUT)
    mlflow.log_param("model_path", MODEL_OUTPUT)

    print(f"\n>>> SELESAI! Run ID: {run_id} | Cek metrik di {MLFLOW_URI}")

spark.stop()
