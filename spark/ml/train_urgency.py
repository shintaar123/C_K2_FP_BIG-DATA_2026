"""
Train Urgency Classifier (Binary: tinggi / rendah)
Pipeline: TF-IDF -> Random Forest

Sesuai Section 5b implementation plan:
- Metrik utama: AUC-ROC (target >= 0.80)
- 5-fold CrossValidator untuk tuning numTrees & maxDepth
- Metrik pendukung: F1, Precision, Recall + areaUnderPR

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/ml/train_urgency.py
"""

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# ─── Konfigurasi ─────────────────────────────────────────────────────────────
TARGET_COL   = "urgency_label"
MODEL_OUTPUT = "s3a://mlflow/models/urgency_classifier"
EXPERIMENT   = "urgency-classifier"
RUN_NAME     = "urgency-rf-v1"

LABELED_CSV  = "/opt/spark/work-dir/data/labeled_samples_final.csv"
MLFLOW_URI   = "http://mlflow:5000"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
NUM_FOLDS = 5

spark = (
    SparkSession.builder.appName("train-urgency-classifier")
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

print(f">>> Training {EXPERIMENT} dimulai...")

df = (
    spark.read.option("header", True).option("multiLine", True).option("escape", '"')
    .csv(LABELED_CSV)
)
df = df.filter(
    F.col(TARGET_COL).isin("tinggi", "rendah") &
    F.col("clean_text").isNotNull() &
    (F.length(F.col("clean_text")) > 5)
)

print(f">>> Total sampel: {df.count()}")
print(f"\n>>> Distribusi {TARGET_COL}:")
df.groupBy(TARGET_COL).count().show()

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

STOP_WORDS_ID = [
    "yang","dan","di","ke","dari","untuk","dengan","ini","itu","ada","juga",
    "sudah","akan","saya","kami","kita","mereka","bisa","tidak","tak","belum",
    "telah","oleh","pada","dalam","adalah","atau","karena","jika","maka",
    "tapi","namun","saat","seperti","lebih","hanya","agar","ya","dong","nih","sih",
]

indexer = StringIndexer(inputCol=TARGET_COL, outputCol="label", handleInvalid="skip")
rf = RandomForestClassifier(featuresCol="features", labelCol="label", seed=42)

pipeline = Pipeline(stages=[
    indexer,
    Tokenizer(inputCol="clean_text", outputCol="words"),
    StopWordsRemover(inputCol="words", outputCol="filtered_words",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + STOP_WORDS_ID),
    HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=5000),
    IDF(inputCol="raw_features", outputCol="features", minDocFreq=1),
    rf,
])

auc_evaluator = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 150])
    .addGrid(rf.maxDepth, [5, 8, 12])
    .build()
)
cv = CrossValidator(
    estimator=pipeline, estimatorParamMaps=param_grid,
    evaluator=auc_evaluator, numFolds=NUM_FOLDS, parallelism=2, seed=42,
)

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT)

with mlflow.start_run(run_name=RUN_NAME) as run:
    print(f">>> MLflow Run ID: {run.info.run_id}")

    print(">>> Training dengan 5-fold CrossValidator...")
    cv_model = cv.fit(train_df)
    model = cv_model.bestModel
    best_rf = model.stages[-1]

    mlflow.log_param("target", TARGET_COL)
    mlflow.log_param("num_trees", best_rf.getNumTrees)
    mlflow.log_param("max_depth", best_rf.getMaxDepth())
    mlflow.log_param("cv_folds", NUM_FOLDS)

    print(">>> Evaluasi pada test set...")
    predictions = model.transform(test_df)

    auc = auc_evaluator.evaluate(predictions)
    aupr = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR").evaluate(predictions)
    f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
    acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    pr = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    rc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)

    print("\n>>> HASIL EVALUASI:")
    print(f"    AUC-ROC   : {auc:.4f}   <- metrik utama (target >= 0.80)")
    print(f"    AUC-PR    : {aupr:.4f}")
    print(f"    F1        : {f1:.4f}")
    print(f"    Accuracy  : {acc:.4f}")
    print(f"    Precision : {pr:.4f}")
    print(f"    Recall    : {rc:.4f}")

    mlflow.log_metric("auc_roc",   auc)
    mlflow.log_metric("auc_pr",    aupr)
    mlflow.log_metric("f1",        f1)
    mlflow.log_metric("accuracy",  acc)
    mlflow.log_metric("precision", pr)
    mlflow.log_metric("recall",    rc)

    labels = model.stages[0].labels
    i2s = IndexToString(inputCol="prediction", outputCol="pred_label", labels=labels)
    pred_readable = i2s.transform(predictions)

    print("\n>>> Prediksi vs Aktual (8 contoh):")
    pred_readable.select(
        F.col("clean_text").substr(1, 55).alias("text"),
        F.col(TARGET_COL).alias("aktual"),
        F.col("pred_label").alias("prediksi"),
    ).show(8, truncate=False)

    print(">>> Menyimpan model...")
    model.write().overwrite().save(MODEL_OUTPUT)
    print(f">>> SELESAI! Run ID: {run.info.run_id}")

spark.stop()
