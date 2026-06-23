"""
Train Urgency Classifier (Binary: tinggi / rendah)
Pipeline: TF-IDF -> Random Forest
"""

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

LABELED_CSV  = "/opt/spark/work-dir/data/labeled_samples_final.csv"
MODEL_OUTPUT = "s3a://mlflow/models/urgency_classifier"
MLFLOW_URI   = "http://mlflow:5000"
EXPERIMENT   = "urgency-classifier"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

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

print(">>> Training Urgency Classifier dimulai...")

df = spark.read.csv(LABELED_CSV, header=True, inferSchema=True)
df = df.filter(
    F.col("urgency_label").isin("tinggi", "rendah") &
    F.col("clean_text").isNotNull() &
    (F.length(F.col("clean_text")) > 5)
)

print(f">>> Total sampel: {df.count()}")
print("\n>>> Distribusi urgency_label:")
df.groupBy("urgency_label").count().show()

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

STOP_WORDS_ID = [
    "yang","dan","di","ke","dari","untuk","dengan","ini","itu","ada","juga",
    "sudah","akan","saya","kami","kita","mereka","bisa","tidak","tak","belum",
    "telah","oleh","pada","dalam","adalah","atau","karena","jika","maka",
    "tapi","namun","saat","seperti","lebih","hanya","agar","ya","dong","nih","sih",
]

pipeline = Pipeline(stages=[
    StringIndexer(inputCol="urgency_label", outputCol="label", handleInvalid="keep"),
    Tokenizer(inputCol="clean_text", outputCol="words"),
    StopWordsRemover(inputCol="words", outputCol="filtered_words",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + STOP_WORDS_ID),
    HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=5000),
    IDF(inputCol="raw_features", outputCol="features", minDocFreq=1),
    RandomForestClassifier(featuresCol="features", labelCol="label",
        numTrees=100, maxDepth=8, seed=42),
])

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT)

with mlflow.start_run(run_name="urgency-rf-v1") as run:
    print(f">>> MLflow Run ID: {run.info.run_id}")
    mlflow.log_param("num_trees", 100)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("target", "urgency_label")

    print(">>> Training...")
    model = pipeline.fit(train_df)

    print(">>> Evaluasi...")
    predictions = model.transform(test_df)

    acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(predictions)
    f1  = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1").evaluate(predictions)
    pr  = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision").evaluate(predictions)
    rc  = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall").evaluate(predictions)

    print(f"\n>>> HASIL EVALUASI:")
    print(f"    Accuracy  : {acc:.4f}")
    print(f"    F1        : {f1:.4f}")
    print(f"    Precision : {pr:.4f}")
    print(f"    Recall    : {rc:.4f}")

    mlflow.log_metric("accuracy",  acc)
    mlflow.log_metric("f1",        f1)
    mlflow.log_metric("precision", pr)
    mlflow.log_metric("recall",    rc)

    labels = model.stages[0].labels
    pred = predictions
    for i, lbl in enumerate(labels):
        pred = pred.withColumn("pred_label",
            F.when(F.col("prediction") == i, lbl).otherwise(
                F.col("pred_label") if i > 0 else F.lit("?")))

    print("\n>>> Prediksi vs Aktual (8 contoh):")
    pred.select(
        F.col("clean_text").substr(1,55).alias("text"),
        F.col("urgency_label").alias("aktual"),
        F.col("pred_label").alias("prediksi")
    ).show(8, truncate=False)

    print(f">>> Menyimpan model...")
    model.write().overwrite().save(MODEL_OUTPUT)
    print(f">>> SELESAI! Run ID: {run.info.run_id}")

spark.stop()
