from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("register-delta-table")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.uris", "thrift://hive-metastore:9083")
    # arahkan warehouse Spark ke MinIO -> Spark yang tulis ke S3, bukan Hive
    .config("spark.sql.warehouse.dir", "s3a://gold/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .enableHiveSupport()
    .getOrCreate()
)

# database tanpa LOCATION -> Hive tak perlu menyentuh S3 sendiri
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

data = [
    ("2026-06-19", "Tenggilis Mejoyo", "air", 42, 0.85, 0.90, "Q1"),
    ("2026-06-19", "Mulyorejo", "sampah", 18, 0.65, 0.40, "Q2"),
    ("2026-06-19", "Gubeng", "jalan", 9, 0.55, 0.30, "Q3"),
]
cols = ["date", "kecamatan", "category", "complaint_count", "avg_importance", "avg_urgency", "quadrant"]
df = spark.createDataFrame(data, cols)

# tabel ditulis ke s3a://gold/warehouse/gold.db/complaint_daily oleh Spark
df.write.format("delta").mode("overwrite").saveAsTable("gold.complaint_daily")

print(">>> Tabel gold.complaint_daily berhasil dibuat & terdaftar di Hive")
spark.sql("SELECT * FROM gold.complaint_daily").show(truncate=False)
spark.stop()