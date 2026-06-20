from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("test-delta-minio")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Koneksi ke MinIO (S3-compatible)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# Bikin data dummy: beberapa keluhan contoh
data = [
    (1, "Air mati di Tenggilis sejak pagi", "air"),
    (2, "Sampah menumpuk di TPS Mulyorejo", "sampah"),
    (3, "Jalan berlubang di MERR", "jalan"),
]
df = spark.createDataFrame(data, ["id", "raw_text", "category"])

path = "s3a://bronze/test_delta_table"

print(">>> Menulis ke MinIO sebagai Delta...")
df.write.format("delta").mode("overwrite").save(path)

print(">>> Membaca kembali dari MinIO...")
df_read = spark.read.format("delta").load(path)
df_read.show(truncate=False)

print(f">>> Jumlah baris ditulis: {df.count()}, dibaca: {df_read.count()}")
print(">>> SUKSES: Spark + MinIO + Delta Lake terhubung!" if df.count() == df_read.count() else ">>> GAGAL")

spark.stop()