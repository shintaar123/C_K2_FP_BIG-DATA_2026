"""
Silver Transform Pipeline
Bronze (Delta Lake) -> NLP Cleaning -> Silver (Delta Lake)

Tanggung jawab ML Engineer (Zaenal):
- Baca data mentah dari Bronze
- Cleaning dan preprocessing teks
- Hitung importance_score dan urgency_score (formula dari implementation plan)
- Tulis hasil ke Silver layer di MinIO

Jalankan:
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/silver_transform.py
"""

import re
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType


# ─── Konfigurasi ────────────────────────────────────────────────────────────

BRONZE_PATH = "s3a://bronze/news_raw"
SILVER_PATH = "s3a://silver/news_silver"
CHECKPOINT  = "s3a://silver/_checkpoint_silver"

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

# ─── Spark Session ───────────────────────────────────────────────────────────

builder = (
    SparkSession.builder
    .appName("silver-transform")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",   "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "4")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> Silver Transform Pipeline dimulai...")

# ─── UDF: Text Cleaning ──────────────────────────────────────────────────────

def clean_text(text):
    """
    Cleaning teks mentah:
    1. Decode karakter unicode aneh (ð, â, dll dari RSS/YouTube)
    2. Hapus URL
    3. Hapus mention (@user) dan hashtag (#tag)
    4. Hapus karakter non-alfanumerik kecuali spasi dan tanda baca dasar
    5. Normalisasi spasi
    6. Lowercase
    """
    if text is None:
        return ""

    # Normalize unicode (tangani karakter ð â œ dll)
    try:
        text = unicodedata.normalize("NFKD", text)
        text = text.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass

    # Hapus URL
    text = re.sub(r"http\S+|www\.\S+", "", text)

    # Hapus mention dan hashtag (tapi simpan kata setelah #)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#(\w+)", r"\1", text)

    # Hapus karakter khusus, sisakan huruf/angka/spasi/titik/koma
    text = re.sub(r"[^\w\s.,!?]", " ", text)

    # Normalisasi spasi
    text = re.sub(r"\s+", " ", text).strip()

    # Lowercase
    text = text.lower()

    return text if len(text) > 5 else ""

clean_text_udf = F.udf(clean_text, StringType())

# ─── UDF: Extract Kecamatan ─────────────────────────────────────────────────

# Daftar kecamatan Surabaya (40 kecamatan)
KECAMATAN_SURABAYA = [
    "asemrowo", "benowo", "bubutan", "bulak", "dukuh pakis", "gayungan",
    "genteng", "gubeng", "gunung anyar", "jambangan", "karang pilang",
    "kenjeran", "krembangan", "lakarsantri", "mulyorejo", "pabean cantian",
    "pakal", "rungkut", "sambikerep", "sawahan", "semampir", "simokerto",
    "sukolilo", "sukomanunggal", "tambaksari", "tandes", "tegalsari",
    "tenggilis mejoyo", "wiyung", "wonocolo", "wonokromo",
    # variasi nama umum
    "ngagel", "nginden", "griya", "keputih", "medokan", "jemursari",
    "darmo", "mayjend sungkono", "ahmad yani",
]

def extract_kecamatan(text):
    if text is None:
        return ""
    text_lower = text.lower()
    for kec in KECAMATAN_SURABAYA:
        if kec in text_lower:
            return kec.title()
    return ""

extract_kecamatan_udf = F.udf(extract_kecamatan, StringType())

# ─── UDF: Importance Score ───────────────────────────────────────────────────
# Formula dari implementation plan:
# importance_score = (likes*0.4 + shares*0.3 + urgency_keywords*0.3) normalized 0-1

IMPORTANCE_KEYWORDS = [
    "darurat", "parah", "berbahaya", "bahaya", "kritis", "fatal",
    "banyak warga", "ribuan", "seluruh", "seluruh warga", "meluas",
    "berdampak", "lumpuh", "mati total", "rusak parah", "mendesak",
    "segera", "tolong", "minta tolong", "tidak bisa", "terpaksa",
]

def importance_score(text, likes, shares):
    if text is None:
        text = ""
    text_lower = text.lower()

    # Keyword score (0-1)
    keyword_hits = sum(1 for kw in IMPORTANCE_KEYWORDS if kw in text_lower)
    keyword_score = min(keyword_hits / 5.0, 1.0)  # cap di 5 keyword

    # Engagement score (normalized, cap likes=1000, shares=500)
    likes  = likes  if likes  is not None else 0
    shares = shares if shares is not None else 0
    engagement = min(likes / 1000.0, 1.0) * 0.4 + min(shares / 500.0, 1.0) * 0.3

    score = engagement + keyword_score * 0.3
    return round(min(score, 1.0), 4)

importance_score_udf = F.udf(importance_score, FloatType())

# ─── UDF: Urgency Score ──────────────────────────────────────────────────────
# urgency_score = time_sensitivity * 0.5 + impact_severity * 0.5

URGENCY_HIGH_KEYWORDS = [
    "sekarang", "hari ini", "dari tadi", "dari kemarin", "sudah lama",
    "berhari-hari", "berminggu-minggu", "berbulan-bulan", "belum ada respon",
    "tidak ada respon", "tidak kunjung", "tak kunjung", "mati total",
    "kebanjiran", "kebakaran", "terbakar", "kecelakaan", "berbahaya",
    "rawan", "gelap gulita", "bocor", "meluap", "terendam",
]

def urgency_score(text):
    if text is None:
        return 0.0
    text_lower = text.lower()
    hits = sum(1 for kw in URGENCY_HIGH_KEYWORDS if kw in text_lower)
    score = min(hits / 4.0, 1.0)
    return round(score, 4)

urgency_score_udf = F.udf(urgency_score, FloatType())

# ─── Baca Bronze ─────────────────────────────────────────────────────────────

print(f">>> Membaca Bronze dari {BRONZE_PATH}...")


bronze_df = spark.read.format("delta").load(BRONZE_PATH).dropDuplicates(["id"])
total = bronze_df.count()
print(f">>> Total record Bronze (deduplicated): {total}")

# ─── Transform ───────────────────────────────────────────────────────────────

print(">>> Menjalankan NLP cleaning dan scoring...")

silver_df = (
    bronze_df
    # 1. Clean teks
    .withColumn("clean_text", clean_text_udf(F.col("raw_text")))

    # 2. Filter: hapus baris dengan clean_text kosong atau terlalu pendek
    .filter(F.length(F.col("clean_text")) > 10)

    # 3. Extract kecamatan
    .withColumn("kecamatan", extract_kecamatan_udf(F.col("clean_text")))

    # 4. Hitung importance_score
    .withColumn(
        "importance_score",
        importance_score_udf(
            F.col("clean_text"),
            F.col("likes").cast("long"),
            F.col("shares").cast("long"),
        )
    )

    # 5. Hitung urgency_score
    .withColumn("urgency_score", urgency_score_udf(F.col("clean_text")))

    # 6. Placeholder kolom category dan label (diisi nanti oleh model)
    .withColumn("category",          F.lit(None).cast(StringType()))
    .withColumn("importance_label",  F.lit(None).cast(StringType()))
    .withColumn("urgency_label",     F.lit(None).cast(StringType()))

    # 7. Tambah timestamp transform
    .withColumn("transformed_at", F.current_timestamp())

    # 8. Pilih kolom final Silver
    .select(
        "id",
        "source_type",
        "source_name",
        "raw_text",
        "clean_text",
        "author",
        "url",
        "likes",
        "shares",
        "kecamatan",
        "importance_score",
        "urgency_score",
        "category",
        "importance_label",
        "urgency_label",
        "published_at",
        "scraped_at",
        "ingested_at",
        "transformed_at",
    )
)

# ─── Preview ─────────────────────────────────────────────────────────────────

print("\n>>> Preview Silver (5 baris):")
silver_df.select("id", "clean_text", "kecamatan", "importance_score", "urgency_score").show(5, truncate=60)
print(f">>> Total record Silver (setelah filter): {silver_df.count()}")

# ─── Tulis ke Silver Delta Lake ───────────────────────────────────────────────

print(f"\n>>> Menulis Silver ke {SILVER_PATH}...")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)

print(">>> SELESAI. Silver layer berhasil ditulis.")
print(f">>> Cek hasil di MinIO console -> bucket silver -> folder news_silver")
print(f">>> Kolom 'category', 'importance_label', 'urgency_label' masih NULL")
print(f">>> Akan diisi oleh model classifier setelah training selesai.")
