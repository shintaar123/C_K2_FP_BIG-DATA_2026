"""
llm_enrichment.py  (Pipeline + LLM Engineer)
Gold -> LLM-Gold: kirim masalah Q1+Q2 ke LLM, simpan rekomendasi.

Alur (Section 6 implementation plan):
- Baca tabel gold.complaint_daily
- Ambil hanya kuadran Q1 & Q2 (yang dianggap penting), maksimum 20 cluster/hari
- Untuk tiap cluster: ambil <=3 contoh keluhan dari Silver sebagai konteks
- Panggil LLM (Gemini -> NVIDIA -> Groq -> Cerebras -> rule-based) via llm_client
- Tulis hasil ke tabel Hive gold.complaint_enriched (LLM-Gold)

Jalankan (set API key via -e atau .env):
docker compose exec -e GEMINI_API_KEY=xxx spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/llm/llm_enrichment.py
"""

import os
import sys

# supaya bisa import llm_client & prompt_templates yang sefolder
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from llm_client import enrich
from prompt_templates import build_prompt

SILVER_IN       = "s3a://silver/news_scored"
GOLD_TABLE      = "gold.complaint_daily"
ENRICHED_TABLE  = "gold.complaint_enriched"
MAX_CLUSTERS    = 20
TARGET_QUADRANTS = ["Q1", "Q2"]

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"

spark = (
    SparkSession.builder.appName("llm-enrichment")
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

print(">>> llm_enrichment: Gold -> LLM-Gold dimulai...")


def _safe_int(value, default=0):
    """Konversi aman ke int (LLM kadang balas '9', '3 days', atau 9.0)."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    import re as _re
    m = _re.search(r"-?\d+", str(value))
    return int(m.group()) if m else default


# ─── Ambil cluster Q1+Q2 prioritas tertinggi ─────────────────────────────────
gold = spark.table(GOLD_TABLE).filter(F.col("quadrant").isin(TARGET_QUADRANTS))
top = (
    gold.orderBy(F.col("priority_score_base").desc())
    .limit(MAX_CLUSTERS)
)
top_rows = top.collect()
print(f">>> {len(top_rows)} cluster Q1/Q2 akan dikirim ke LLM (maks {MAX_CLUSTERS}).")

if not top_rows:
    print(">>> Tidak ada cluster Q1/Q2. Selesai tanpa enrichment.")
    spark.stop()
    sys.exit(0)

# ─── Siapkan contoh teks per (date, kecamatan, category) dari Silver ─────────
silver = spark.read.format("delta").load(SILVER_IN)
samples_df = (
    silver
    .withColumn(
        "date",
        F.coalesce(F.to_date("published_at"), F.to_date("scraped_at"), F.current_date()),
    )
    .withColumn(
        "kecamatan",
        F.when((F.col("kecamatan").isNull()) | (F.trim("kecamatan") == ""), F.lit("Tidak Diketahui"))
         .otherwise(F.col("kecamatan")),
    )
    .withColumn("category", F.coalesce(F.col("category"), F.lit("Lainnya")))
    .groupBy("date", "kecamatan", "category")
    .agg(F.slice(F.collect_list("clean_text"), 1, 3).alias("samples"))
)
samples_map = {
    (r["date"], r["kecamatan"], r["category"]): r["samples"]
    for r in samples_df.collect()
}

# ─── Panggil LLM per cluster (di driver) ─────────────────────────────────────
results = []
for row in top_rows:
    meta = {
        "date": row["date"],
        "category": row["category"],
        "kecamatan": row["kecamatan"],
        "complaint_count": row["complaint_count"],
        "complaint_growth_rate_3day": row["complaint_growth_rate_3day"],
        "quadrant": row["quadrant"],
        "avg_importance": row["avg_importance"],
        "avg_urgency": row["avg_urgency"],
    }
    samples = samples_map.get((row["date"], row["kecamatan"], row["category"]), [])
    prompt = build_prompt(meta, samples)
    out = enrich(prompt, meta)

    results.append((
        str(row["date"]),
        row["kecamatan"],
        row["category"],
        int(row["complaint_count"]),
        row["quadrant"],
        float(row["priority_score_base"]),
        str(out.get("complexity", "medium")),
        _safe_int(out.get("estimated_resolution_days"), 0),
        str(out.get("recommended_action", "")),
        _safe_int(out.get("priority_score"), 0),
        str(out.get("summary", "")),
        str(out.get("llm_provider", "unknown")),
    ))
    print(f"    [{out.get('llm_provider')}] {row['kecamatan']}/{row['category']} "
          f"-> prio {out.get('priority_score')}")

# ─── Bangun DataFrame LLM-Gold + ranking ──────────────────────────────────────
schema = StructType([
    StructField("date", StringType()),
    StructField("kecamatan", StringType()),
    StructField("category", StringType()),
    StructField("complaint_count", IntegerType()),
    StructField("quadrant", StringType()),
    StructField("priority_score_base", DoubleType()),
    StructField("complexity", StringType()),
    StructField("estimated_resolution_days", IntegerType()),
    StructField("llm_recommendation", StringType()),
    StructField("llm_priority_score", IntegerType()),
    StructField("llm_summary", StringType()),
    StructField("llm_provider", StringType()),
])
enriched = spark.createDataFrame(results, schema)

# priority_rank: urut berdasarkan priority_score dari LLM (tertinggi = rank 1)
from pyspark.sql.window import Window
w = Window.orderBy(F.col("llm_priority_score").desc(), F.col("priority_score_base").desc())
enriched = (
    enriched
    .withColumn("priority_rank", F.row_number().over(w))
    .withColumn("enriched_at", F.current_timestamp())
)

print("\n>>> Preview LLM-Gold:")
enriched.select(
    "priority_rank", "kecamatan", "category", "quadrant",
    "complexity", "estimated_resolution_days", "llm_priority_score", "llm_summary",
).orderBy("priority_rank").show(20, truncate=60)

print(f"\n>>> Menulis & register tabel {ENRICHED_TABLE}...")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")
(
    enriched.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(ENRICHED_TABLE)
)

print(">>> SELESAI. LLM-Gold siap dibaca Trino/Superset.")
spark.stop()
