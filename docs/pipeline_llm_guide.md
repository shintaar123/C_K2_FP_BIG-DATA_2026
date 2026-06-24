# Pipeline + LLM — Panduan Menjalankan

Bagian **Pipeline + LLM Engineer**: Gold aggregation, LLM enrichment, integrasi
end-to-end, dan Airflow ETL DAG. Dokumen ini menjelaskan alur dan cara menjalankan.

## Alur Data End-to-End

```
Bronze (news_raw)
  └─ silver_transform.py   →  s3a://silver/news_silver   (clean_text, score; label NULL)
       └─ ml/predict_batch.py  →  s3a://silver/news_scored   (category/importance/urgency terisi)
            └─ gold_aggregate.py   →  Hive: gold.complaint_daily   (agregasi + 4-kuadran + anomaly)
                 └─ llm/llm_enrichment.py  →  Hive: gold.complaint_enriched   (LLM-Gold)
                      └─ Trino → Superset / Grafana
```

> **Penting:** `predict_batch.py` adalah jembatan yang sebelumnya hilang.
> `silver_transform.py` menulis `category`/`importance_label`/`urgency_label`
> sebagai NULL — tanpa langkah predict, Gold tidak punya data untuk diagregasi.

## Prasyarat

1. Semua service Docker `Up` (lihat `HOWTORUNarsitektur.md`).
2. Bucket `gold` sudah dibuat di MinIO (selain bronze/silver/mlflow).
3. Model sudah dilatih (`train_classifier.py`, `train_importance.py`, `train_urgency.py`)
   dan tersimpan di `s3a://mlflow/models/*`.
4. Folder `./data` dan `./llm` sudah ter-mount ke spark-master/worker
   (sudah diatur di `docker-compose.yml`). Kalau service lama masih jalan:
   `docker compose up -d --force-recreate spark-master spark-worker`.

## Menjalankan Manual (urut)

```bash
PKG="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"

# 1. Bronze -> Silver
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages $PKG --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/silver_transform.py

# 2. Silver -> Silver ter-skor (apply 3 model)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages $PKG --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/ml/predict_batch.py

# 3. Silver ter-skor -> Gold (agregasi + 4-kuadran)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages $PKG --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/app/gold_aggregate.py

# 4. Gold Q1+Q2 -> LLM-Gold (isi API key salah satu provider; tanpa key tetap jalan rule-based)
docker compose exec -e GEMINI_API_KEY=ISI_KEY spark-master /opt/spark/bin/spark-submit \
  --packages $PKG --conf spark.jars.ivy=/tmp/.ivy \
  /opt/spark/work-dir/llm/llm_enrichment.py
```

## Menjalankan via Airflow

DAG yang sudah di-wire (lewat `spark_submit_helper` → exec ke spark-master):

| DAG | Jadwal | Tugas |
|---|---|---|
| `etl_pipeline_dag` | 08.00 harian | silver_transform → predict_batch → gold_aggregate |
| `llm_enrichment_dag` | 10.00 harian | llm_enrichment (Gold Q1/Q2 → LLM-Gold) |
| `ml_retrain_dag` | Senin 02.00 | retrain 3 classifier + log MLflow |

API key LLM untuk DAG di-set sebagai **Airflow Variable** (`GEMINI_API_KEY`,
`GROQ_API_KEY`, `CEREBRAS_API_KEY`) lewat Airflow UI → Admin → Variables, atau:

```bash
docker compose exec airflow-scheduler airflow variables set GEMINI_API_KEY "ISI_KEY"
```

> Airflow menjalankan Spark dengan `docker exec` ke container `spark-master`
> (socket `/var/run/docker.sock` di-mount + paket `docker` di-install otomatis).

## Verifikasi via Trino

```sql
SHOW TABLES FROM delta.gold;
SELECT * FROM delta.gold.complaint_daily ORDER BY priority_score_base DESC LIMIT 20;
SELECT * FROM delta.gold.complaint_enriched ORDER BY priority_rank LIMIT 20;
```

## Skema Tabel Gold

**gold.complaint_daily** — agregasi harian per kecamatan & kategori:
`date, kecamatan, category, complaint_count, avg_importance, avg_urgency,
importance_high_ratio, urgency_high_ratio, quadrant, complaint_growth_rate_3day,
is_anomaly, priority_score_base`

**gold.complaint_enriched** (LLM-Gold) — hasil LLM untuk Q1/Q2:
`date, kecamatan, category, complaint_count, quadrant, priority_score_base,
complexity, estimated_resolution_days, llm_recommendation, llm_priority_score,
llm_summary, llm_provider, priority_rank, enriched_at`

## Catatan Desain

- **LLM fallback chain:** Gemini → Groq → Cerebras → rule-based. Pipeline tidak
  pernah crash karena LLM; tanpa API key sekalipun, rule-based mengisi rekomendasi.
- **`is_anomaly`** di Gold saat ini berbasis z-score (>2σ) terhadap histori per
  grup. Isolation Forest (tugas ML Engineer, `train_anomaly.py`) bisa menggantikan
  flag ini setelah ada cukup histori — Gold layer sudah menyediakan time-series-nya.
- **Token hemat:** LLM hanya menerima agregasi Gold + maks 3 contoh teks per cluster,
  bukan raw data individual (sesuai catatan privasi Section 6).
