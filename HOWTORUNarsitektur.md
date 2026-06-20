# Surabaya Public Complaint Early Warning System

Sistem deteksi dini & prioritisasi keluhan layanan publik Kota Surabaya berbasis media sosial & pemberitaan. Memantau berita lokal dan media sosial secara otomatis, mengklasifikasi jenis keluhan warga, memetakan lokasinya per kecamatan, dan memberi peringatan dini sebelum masalah viral.

Repositori ini berisi **infrastruktur** sistem (Docker Compose): seluruh layanan lakehouse yang menjadi fondasi pipeline data, mulai dari ingestion sampai serving.

---

## Arsitektur Singkat

```
SUMBER DATA -> Kafka -> Spark (Bronze -> Silver -> Gold) -> Trino -> Superset / Grafana
                          (Delta Lake di MinIO, katalog di Hive Metastore)
                          orkestrasi: Airflow   |   ML tracking: MLflow
```

Penjelasan lengkap tiap komponen dan justifikasi teknisnya ada di `docs/architecture.md`.

---

## Prasyarat

- **Docker Desktop** (atau Docker Engine + Compose v2) terpasang dan berjalan.
- RAM minimal **16 GB** (saat idle pemakaian ringan ~3 GB; beban naik saat job Spark/ML berjalan).
- Koneksi internet untuk unduhan image pertama kali.

Cek instalasi:

```bash
docker --version
docker compose version
```

---

## Setup (sekali di awal)

### 1. Clone & masuk folder

```bash
git clone <URL_REPO_INI>
cd surabaya-complaint-ews
```

### 2. Siapkan file environment

File `.env` berisi kredensial dan tidak ikut di-commit (lihat `.gitignore`). Salin dari contoh:

```bash
# Windows (PowerShell)
copy .env.example .env

# Linux / Mac
cp .env.example .env
```

Lalu isi nilai yang masih kosong di `.env`:

- `AIRFLOW__CORE__FERNET_KEY` — generate setelah Airflow berjalan (lihat langkah 5).
- `SUPERSET_SECRET_KEY` — ganti dengan string acak panjang (minimal 32 karakter).

### 3. Unduh driver JAR untuk Hive

File `.jar` tidak ikut di-commit (besar). Unduh ke folder `hive/lib/`:

```bash
# Driver PostgreSQL (koneksi Hive ke database metadata)
curl -L -o hive/lib/postgresql-42.7.4.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# Konektor S3A (supaya Hive bisa mengakses MinIO) - harus cocok dengan Hadoop 3.3.6 di Hive
curl -L -o hive/lib/hadoop-aws-3.3.6.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
curl -L -o hive/lib/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
```

> Di PowerShell, gunakan `curl.exe` (bukan `curl`).

---

## Menjalankan

Nyalakan semua layanan:

```bash
docker compose up -d
```

Pertama kali akan lama karena mengunduh banyak image. Cek status:

```bash
docker compose ps
```

Tunggu sampai layanan inti berstatus `Up` / `healthy`.

### Inisialisasi sekali jalan (database internal)

Layanan yang punya database sendiri perlu di-setup sekali. Buat database di Postgres bersama:

```bash
docker compose exec hive-postgres psql -U hive -d metastore -c "CREATE DATABASE mlflow;"
docker compose exec hive-postgres psql -U hive -d metastore -c "CREATE DATABASE airflow;"
docker compose exec hive-postgres psql -U hive -d metastore -c "CREATE DATABASE superset;"
```

Init Airflow & Superset (membuat user admin):

```bash
docker compose up airflow-init
docker compose up superset-init
```

Buat 4 topik Kafka:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-rss    --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-x      --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-reddit --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic raw-yt     --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Buat 4 bucket di MinIO (lewat konsol web :9001, tombol Create Bucket): `bronze`, `silver`, `gold`, `mlflow`.

### Generate Fernet key Airflow (langkah 5)

```bash
docker compose exec airflow-webserver python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Salin hasilnya ke `AIRFLOW__CORE__FERNET_KEY` di `.env`, lalu:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

---

## Akses Antarmuka (UI)

| Layanan | URL | Login default |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master | http://localhost:8080 | - |
| Trino | http://localhost:8081 | username bebas, tanpa password |
| MLflow | http://localhost:5000 | - |
| Airflow | http://localhost:8082 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

> Kredensial default di atas hanya untuk development lokal. Jangan dipakai di lingkungan produksi.

---

## Verifikasi Cepat (smoke test)

Memastikan jalur lakehouse berfungsi end-to-end (Spark menulis -> Hive mencatat -> MinIO menyimpan -> Trino membaca):

```bash
# 1. Spark membuat tabel Delta yang terdaftar di Hive
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.jars.ivy=/tmp/.ivy /opt/spark/work-dir/app/test_register_table.py

# 2. Trino membaca tabel yang sama lewat SQL
docker compose exec trino trino
#   lalu di prompt trino>:
#   SHOW SCHEMAS FROM delta;
#   SELECT * FROM delta.gold.complaint_daily;
```

---

## Mematikan

```bash
# Hentikan layanan (data tetap tersimpan di volume)
docker compose down

# Hentikan + hapus semua data (reset total)
docker compose down -v
```

---

## Struktur Folder

```
surabaya-complaint-ews/
├── docker-compose.yml      # Definisi semua layanan
├── .env                    # Kredensial (TIDAK di-commit)
├── .env.example            # Template kredensial
├── hive/
│   ├── metastore-site.xml  # Konfigurasi Hive Metastore
│   └── lib/                # Driver JAR (TIDAK di-commit, unduh manual)
├── kafka/                  # Script setup topik
├── trino/catalog/          # Konfigurasi koneksi Trino (delta.properties)
├── spark/                  # Script Spark + tes
├── airflow/dags/           # 4 DAG (kerangka): scraping, etl, llm, retrain
├── superset/               # Export dashboard
├── grafana/                # Dashboard alerting
├── data/                   # GeoJSON kecamatan, labeled samples
└── docs/                   # architecture.md & dokumentasi
```

---

## Pembagian Peran Tim

| Role | Tanggung Jawab |
|---|---|
| **Infrastructure** | Docker, Kafka, MinIO, Trino, Hive, Airflow, MLflow (repo ini) |
| Ingestion Engineer | Scraper (RSS/X/Reddit/YouTube) -> Kafka -> Bronze |
| ML Engineer | Labeling, Silver NLP, 4 model Spark MLlib, MLflow |
| Pipeline + LLM | Gold agregasi, LLM enrichment, integrasi, Airflow ETL DAG |
| Visualization | Superset dashboard, Grafana alerting, slide, dokumentasi |
