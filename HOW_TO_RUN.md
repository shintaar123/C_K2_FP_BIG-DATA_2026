# 🚀 Cara Menjalankan — Surabaya EWS (Early Warning System Pengaduan Warga)

Pipeline Big Data: **Ingestion (RSS + sosial) → Kafka → Spark (Bronze/Silver/Gold di Delta Lake + MinIO) → ML (kategori, importance, urgency, anomaly) → LLM enrichment → Dashboard.**

Panduan ini untuk **Windows + PowerShell** (sesuai skrip di repo). Jalankan semua perintah dari folder root project.

---

## 🧩 Prasyarat
- **Docker Desktop** (sudah running)
- **Python 3.9+** (untuk dashboard custom)
- **Git**
- RAM kosong ± 8 GB & koneksi internet (download image + paket Spark saat pertama kali)

---

## 🅰️ PERTAMA KALI (habis `git clone`)

### 1) Clone & masuk folder
```powershell
git clone <URL-REPO-KAMU>
cd "FP BIG D"
```

### 2) Buat file `.env`
File `.env` tidak ikut ter-clone (rahasia). Salin dari contoh:
```powershell
copy .env.example .env
```
> Opsional: isi `NVIDIA_API_KEY` di `.env` untuk rekomendasi LLM. **Tanpa key pun tetap jalan** (otomatis pakai rule-based fallback).

### 3) Download 3 JAR untuk Hive Metastore (WAJIB)
Jar ini di-`gitignore` (ada yang 296 MB) jadi tidak ikut ter-clone. Tanpa ini, `hive-metastore` gagal start.
```powershell
New-Item -ItemType Directory -Force -Path hive/lib | Out-Null
$jars = @{
  "postgresql-42.7.4.jar"            = "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
  "hadoop-aws-3.3.6.jar"             = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"
  "aws-java-sdk-bundle-1.12.367.jar" = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar"
}
foreach ($n in $jars.Keys) { if (-not (Test-Path "hive/lib/$n")) { Write-Host "Download $n..."; Invoke-WebRequest -Uri $jars[$n] -OutFile "hive/lib/$n" } }
```

### 4) Nyalakan semua container
```powershell
docker compose up -d
docker compose ps
```
Tunggu semua status `healthy`/`Up` (pertama kali bisa 3–5 menit karena download image).

### 5) Install dependency di Spark (sekali saja)
```powershell
docker compose exec spark-master bash -c "pip install -q feedparser==6.0.11 kafka-python-ng==2.2.3 requests==2.32.3 beautifulsoup4==4.12.3 python-dotenv==1.0.1 mlflow==2.17.2 boto3 urllib3==1.26.20"
```

### 6) Scraping data → Kafka
```powershell
docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_rss_to_kafka.py
docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_social_to_kafka.py
```
Sukses jika muncul `Berhasil kirim XX/XX record ke topic 'raw-rss'`.

### 7) Jalankan pipeline penuh + latih model
```powershell
.\run_pipeline.ps1 -Ingest -Train
```
⏳ Paling lama di sini (5–15 menit; pertama kali Spark download paket). Tunggu sampai muncul **`PIPELINE SELESAI!`**.

### 8) Bangun dashboard Superset
```powershell
docker start -a superset-bootstrap
```
Sukses jika muncul `Selesai. Buka Superset di http://localhost:8088`.

### 9) Jalankan dashboard custom (terminal baru)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dashboard.txt
python dashboard_server.py
```
Buka **http://localhost:8050**.

✅ Selesai. Buka semua dashboard (lihat tabel di bawah).

---

## 🔁 MENJALANKAN ULANG (habis tutup laptop / restart)

Data tersimpan di volume Docker, jadi **tidak perlu mengulang pipeline**. Cukup:

```powershell
# 1. Hidupkan lagi container yang sudah ada
docker compose start
docker compose ps                      # tunggu healthy

# 2. Jalankan dashboard custom (terminal baru)
.\.venv\Scripts\Activate.ps1
python dashboard_server.py
```
Lalu buka dashboard. **Tidak perlu** install dependency atau training ulang.

> Jika dashboard Superset kelihatan kosong setelah restart, jalankan sekali: `docker start -a superset-bootstrap`.

### ⚠️ PENTING
- Untuk mematikan: pakai **`docker compose stop`** (data & container aman). Lalu `docker compose start` untuk menghidupkan lagi.
- **JANGAN `docker compose down -v`** → itu MENGHAPUS semua data (MinIO, Postgres, Kafka) dan kamu harus ulang dari Fase A.
- `docker compose down` (tanpa `-v`) menghapus container (data volume tetap aman), tapi kamu perlu **ulang Langkah 5** (install dependency) karena container Spark dibuat ulang.

---

## 🔄 (Opsional) Mode autonomous — loop terus-menerus
Setelah pipeline pertama sukses, untuk update data otomatis berkala:
```powershell
.\run_continuous.ps1 -IntervalSeconds 1800     # tiap 30 menit; Ctrl+C untuk stop
```

---

## 🌐 Alamat & Login
| Layanan | URL | Login |
|---|---|---|
| **Dashboard custom** (utama) | http://localhost:8050 | — |
| **Superset** | http://localhost:8088 | `admin` / `admin` |
| **Grafana** | http://localhost:3000 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Spark UI | http://localhost:8080 | — |
| Trino UI | http://localhost:8081 | — |
| MLflow | http://localhost:5001 | — |

- Superset → menu **Dashboards** → "Surabaya EWS — Analitik Keluhan"
- Grafana → menu **Dashboards** → "Surabaya EWS Monitoring"

---

## 🛟 Troubleshooting singkat
| Gejala | Solusi |
|---|---|
| `hive-metastore` restart terus | Jar di `hive/lib/` belum lengkap → ulangi **Langkah 3** lalu `docker compose up -d` |
| `ModelInputExample ... ImportError` saat training | Dependency belum lengkap → ulangi **Langkah 5** (pakai `mlflow==2.17.2`, bukan skinny) |
| `NoBrokersAvailable` saat ingestion | Container `kafka` belum `healthy`, tunggu lalu ulangi |
| Dashboard custom error konek MinIO | Pastikan container `minio` jalan (port 9000) |
| Dashboard Superset kosong | `docker start -a superset-bootstrap` |
| Chart Grafana kosong | Pastikan pipeline sudah jalan (tabel `gold` terisi), lalu refresh dashboard |
| Browser nampilin versi lama | Hard refresh `Ctrl+F5` |

---

## 📝 Catatan
- Pertama kali submit Spark akan mengunduh paket (delta, hadoop-aws, kafka) ke folder `.ivy/` — wajar lambat; run berikutnya cepat (ter-cache).
- Ingestion sosial (YouTube/Threads) otomatis di-skip kalau tanpa API key/Playwright — itu normal, bukan error. Data RSS sudah cukup untuk demo.
