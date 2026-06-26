# Arsitektur Infrastruktur — Surabaya Complaint Early Warning System

Dokumen ini menjelaskan rancangan infrastruktur Big Data sistem, justifikasi pemilihan tiap teknologi, dan alur data end-to-end. Disusun sebagai pendukung komponen penilaian **K2 — Desain Infrastruktur Big Data**.

---

## 1. Gambaran Umum

Sistem dibangun di atas arsitektur **lakehouse** dengan pola **Medallion (Bronze -> Silver -> Gold)**. Seluruh komponen dijalankan sebagai container melalui satu `docker-compose.yml`, sehingga seluruh pipeline dapat dijalankan secara reproducible di mesin mana pun yang memiliki Docker.

Pipeline mencakup keempat lapisan arsitektur Big Data yang lengkap:

| Lapisan | Teknologi | Peran |
|---|---|---|
| **Ingestion** | Apache Kafka | Menampung & menyalurkan data masuk dari scraper |
| **Storage** | MinIO + Delta Lake | Menyimpan seluruh layer data (object storage S3-compatible) |
| **Processing** | Apache Spark | Transformasi Bronze -> Silver -> Gold + ML |
| **Serving** | Trino + Superset + Grafana + FastAPI custom | Query SQL & visualisasi untuk pengguna akhir (lihat Section 7) |
| **Catalog** | Hive Metastore | Katalog metadata seluruh tabel |
| **Orchestration** | Script loop (`run_continuous`) | Penjadwalan & loop pipeline |
| **ML Tracking** | MLflow | Pelacakan eksperimen & versi model |

---

## 2. Alur Data End-to-End

```
[SUMBER DATA]
  RSS Berita - X (Twitter) - Reddit - YouTube
        |
        v  (Python scraper)
[INGESTION]  Apache Kafka  (topik: raw-rss, raw-x, raw-reddit, raw-yt)
        |
        v  (Spark Structured Streaming)
[BRONZE]  Data mentah, format Delta Lake di MinIO  (s3a://bronze/)
        |
        v  (Spark Batch: cleaning, klasifikasi kategori, NER lokasi, scoring)
[SILVER]  Data bersih + terklasifikasi  (s3a://silver/)
        |
        v  (Spark Batch: agregasi per kecamatan/kategori/hari + label 4-kuadran)
[GOLD]  Data agregat siap analitik  (s3a://gold/)
        |
        +--> (LLM enrichment) --> [LLM-GOLD]  rekomendasi tindakan
        |
        v  (Trino sebagai query engine)
[SERVING]  Superset (dashboard 4-kuadran, heatmap)  +  Grafana (alerting)

Katalog metadata seluruh tabel: Hive Metastore
Pelacakan model ML: MLflow
Orkestrasi pipeline: script run_continuous (loop) / run_pipeline
```

Jalur ini telah diverifikasi end-to-end melalui smoke test: Spark menulis tabel Delta -> Hive mencatat metadata -> MinIO menyimpan file -> Trino membaca kembali lewat SQL standar.

---

## 3. Justifikasi Pemilihan Teknologi

Setiap teknologi dipilih dengan alasan teknis eksplisit, bukan sekadar mengikuti tren.

### Apache Kafka (Ingestion)
Dipilih karena **throughput tinggi** dan kemampuan **decoupling** antara produsen dan konsumen data. Scraper dari banyak sumber dapat menulis ke Kafka secara paralel tanpa saling memblokir, dan Spark dapat mengonsumsinya dengan laju sendiri. Saat terjadi lonjakan keluhan (misal isu viral), Kafka berfungsi sebagai buffer sehingga data tidak hilang. Menggunakan mode **KRaft** (tanpa Zookeeper) untuk arsitektur yang lebih sederhana.

### MinIO (Storage)
Dipilih sebagai **object storage S3-compatible** yang dapat berjalan lokal tanpa biaya cloud. Kompatibilitas S3 berarti seluruh ekosistem (Spark, Trino, Hive, MLflow) dapat mengaksesnya menggunakan protokol S3A standar, sehingga sistem mudah dipindahkan ke cloud nyata (AWS S3) tanpa mengubah kode.

### Delta Lake (Format Storage)
Dipilih sebagai format tabel lakehouse karena memberikan **transaksi ACID, versioning (time travel), dan schema enforcement** di atas object storage murah. Folder `_delta_log` mencatat riwayat transaksi, memberikan keandalan database tradisional pada data lake. Berjalan sebagai library di dalam Spark (bukan container terpisah).

### Apache Spark (Processing)
Dipilih karena **in-memory distributed processing** yang jauh lebih cepat dibanding pemrosesan baris-per-baris, serta memiliki **MLlib** bawaan untuk model klasifikasi (kategori, importance, urgency, anomaly). Satu engine menangani baik transformasi batch maupun streaming, mengurangi kompleksitas stack.

### Hive Metastore (Catalog)
Dipilih sebagai **katalog metadata terpusat**. Menyimpan definisi skema seluruh tabel Delta sehingga Spark dan Trino berbagi pemahaman struktur tabel yang konsisten. Tanpa katalog ini, Trino tidak akan tahu cara membaca tabel yang ditulis Spark. Menggunakan PostgreSQL sebagai backend penyimpanan metadata.

### Trino (Query Engine)
Dipilih sebagai **query engine SQL terdistribusi** yang memungkinkan dashboard membaca layer Gold menggunakan SQL standar, tanpa harus menjalankan Spark setiap kali. Trino dioptimasi untuk query analitik interaktif (latensi rendah), cocok untuk kebutuhan dashboard yang responsif.

### Apache Superset & Grafana (Serving)
**Superset** dipilih untuk dashboard analitik eksploratif (scatter 4-kuadran, heatmap peta kecamatan). **Grafana** dipilih untuk monitoring time-series dan **alerting visual** (panel berubah warna sesuai threshold) saat muncul keluhan prioritas tinggi (Q1). Keduanya open-source dan terhubung ke Trino sebagai sumber data. Detail lengkap & cara provisioning ada di Section 7.

### Orkestrasi (Script Loop)
Orkestrasi pipeline dilakukan lewat script `run_continuous` (PowerShell/Python) yang menjalankan seluruh tahap secara berurutan (Ingestion → Bronze → Silver → Predict → Gold → LLM enrichment) lalu mengulanginya dengan jeda yang dapat dikonfigurasi. Pendekatan ini ringan, tanpa service orkestrator tambahan, dan cukup untuk skala proyek ini. Dependency antar-tahap dijaga oleh urutan eksekusi di dalam script.

### MLflow (ML Tracking)
Dipilih untuk **melacak eksperimen model**: menyimpan metrik (F1, AUC, Precision@K), parameter, dan artefak model setiap kali pelatihan dijalankan. Memungkinkan perbandingan antar-versi model dari waktu ke waktu, bukan sekadar angka sekali jalan. Backend metadata di PostgreSQL, artefak model di MinIO.

---

## 4. Pertimbangan Keandalan & Skalabilitas

- **Reproducibility:** seluruh stack didefinisikan dalam satu `docker-compose.yml` dengan versi image yang dikunci (pinned), sehingga identik di setiap mesin tim.
- **Pemisahan storage & compute:** data tersimpan di MinIO (storage) terpisah dari Spark/Trino (compute), sehingga compute dapat di-scale tanpa memindahkan data — prinsip inti arsitektur lakehouse.
- **Decoupling via Kafka:** kegagalan satu scraper tidak menjatuhkan pipeline lain; data tertahan di Kafka sampai dapat diproses.
- **Graceful degradation:** jika satu sumber data gagal, pipeline tetap berjalan dengan sumber yang tersisa.
- **Manajemen dependency:** image resmi (Apache, MinIO) dipilih dibanding image pihak ketiga untuk menghindari risiko image hilang/dibekukan, demi keberlanjutan jangka panjang.

---

## 5. Serving Layer — Hybrid (Superset + Grafana + FastAPI Custom)

Sesuai rencana implementasi & untuk memaksimalkan nilai K2 (Desain Infrastruktur)
sekaligus inovasi, **serving layer dipecah ke tiga tools yang saling melengkapi**:

| Tool | Peran | Audience | Port |
|---|---|---|---|
| **Apache Superset** | Dashboard analitik eksploratif (scatter 4-kuadran Eisenhower, heatmap kecamatan x kategori, distribusi kategori, tren waktu, tabel LLM-enriched). | Analis / dosen — eksplorasi mendalam | 8088 |
| **Grafana** | Monitoring real-time dengan **visual alert** (panel berubah merah saat threshold dilewati, mis. Q1 ≥ 25). Cocok untuk control-center / operator. | Operator pemerintah daerah | 3000 |
| **FastAPI Custom Dashboard** | Tampilan storytelling interaktif: peta Surabaya (Leaflet) + scatter 4-kuadran Chart.js + tren harian + indikator anomali + sidebar rekomendasi LLM. Value-add inovasi tim. | Stakeholder / presentasi | 8050 |

### 5.1 Provisioning Otomatis (Reproducible, bukan klik manual)

Karena seluruh stack berjalan via `docker compose`, **tidak ada klik UI** untuk
setup dashboard. Semua aset disimpan sebagai file di repo dan dimount ke
container.

**Grafana** — file-based provisioning (native Grafana feature):
```
grafana/provisioning/
├── datasources/trino.yaml          # Datasource Trino otomatis ke-load
└── dashboards/
    ├── dashboard-provider.yaml      # Provider config (load semua *.json)
    └── surabaya-ews-monitoring.json # Dashboard utama (9 panel)
```
Saat container Grafana start, plugin `trino-datasource` di-install otomatis,
datasource dibaca dari YAML, dashboard di-import dari JSON. Visual alert
diimplementasikan via threshold warna (green/yellow/red) per panel — tanpa
notifikasi eksternal (email/Telegram), sesuai keputusan tim.

**Superset** — REST API bootstrap (lebih robust daripada YAML asset bundle):
```
superset/
├── bootstrap.py       # Python script idempoten via Superset REST API
└── README.md          # Cara rerun manual + troubleshooting
```
Service baru di `docker-compose.yml`: `superset-bootstrap` yang menunggu
Superset siap (poll `/health`), login admin, lalu membuat: database Trino
(`trino://admin@trino:8080/delta`) → 2 dataset (`gold.complaint_daily`,
`gold.complaint_enriched`) → 9 chart (termasuk Scatter 4-Kuadran wajib) →
1 dashboard. Script idempoten — aman di-rerun, skip resource yang sudah ada.

**FastAPI Custom** — kode langsung di repo:
```
dashboard_server.py            # FastAPI (port 8050) baca Delta via deltalake
templates/index.html           # UI dark-mode (Leaflet + Chart.js)
requirements-dashboard.txt     # Versi pinned, pyarrow eksplisit
```

### 5.2 Cara Menjalankan (Urutan)

```bash
# 1) Start seluruh stack
docker compose up -d

# 2) Setelah Spark master/worker siap, jalankan pipeline (lihat README utama)
#    Output: tabel gold.complaint_daily & gold.complaint_enriched (Delta di MinIO)

# 3) Cek hasil:
#    Trino    -> http://localhost:8081  (query: SELECT * FROM delta.gold.complaint_daily)
#    Superset -> http://localhost:8088  (login admin/admin -> dashboard "Surabaya EWS — Analitik Keluhan")
#    Grafana  -> http://localhost:3000  (login admin/admin -> folder "Surabaya EWS")

# 4) FastAPI custom dashboard (jalan terpisah di host):
pip install -r requirements-dashboard.txt
python dashboard_server.py
# Buka http://localhost:8050
```

### 5.3 Konsistensi Data antar Serving Tools

Ketiga dashboard membaca **sumber data yang sama** (tabel Delta di MinIO):
- Superset & Grafana via **Trino** (SQL terdistribusi) — `delta.gold.*`
- FastAPI custom via library **deltalake** (Python, langsung baca S3) —
  `s3://gold/warehouse/gold.db/*`

Implikasinya: kalau pipeline Spark re-run dan tabel ter-update, ketiga
dashboard akan menunjukkan angka konsisten setelah refresh (Grafana &
FastAPI auto-refresh 30 detik; Superset manual atau dijadwalkan per chart).

> **Catatan teknis penting:** Penulisan tabel Gold di `spark/gold_aggregate.py`
> dan `llm/llm_enrichment.py` sudah dibuat **idempoten** (DROP TABLE + clean
> warehouse path sebelum saveAsTable) untuk menghindari bug Hive
> `"Could not alter schema in a Hive compatible way"` yang menyebabkan Trino
> gagal membaca tabel saat schema berubah antar run.

---



## 6. Pemetaan ke Materi Kuliah

| Komponen | Teknologi | Materi |
|---|---|---|
| Message Broker | Apache Kafka | Minggu 7 |
| Object Storage | MinIO | Minggu 4/9 |
| Processing Engine | Apache Spark | Minggu 5 |
| Storage Format | Delta Lake (Medallion) | Minggu 10 |
| ML Library | Spark MLlib | Minggu 6/13 |
| ML Tracking | MLflow | Minggu 13 |
| Orchestration | Script loop (`run_continuous`) | Minggu 12 |
| Data Catalog | Hive Metastore | Minggu 11 |
| Query Engine | Trino | Minggu 14 |
| Dashboard | Superset + Grafana (+ FastAPI custom value-add) | Minggu 14 |

Seluruh materi minggu 4-14 termanfaatkan dalam infrastruktur ini.

---

## 7. Versi Komponen (Pinned)

| Komponen | Versi |
|---|---|
| Apache Kafka | apache/kafka:3.9.0 |
| MinIO | minio/minio:latest |
| Apache Spark | apache/spark:3.5.3 |
| Delta Lake | delta-spark 3.2.0 |
| Hadoop AWS (S3A) | hadoop-aws 3.3.4 (Spark) / 3.3.6 (Hive) |
| Hive Metastore | apache/hive:4.0.0 |
| PostgreSQL | postgres:15 |
| Trino | trinodb/trino:455 |
| MLflow | ghcr.io/mlflow/mlflow:v2.17.2 |
| Apache Superset | apache/superset:4.1.1 |
| Grafana | grafana/grafana:11.3.0 |
| Grafana plugin (Trino) | trino-datasource (terbaru, auto-install) |
| FastAPI (dashboard custom) | fastapi 0.115.6, uvicorn 0.32.1 |
| Delta Lake Python (dashboard) | deltalake 0.22.3 + pyarrow 18.1.0 |
