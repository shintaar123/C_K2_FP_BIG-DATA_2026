# Superset Bootstrap — Surabaya EWS

Folder ini berisi aset reproducible untuk Apache Superset agar dashboard analitik
proyek Surabaya Complaint Early Warning System dapat dibuat **otomatis**
saat `docker compose up`, tanpa klik manual di UI.

## Cara Kerja Otomatis

Saat `docker compose up`, urutan service jalan:

1. `superset-init` — buat DB metadata + akun admin/admin
2. `superset` — start web server di port 8088
3. **`superset-bootstrap`** — service baru yang:
   - Tunggu `/health` Superset OK
   - Login sebagai admin
   - Buat koneksi database **Trino Delta** (`trino://admin@trino:8080/delta`)
   - Buat dataset `gold.complaint_daily` & `gold.complaint_enriched`
   - Buat 9 chart (termasuk **Scatter 4-Kuadran Eisenhower** wajib)
   - Buat dashboard **"Surabaya EWS — Analitik Keluhan"**

Script `bootstrap.py` **idempoten**: kalau resource sudah ada, skip — aman
di-rerun berkali-kali.

## Cara Menjalankan Ulang Manual

Kalau ingin re-run hanya bootstrap (misalnya setelah hapus dashboard):

```bash
# Dari host (di repo root)
docker compose run --rm superset-bootstrap

# Atau langsung jalankan script di dalam container Python apa pun
docker compose exec superset python /bootstrap/bootstrap.py
```

## Chart yang Dibuat

| # | Nama                              | Tipe                     | Dataset             |
|---|-----------------------------------|--------------------------|---------------------|
| 1 | Scatter 4-Kuadran Eisenhower      | bubble_v2 (WAJIB)        | complaint_daily     |
| 2 | Distribusi Kuadran                | pie (donut)              | complaint_daily     |
| 3 | Top Kecamatan (Total Keluhan)     | dist_bar                 | complaint_daily     |
| 4 | Sebaran Kategori                  | dist_bar                 | complaint_daily     |
| 5 | Tren Harian per Kuadran           | echarts_timeseries_line  | complaint_daily     |
| 6 | Total Keluhan                     | big_number_total         | complaint_daily     |
| 7 | Keluhan Kritis Q1                 | big_number_total         | complaint_daily     |
| 8 | Heatmap Kecamatan x Kategori      | heatmap                  | complaint_daily     |
| 9 | Top Prioritas LLM-Enriched        | table                    | complaint_enriched  |

## Troubleshooting

**Bootstrap gagal di chart tertentu?**
Script sudah dirancang fault-tolerant: kalau satu chart gagal, lanjut ke chart
berikutnya. Cek log dengan:
```bash
docker compose logs superset-bootstrap
```

**Trino belum punya tabel `gold.complaint_daily`?**
Jalankan pipeline Spark dulu — lihat `docs/architecture.md` & `run_continuous.sh`.

**Mau hapus semua aset dan re-bootstrap?**
Login Superset (http://localhost:8088, admin/admin), hapus dashboard +
chart + dataset + database lewat UI, lalu:
```bash
docker compose restart superset-bootstrap
```
