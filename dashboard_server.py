import os

# ── Matikan lookup metadata AWS (IMDS) ──────────────────────────────────────
# deltalake/object_store default-nya nyoba ambil region & kredensial dari
# endpoint metadata EC2 (169.254.169.254) -> di laptop ini nggak ada, jadi
# timeout berulang & bikin log penuh WARN. Set region eksplisit + disable IMDS.
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from deltalake import DeltaTable
import pandas as pd

app = FastAPI(title="Surabaya EWS Dashboard")

# Konfigurasi Storage Options untuk Delta Lake di MinIO
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin123",
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_REGION": "us-east-1",
    "AWS_EC2_METADATA_DISABLED": "true",
}

# Koordinat Kecamatan di Surabaya
DISTRICT_COORDS = {
    "Tenggilis Mejoyo": [-7.3188, 112.7661],
    "Mulyorejo": [-7.2667, 112.7833],
    "Gubeng": [-7.2764, 112.7516],
    "Sukolilo": [-7.2941, 112.7981],
    "Wonokromo": [-7.2994, 112.7381],
    "Tegalsari": [-7.2636, 112.7383],
    "Genteng": [-7.2589, 112.7431],
    "Sawahan": [-7.2711, 112.7214],
    "Tandes": [-7.2608, 112.6711],
    "Karang Pilang": [-7.3325, 112.7011],
    "Rungkut": [-7.3197, 112.7844],
    "Kenjeran": [-7.2183, 112.7744],
    "Benowo": [-7.2483, 112.6244],
    "Tambaksari": [-7.2514, 112.7628],
    "Krembangan": [-7.2250, 112.7264],
    "Semampir": [-7.2081, 112.7456],
    "Bubutan": [-7.2458, 112.7333],
    "Asemrowo": [-7.2472, 112.6953],
    "Bulak": [-7.2447, 112.7933],
    "Dukuh Pakis": [-7.2886, 112.6989],
    "Gayungan": [-7.3314, 112.7247],
    "Gunung Anyar": [-7.3353, 112.7956],
    "Jambangan": [-7.3244, 112.7161],
    "Lakarsantri": [-7.3094, 112.6453],
    "Pabean Cantian": [-7.2133, 112.7364],
    "Pakal": [-7.2439, 112.6011],
    "Sambikerep": [-7.2794, 112.6511],
    "Simokerto": [-7.2392, 112.7561],
    "Sukomanunggal": [-7.2653, 112.6956],
    "Wiyung": [-7.3061, 112.6853],
    "Wonocolo": [-7.3175, 112.7356],
    "Tidak Diketahui": [-7.2575, 112.7521]
}

# Map warna prioritas untuk peta (Urbi style)
# Q1: High Urgent, High Important -> Red
# Q2: Low Urgent, High Important -> Orange
# Q3: High Urgent, Low Important -> Yellow
# Q4: Low Urgent, Low Important -> Blue
QUADRANT_COLORS = {
    "Q1": "#ff0055", # Neon Red-Pink
    "Q2": "#ff7700", # Neon Orange
    "Q3": "#ffcc00", # Neon Yellow
    "Q4": "#00aaff"  # Neon Blue
}

# Bikin folder templates dan static
os.makedirs("templates", exist_ok=True)

def get_df_from_delta(s3_path):
    try:
        dt = DeltaTable(s3_path, storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        print(f"Error loading Delta table at {s3_path}: {e}")
        return pd.DataFrame()

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    try:
        with open("templates/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        return f"<h1>Error loading index.html</h1><p>{e}</p>"

@app.get("/api/stats")
async def get_stats():
    """Mengambil metrik statistik global untuk dashboard."""
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    df_enriched = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_enriched")
    
    if df_daily.empty:
        return {
            "total_complaints": 0,
            "q1_q2_count": 0,
            "top_district": "N/A",
            "top_category": "N/A",
            "avg_resolution_days": 0
        }
    
    total_complaints = int(df_daily["complaint_count"].sum())
    q1_q2_df = df_daily[df_daily["quadrant"].isin(["Q1", "Q2"])]
    q1_q2_count = int(q1_q2_df["complaint_count"].sum())
    
    # Cari kecamatan dengan laporan terparah (mengabaikan 'Tidak Diketahui')
    # Prioritas kuadran terburuk: Q1 > Q2 > Q3 > Q4, lalu ambil yang complaint_count terbanyak
    df_valid = df_daily[df_daily["kecamatan"].str.strip().str.lower() != "tidak diketahui"]
    top_district = "N/A"
    
    if not df_valid.empty:
        for q in ["Q1", "Q2", "Q3", "Q4"]:
            df_q = df_valid[df_valid["quadrant"] == q]
            if not df_q.empty:
                # Ambil kecamatan dengan total laporan terbanyak di kuadran prioritas ini
                top_district = df_q.groupby("kecamatan")["complaint_count"].sum().idxmax()
                break
        
        # Fallback jika tidak kecocokan kuadran (walau hampir pasti ada salah satu)
        if top_district == "N/A":
            top_district = df_valid.groupby("kecamatan")["complaint_count"].sum().idxmax()
    
    # Cari kategori terbanyak
    category_sum = df_daily.groupby("category")["complaint_count"].sum()
    top_category = category_sum.idxmax() if not category_sum.empty else "N/A"
    
    # Rata-rata estimasi resolusi dari LLM
    avg_resolution = 0
    if not df_enriched.empty and "estimated_resolution_days" in df_enriched.columns:
        avg_resolution = round(float(df_enriched["estimated_resolution_days"].mean()), 1)

    # ── Metrik tambahan untuk dashboard yang dipercantik ────────────────────
    # Anomaly count (cluster dengan is_anomaly=True)
    anomaly_count = 0
    if "is_anomaly" in df_daily.columns:
        anomaly_count = int(df_daily["is_anomaly"].fillna(False).astype(bool).sum())

    # Rata-rata growth rate 3-hari (indikator tren naik)
    avg_growth_rate = 0.0
    if "complaint_growth_rate_3day" in df_daily.columns:
        avg_growth_rate = round(
            float(df_daily["complaint_growth_rate_3day"].fillna(0).mean()) * 100, 1
        )

    # Importance/Urgency high ratio rata-rata (skala 0-100%)
    avg_importance_ratio = 0.0
    avg_urgency_ratio = 0.0
    if "importance_high_ratio" in df_daily.columns:
        avg_importance_ratio = round(
            float(df_daily["importance_high_ratio"].fillna(0).mean()) * 100, 1
        )
    if "urgency_high_ratio" in df_daily.columns:
        avg_urgency_ratio = round(
            float(df_daily["urgency_high_ratio"].fillna(0).mean()) * 100, 1
        )

    return {
        "total_complaints": total_complaints,
        "q1_q2_count": q1_q2_count,
        "top_district": top_district,
        "top_category": top_category.upper(),
        "avg_resolution_days": avg_resolution,
        # ── tambahan
        "anomaly_count": anomaly_count,
        "avg_growth_rate_pct": avg_growth_rate,
        "avg_importance_ratio_pct": avg_importance_ratio,
        "avg_urgency_ratio_pct": avg_urgency_ratio,
    }

@app.get("/api/districts")
async def get_districts():
    """Agregasi keluhan per kecamatan untuk peta.

    Warna marker = kuadran PALING PARAH yang muncul di kecamatan itu
    (urutan keparahan Q1 > Q2 > Q3 > Q4). Tujuannya supaya kecamatan yang
    punya cluster kritis (Q1) langsung menonjol merah — sesuai semangat
    early-warning. Popup dibuat konsisten: total laporan, jumlah laporan
    prioritas (Q1/Q2), serta rasio importance & urgency tinggi (%).
    """
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    if df_daily.empty:
        return []

    result = []
    for kec, g in df_daily.groupby("kecamatan"):
        kec = str(kec).strip()
        total = int(g["complaint_count"].sum())
        w = g["complaint_count"].clip(lower=1)
        imp_ratio = float((g["importance_high_ratio"].fillna(0) * w).sum() / w.sum())
        urg_ratio = float((g["urgency_high_ratio"].fillna(0) * w).sum() / w.sum())
        # Warna peta = TINGKAT URGENSI kecamatan. Importance di data ini hampir
        # selalu tinggi (semua dianggap penting), jadi yang benar-benar
        # membedakan antar-kecamatan adalah urgency -> dipakai sebagai gradien
        # warna biar peta informatif & gampang dibaca orang awam.
        if urg_ratio >= 0.50:
            tier, color, label = "T1", "#ef4444", "Sangat Mendesak"
        elif urg_ratio >= 0.30:
            tier, color, label = "T2", "#f97316", "Mendesak"
        elif urg_ratio >= 0.15:
            tier, color, label = "T3", "#eab308", "Cukup Mendesak"
        else:
            tier, color, label = "T4", "#3b82f6", "Kurang Mendesak"
        q1q2 = int(g[g["quadrant"].isin(["Q1", "Q2"])]["complaint_count"].sum())
        coords = DISTRICT_COORDS.get(kec, DISTRICT_COORDS["Tidak Diketahui"])
        result.append({
            "kecamatan": kec,
            "coords": coords,
            "complaint_count": total,
            "priority_count": q1q2,
            "importance_high_pct": round(imp_ratio * 100, 1),
            "urgency_high_pct": round(urg_ratio * 100, 1),
            "tier": tier,
            "color": color,
            "priority_level": label,
        })
    return result

@app.get("/api/enriched")
async def get_enriched():
    """Mengambil rekomendasi LLM hasil analisis cluster prioritas Q1/Q2."""
    df_enriched = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_enriched")
    
    if df_enriched.empty:
        return []
        
    # Urutkan berdasarkan rank prioritas LLM
    df_sorted = df_enriched.sort_values(by="priority_rank")
    
    result = []
    for _, row in df_sorted.iterrows():
        result.append({
            "priority_rank": int(row["priority_rank"]),
            "kecamatan": row["kecamatan"],
            "category": row["category"].upper(),
            "complaint_count": int(row["complaint_count"]),
            "quadrant": row["quadrant"],
            "complexity": row["complexity"].capitalize(),
            "estimated_resolution_days": int(row["estimated_resolution_days"]),
            "llm_recommendation": row["llm_recommendation"],
            "llm_priority_score": int(row["llm_priority_score"]),
            "llm_summary": row["llm_summary"],
            "llm_provider": row["llm_provider"]
        })
    return result

@app.get("/api/categories")
async def get_categories():
    """Mendapatkan statistik per kategori keluhan."""
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    if df_daily.empty:
        return []
        
    cat_df = df_daily.groupby("category")["complaint_count"].sum().reset_index()
    return [{"category": row["category"].upper(), "count": int(row["complaint_count"])} for _, row in cat_df.iterrows()]


@app.get("/api/scatter")
async def get_scatter():
    """Data untuk Scatter 4-Kuadran Eisenhower.

    Tiap titik = (avg_urgency, avg_importance) per (kecamatan, category) di-rata-rata
    sepanjang waktu, ukuran titik = total complaint_count, warna = quadrant dominan.
    Ini memenuhi requirement plan: chart wajib "scatter 4-kuadran".
    """
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    if df_daily.empty:
        return []

    grouped = (
        df_daily.groupby(["kecamatan", "category"])
        .agg(
            urg_ratio=("urgency_high_ratio", "mean"),
            imp_ratio=("importance_high_ratio", "mean"),
            complaint_count=("complaint_count", "sum"),
            quadrant=("quadrant", lambda x: x.value_counts().index[0]),
        )
        .reset_index()
    )

    result = []
    for _, row in grouped.iterrows():
        q = row["quadrant"]
        result.append({
            "kecamatan": row["kecamatan"],
            "category": row["category"].upper(),
            # Sumbu pakai RASIO (0-1) yang menentukan kuadran -> posisi titik
            # konsisten dengan warnanya. x = % mendesak, y = % penting.
            "x": round(float(row["urg_ratio"]), 4),
            "y": round(float(row["imp_ratio"]), 4),
            "r": max(6, min(28, 5 + int(row["complaint_count"]) * 1.4)),
            "complaint_count": int(row["complaint_count"]),
            "quadrant": q,
            "color": QUADRANT_COLORS.get(q, "#3b82f6"),
        })
    return result


@app.get("/api/trend")
async def get_trend():
    """Tren keluhan harian per kuadran (untuk line chart time-series)."""
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    if df_daily.empty:
        return {"dates": [], "series": {}}

    # Pastikan kolom date jadi string YYYY-MM-DD agar JSON-safe & sortable
    df_daily = df_daily.copy()
    df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.strftime("%Y-%m-%d")

    pivot = (
        df_daily.groupby(["date", "quadrant"])["complaint_count"]
        .sum()
        .unstack(fill_value=0)
        .sort_index()
    )

    dates = list(pivot.index)
    series = {}
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        if q in pivot.columns:
            series[q] = {
                "data": [int(v) for v in pivot[q].tolist()],
                "color": QUADRANT_COLORS[q],
            }
        else:
            series[q] = {"data": [0] * len(dates), "color": QUADRANT_COLORS[q]}

    return {"dates": dates, "series": series}


@app.get("/api/anomalies")
async def get_anomalies():
    """Cluster anomali (is_anomaly=True) + growth_rate tinggi → indikator early warning."""
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    if df_daily.empty:
        return []

    df = df_daily.copy()
    if "is_anomaly" not in df.columns:
        return []
    df["is_anomaly"] = df["is_anomaly"].fillna(False).astype(bool)
    df["complaint_growth_rate_3day"] = df.get("complaint_growth_rate_3day", 0).fillna(0)

    # Anggap "anomali" kalau flag True ATAU growth_rate >= 50%
    flagged = df[(df["is_anomaly"]) | (df["complaint_growth_rate_3day"] >= 0.5)]
    if flagged.empty:
        return []

    flagged = flagged.copy()
    flagged["date"] = pd.to_datetime(flagged["date"]).dt.strftime("%Y-%m-%d")
    flagged = flagged.sort_values(
        by=["complaint_growth_rate_3day", "complaint_count"], ascending=[False, False]
    )

    result = []
    for _, row in flagged.head(20).iterrows():
        q = row.get("quadrant", "Q4")
        result.append({
            "date": row["date"],
            "kecamatan": row["kecamatan"],
            "category": str(row["category"]).upper(),
            "complaint_count": int(row["complaint_count"]),
            "growth_rate_pct": round(float(row["complaint_growth_rate_3day"]) * 100, 1),
            "is_anomaly": bool(row["is_anomaly"]),
            "quadrant": q,
            "color": QUADRANT_COLORS.get(q, "#00ffcc"),
        })
    return result


if __name__ == "__main__":
    uvicorn.run("dashboard_server:app", host="0.0.0.0", port=8050, reload=True)
