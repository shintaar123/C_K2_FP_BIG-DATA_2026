import os
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
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
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
        
    return {
        "total_complaints": total_complaints,
        "q1_q2_count": q1_q2_count,
        "top_district": top_district,
        "top_category": top_category.upper(),
        "avg_resolution_days": avg_resolution
    }

@app.get("/api/districts")
async def get_districts():
    """Mengambil agregasi keluhan per kecamatan beserta koordinat peta."""
    df_daily = get_df_from_delta("s3://gold/warehouse/gold.db/complaint_daily")
    
    if df_daily.empty:
        return []
        
    # Group by kecamatan dan ambil metrik relevan
    grouped = df_daily.groupby("kecamatan").agg({
        "complaint_count": "sum",
        "avg_importance": "mean",
        "avg_urgency": "mean",
        "quadrant": lambda x: x.value_counts().index[0] # kuadran dominan
    }).reset_index()
    
    result = []
    for _, row in grouped.iterrows():
        kec = row["kecamatan"].strip()
        coords = DISTRICT_COORDS.get(kec, DISTRICT_COORDS["Tidak Diketahui"])
        color = QUADRANT_COLORS.get(row["quadrant"], "#00ffcc")
        
        # Peta prioritas: prioritas lebih tinggi jika Q1 (Urgent & Important)
        priority_level = "Rendah"
        if row["quadrant"] == "Q1":
            priority_level = "Kritis (Q1)"
        elif row["quadrant"] == "Q2":
            priority_level = "Tinggi (Q2)"
        elif row["quadrant"] == "Q3":
            priority_level = "Sedang (Q3)"
            
        result.append({
            "kecamatan": kec,
            "coords": coords,
            "complaint_count": int(row["complaint_count"]),
            "avg_importance": round(float(row["avg_importance"]), 2),
            "avg_urgency": round(float(row["avg_urgency"]), 2),
            "quadrant": row["quadrant"],
            "color": color,
            "priority_level": priority_level
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

if __name__ == "__main__":
    uvicorn.run("dashboard_server:app", host="0.0.0.0", port=8050, reload=True)
