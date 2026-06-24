"""
prompt_templates.py  (Pipeline + LLM Engineer)
Template prompt untuk LLM enrichment Gold layer.

Input ke LLM = agregasi Gold (bukan raw text individual warga) supaya:
- hemat token
- tidak mengirim data personal sensitif ke free tier (lihat catatan Section 6)
"""

SYSTEM_INSTRUCTION = (
    "Kamu adalah sistem analitik keluhan layanan publik Kota Surabaya. "
    "Jawab HANYA dengan satu objek JSON valid, tanpa teks lain, tanpa code fence."
)

# Bobot kuadran untuk membantu LLM memahami prioritas
QUADRANT_DESC = {
    "Q1": "PENTING & MENDESAK (prioritas utama, tangani segera)",
    "Q2": "PENTING tapi TIDAK MENDESAK (jadwalkan)",
    "Q3": "TIDAK PENTING tapi MENDESAK (delegasikan)",
    "Q4": "TIDAK PENTING & TIDAK MENDESAK (monitor saja)",
}


def build_prompt(meta, sample_texts):
    """Bangun prompt enrichment dari satu baris Gold.

    meta: dict berisi category, kecamatan, complaint_count,
          complaint_growth_rate_3day, quadrant, avg_importance, avg_urgency.
    sample_texts: list contoh keluhan (sudah dibersihkan, max 3).
    """
    growth_pct = round((meta.get("complaint_growth_rate_3day") or 0.0) * 100, 1)
    quadrant = meta.get("quadrant", "Q2")
    samples = sample_texts[:3] if sample_texts else ["(tidak ada contoh teks)"]
    samples_block = "\n".join(f"  - {s}" for s in samples)

    return f"""{SYSTEM_INSTRUCTION}

Data masalah hari ini:
- Kategori        : {meta.get('category')}
- Kecamatan       : {meta.get('kecamatan')}
- Jumlah keluhan  : {meta.get('complaint_count')} dalam 24 jam (perubahan {growth_pct}% vs rata-rata 3 hari)
- Kuadran         : {quadrant} - {QUADRANT_DESC.get(quadrant, '')}
- Avg importance  : {meta.get('avg_importance')}
- Avg urgency     : {meta.get('avg_urgency')}
- Contoh keluhan  :
{samples_block}

Berikan analisis dalam format JSON dengan field berikut (TANPA penjelasan tambahan):
{{
  "complexity": "low | medium | high",
  "estimated_resolution_days": <angka integer>,
  "recommended_action": "<tindakan konkret 1-2 kalimat untuk pemkot>",
  "priority_score": <angka 1-10>,
  "summary": "<ringkasan 1 kalimat>"
}}"""
