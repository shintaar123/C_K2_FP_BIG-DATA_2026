"""
llm_client.py  (Pipeline + LLM Engineer)
Wrapper LLM dengan fallback chain GRATIS:
Gemini -> NVIDIA NIM -> Groq -> Cerebras -> rule-based.

Desain (Section 6 implementation plan):
- Provider utama  : Google Gemini 2.5 Flash  (1.500 req/hari gratis)
- Fallback 1      : NVIDIA NIM (build.nvidia.com) Llama 3.3 70B  (OpenAI-compatible)
- Fallback 2      : Groq  Llama 3.3 70B       (OpenAI-compatible)
- Fallback 3      : Cerebras Llama 3.3 70B
- Fallback akhir  : rekomendasi rule-based (tanpa LLM) -> pipeline TIDAK pernah crash

Hanya pakai stdlib (urllib, json) supaya bisa jalan di dalam container Spark
tanpa install dependency tambahan. API key dibaca dari environment variable.
Cukup isi SALAH SATU API key; provider yang key-nya kosong otomatis dilewati.
"""

import json
import os
import urllib.request
import urllib.error

GEMINI_API_KEY   = os.environ.get("GEMINI_API_KEY", "")
NVIDIA_API_KEY   = os.environ.get("NVIDIA_API_KEY", "")
GROQ_API_KEY     = os.environ.get("GROQ_API_KEY", "")
CEREBRAS_API_KEY = os.environ.get("CEREBRAS_API_KEY", "")

GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
NVIDIA_MODEL   = os.environ.get("NVIDIA_MODEL", "z-ai/glm-5.1")
GROQ_MODEL     = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
CEREBRAS_MODEL = os.environ.get("CEREBRAS_MODEL", "llama-3.3-70b")

HTTP_TIMEOUT = 120  # model reasoning (mis. GLM-5.1) bisa lebih lama merespons


# ─── HTTP helper ─────────────────────────────────────────────────────────────

def _post_json(url, payload, headers):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ─── Provider calls (mengembalikan raw text dari model) ──────────────────────

def _call_gemini(prompt):
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    resp = _post_json(url, payload, {"Content-Type": "application/json"})
    return resp["candidates"][0]["content"]["parts"][0]["text"]


def _call_openai_compatible(prompt, url, api_key, model):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 1024,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    resp = _post_json(url, payload, headers)
    msg = resp["choices"][0]["message"]
    # sebagian model reasoning menaruh jawaban di 'content'; abaikan 'reasoning_content'
    return msg.get("content") or ""


def _call_groq(prompt):
    return _call_openai_compatible(
        prompt, "https://api.groq.com/openai/v1/chat/completions",
        GROQ_API_KEY, GROQ_MODEL,
    )


def _call_nvidia(prompt):
    return _call_openai_compatible(
        prompt, "https://integrate.api.nvidia.com/v1/chat/completions",
        NVIDIA_API_KEY, NVIDIA_MODEL,
    )


def _call_cerebras(prompt):
    return _call_openai_compatible(
        prompt, "https://api.cerebras.ai/v1/chat/completions",
        CEREBRAS_API_KEY, CEREBRAS_MODEL,
    )


# ─── Parsing JSON dari output model ──────────────────────────────────────────

def _parse_json_response(text):
    """Ambil objek JSON pertama dari teks model (buang ```json fences dst)."""
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("```", 2)[1]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(cleaned[start:end + 1])
    except json.JSONDecodeError:
        return None


# ─── Rule-based fallback (tanpa LLM) ─────────────────────────────────────────

def _rule_based(meta):
    """Rekomendasi cadangan jika semua provider gagal. Tidak pernah meng-crash pipeline."""
    quadrant = meta.get("quadrant", "Q2")
    category = (meta.get("category") or "lainnya").lower()
    count = meta.get("complaint_count", 0)

    complexity = "high" if quadrant == "Q1" else "medium"
    est_days = {"air": 3, "listrik": 2, "banjir": 5, "jalan": 7, "sampah": 2}.get(category, 4)
    priority = 9 if quadrant == "Q1" else (6 if quadrant == "Q2" else 4)
    action = (
        f"Tindak lanjuti keluhan {category} di {meta.get('kecamatan', '-')} "
        f"({count} laporan). Koordinasikan dengan dinas terkait sesuai kuadran {quadrant}."
    )
    return {
        "complexity": complexity,
        "estimated_resolution_days": est_days,
        "recommended_action": action,
        "priority_score": priority,
        "summary": f"Keluhan {category} kuadran {quadrant} di {meta.get('kecamatan', '-')}.",
        "llm_provider": "rule-based-fallback",
    }


# ─── Entry point ─────────────────────────────────────────────────────────────

def enrich(prompt, meta):
    """Coba provider berurutan; kembalikan dict hasil + provider yang dipakai.
    `meta` dipakai untuk rule-based fallback bila semua LLM gagal."""
    providers = []
    if GEMINI_API_KEY:
        providers.append(("gemini", _call_gemini))
    if NVIDIA_API_KEY:
        providers.append(("nvidia", _call_nvidia))
    if GROQ_API_KEY:
        providers.append(("groq", _call_groq))
    if CEREBRAS_API_KEY:
        providers.append(("cerebras", _call_cerebras))

    for name, fn in providers:
        try:
            raw = fn(prompt)
            parsed = _parse_json_response(raw)
            if parsed:
                parsed["llm_provider"] = name
                return parsed
            print(f"[llm_client] {name}: respons tidak bisa di-parse, lanjut fallback.")
        except urllib.error.HTTPError as e:
            print(f"[llm_client] {name} HTTPError {e.code}: {e.reason}")
        except Exception as e:  # noqa: BLE001
            print(f"[llm_client] {name} gagal: {e}")

    print("[llm_client] Semua provider LLM gagal/kosong -> pakai rule-based fallback.")
    return _rule_based(meta)
