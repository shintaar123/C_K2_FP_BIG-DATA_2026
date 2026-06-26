"""
Daftar sumber RSS yang di-scrape.
Tambah/edit URL di sini saja -- scraper baca dari list ini, jangan hardcode di file lain.

Cara cek RSS valid: buka URL di browser, harus muncul XML (bukan halaman HTML biasa).
Kalau satu sumber matiin RSS-nya nanti, tinggal comment-out barisnya, pipeline lain gak kena dampak.
"""

RSS_SOURCES = [
    {
        "source_name": "detik_jatim",
        "feed_url": "https://www.detik.com/jatim/rss",
        "category_hint": "berita_umum",
    },
    {
        "source_name": "beritajatim",
        "feed_url": "https://beritajatim.com/feed",
        "category_hint": "berita_umum",
    },
    {
        "source_name": "kompas_surabaya",
        "feed_url": "https://surabaya.kompas.com/rss",
        "category_hint": "berita_umum",
    },
    # surabaya.go.id dipakai sebagai data SEKUNDER (wilayah/kelurahan),
    # bukan RSS berita -- jangan ditambah di sini, itu job-nya bukan scraper RSS.
]

# Kafka
import os
# Default kafka:9094 (listener INTERNAL) karena script ingestion ini selalu
# dijalankan DI DALAM container (docker compose exec spark-master ...), di mana
# 'localhost' = container itu sendiri, bukan broker Kafka. Bisa di-override
# lewat env KAFKA_BOOTSTRAP_SERVERS kalau dijalankan dari host.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9094")
KAFKA_TOPIC_RSS = "raw-rss"

# Keyword sederhana buat filter awal: hanya simpan artikel yang KEMUNGKINAN soal keluhan publik.
# Ini bukan klasifikasi final (itu kerjaan Silver/NLP), cuma biar Bronze gak penuh artikel olahraga/gosip.
RELEVANT_KEYWORDS = [
    "pdam", "air", "pipa bocor", "sampah", "tps", "banjir", "genangan",
    "jalan rusak", "jalan berlubang", "lampu mati", "pln", "macet",
    "keluhan", "warga", "dprd surabaya", "pemkot surabaya",
]

# ── Reddit ───────────────────────────────────────────────────────────────────
REDDIT_SUBREDDITS = ["indonesia", "Surabaya"]
REDDIT_SEARCH_KEYWORDS = [
    "Surabaya air", "Surabaya sampah", "Surabaya banjir",
    "PDAM Surabaya", "Surabaya jalan rusak",
]
KAFKA_TOPIC_REDDIT = "raw-reddit"

# ── YouTube ──────────────────────────────────────────────────────────────────
YOUTUBE_SEARCH_QUERIES = [
    "keluhan PDAM Surabaya",
    "banjir Surabaya",
    "sampah Surabaya",
    "jalan rusak Surabaya",
    "air mati Surabaya",
]
KAFKA_TOPIC_YOUTUBE = "raw-yt"

# ── X (Twitter) ──────────────────────────────────────────────────────────────
X_SEARCH_QUERIES = [
    "#PDAMSurabaya",
    "#sampahsurabaya",
    "keluhan Surabaya",
    "air mati Surabaya",
    "@pemkot_sby keluhan",
]
KAFKA_TOPIC_X = "raw-x"

# ── Threads (Meta) ───────────────────────────────────────────────────────────
# Hashtag/profil publik yang dipantau. Threads sangat agresif anti-bot (butuh login),
# jadi scraper berusaha best-effort lalu fallback ke data generate realistis.
THREADS_HASHTAGS = ["surabaya", "keluhansurabaya", "pdamsurabaya", "sampahsurabaya"]
THREADS_PROFILES = ["pemkotsurabaya"]
KAFKA_TOPIC_THREADS = "raw-threads"