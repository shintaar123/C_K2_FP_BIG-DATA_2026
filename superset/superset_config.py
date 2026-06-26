"""
superset_config.py — konfigurasi Apache Superset untuk Surabaya EWS.

WAJIB di-mount ke /app/pythonpath/superset_config.py supaya Superset benar-benar
pakai Postgres (bukan SQLite default), dan supaya SECRET_KEY konsisten antar
restart sehingga data terenkripsi (mis. password datasource) tetap bisa
di-decrypt.

Tanpa file ini, image apache/superset:4.1.1 fallback ke SQLite di filesystem
container -> tidak persistent + tabel partial -> 500 errors di UI.
"""

import os

# ─── Database metadata ──────────────────────────────────────────────────────
# Pakai Postgres yang sama dengan Hive Metastore (container hive-postgres).
# Database `superset` sudah dibuat lewat docker-compose env / migration.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://hive:hivepassword@hive-postgres:5432/superset",
)

# ─── Secret key ─────────────────────────────────────────────────────────────
# Ambil dari env (SUPERSET_SECRET_KEY) supaya konsisten antar service & restart.
# Fallback ke nilai dev — JANGAN dipakai di production.
SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "CHANGE_ME_super_secret_key_min_32chars_long_for_surabaya_ews",
)

# Untuk Flask-AppBuilder (juga butuh secret)
WTF_CSRF_ENABLED = True
# Bebas-kan endpoint API yang biasa diakses programmatik (bootstrap script).
# JWT bearer sudah cukup untuk auth, CSRF tidak diperlukan.
WTF_CSRF_EXEMPT_LIST = [
    "superset.views.core.log",
    "superset.charts.data.api.ChartDataRestApi",
]

# ─── Cache config (in-memory, cukup untuk demo) ─────────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60,
}
DATA_CACHE_CONFIG = CACHE_CONFIG

# ─── Feature flags ──────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    # Aktifkan API JSON penuh
    "VERSIONED_EXPORT": True,
}

# ─── Logging ────────────────────────────────────────────────────────────────
ENABLE_TIME_ROTATE = True

# ─── CORS (untuk dashboard FastAPI custom yang akses Trino lewat browser) ───
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["*"],
}

# Public role (gak dipakai di demo, tapi safe default)
PUBLIC_ROLE_LIKE = "Gamma"
