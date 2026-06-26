"""
bootstrap.py — Auto-provision Superset dengan koneksi Trino + dataset + chart + dashboard
untuk proyek Surabaya Complaint Early Warning System.

Dijalankan otomatis oleh service `superset-bootstrap` di docker-compose, setelah
service `superset` siap menerima request.

Alur:
1) Login sebagai admin/admin (default Superset di stack ini)
2) Tambahkan database Trino (SQLAlchemy URI: trino://admin@trino:8080/delta)
3) Tambahkan dataset gold.complaint_daily & gold.complaint_enriched
4) Buat charts (termasuk "Scatter 4-Kuadran Eisenhower" yang wajib ada)
5) Buat dashboard "Surabaya EWS — Analitik" yang merangkum semua chart

Script ini IDEMPOTEN: kalau resource sudah ada (cek by name), skip — tidak
duplikasi. Kalau gagal pada satu chart, tetap lanjut ke chart berikutnya
supaya bootstrap parsial lebih baik daripada gagal total.

ENV (opsional, ada default):
- SUPERSET_URL       (default: http://superset:8088)
- SUPERSET_USERNAME  (default: admin)
- SUPERSET_PASSWORD  (default: admin)
- TRINO_HOST         (default: trino)
- TRINO_PORT         (default: 8080)
- TRINO_CATALOG      (default: delta)
- TRINO_USER         (default: admin)
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests


SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
USERNAME = os.environ.get("SUPERSET_USERNAME", "admin")
PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = os.environ.get("TRINO_PORT", "8080")
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "delta")
TRINO_USER = os.environ.get("TRINO_USER", "admin")

DB_NAME = "Trino Delta"
SCHEMA = "gold"
DATASETS = ["complaint_daily", "complaint_enriched"]

DASHBOARD_TITLE = "Surabaya EWS — Analitik Keluhan"


# ─── HTTP helpers ────────────────────────────────────────────────────────────
class SupersetClient:
    def __init__(self, base: str, username: str, password: str):
        self.base = base
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.csrf_token: Optional[str] = None
        self.username = username
        self.password = password

    def wait_until_ready(self, max_wait: int = 600, interval: int = 5) -> None:
        """Polling /health sampai Superset siap menerima request."""
        url = f"{self.base}/health"
        deadline = time.time() + max_wait
        while time.time() < deadline:
            try:
                r = self.session.get(url, timeout=5)
                if r.status_code == 200:
                    print(f"[ready] Superset OK ({r.text.strip()})")
                    return
            except requests.RequestException as e:
                print(f"[wait] Superset belum siap: {e}")
            time.sleep(interval)
        raise RuntimeError(f"Superset tidak siap setelah {max_wait}s di {url}")

    def login(self) -> None:
        url = f"{self.base}/api/v1/security/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True,
        }
        r = self.session.post(url, json=payload, timeout=30)
        r.raise_for_status()
        self.access_token = r.json()["access_token"]

        # CSRF token TIDAK wajib kalau pakai JWT Bearer di Superset 4.1.x.
        # Endpoint /api/v1/security/csrf_token/ kadang return 403 di versi ini
        # walau JWT-nya valid -> kita skip kalau gagal, lanjut pakai JWT-only.
        self.csrf_token = None
        try:
            csrf_r = self.session.get(
                f"{self.base}/api/v1/security/csrf_token/",
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )
            if csrf_r.status_code == 200:
                self.csrf_token = csrf_r.json().get("result")
                print(f"[login] OK sebagai {self.username} (+ CSRF)")
            else:
                print(f"[login] OK sebagai {self.username} "
                      f"(CSRF skip — status {csrf_r.status_code}, JWT-only)")
        except Exception as e:
            print(f"[login] OK sebagai {self.username} (CSRF skip — {e})")

        # Ambil ID user admin supaya bisa di-set eksplisit sebagai owner chart
        # & dashboard. Tanpa owners eksplisit, Superset coba pakai current_user
        # yang kadang ke-resolve sebagai Anonymous -> crash _sa_instance_state.
        self.user_id = 1  # admin hampir selalu id=1 di DB fresh (fallback aman)
        try:
            me = self.session.get(
                f"{self.base}/api/v1/me/",
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=10,
            )
            if me.status_code == 200:
                self.user_id = me.json()["result"]["id"]
                print(f"[login] admin user_id = {self.user_id}")
            else:
                print(f"[login] /me/ status {me.status_code}, fallback user_id=1")
        except Exception as e:
            print(f"[login] /me/ gagal ({e}), fallback user_id=1")

    def _headers(self, with_csrf: bool = False) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if with_csrf and self.csrf_token:
            h["X-CSRFToken"] = self.csrf_token
            h["Referer"] = self.base
        return h

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = self.session.get(
            f"{self.base}{path}", headers=self._headers(), params=params, timeout=60
        )
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        r = self.session.post(
            f"{self.base}{path}",
            headers=self._headers(with_csrf=True),
            json=body,
            timeout=60,
        )
        if r.status_code >= 400:
            print(f"[ERROR] POST {path} -> {r.status_code}: {r.text[:600]}")
        r.raise_for_status()
        return r.json()


# ─── Step 1: Database (Trino) ────────────────────────────────────────────────
def ensure_database(client: SupersetClient) -> int:
    sqlalchemy_uri = (
        f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}"
    )
    print(f"[db] Memastikan database '{DB_NAME}' -> {sqlalchemy_uri}")

    # Cek dulu kalau sudah ada
    existing = client.get(
        "/api/v1/database/",
        params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": DB_NAME}]})},
    )
    if existing.get("count", 0) > 0:
        db_id = existing["result"][0]["id"]
        print(f"[db] Sudah ada (id={db_id}), skip create.")
        return db_id

    body = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_run_async": True,
        "extra": json.dumps({
            "metadata_params": {},
            "engine_params": {},
            "schemas_allowed_for_file_upload": [],
        }),
    }
    res = client.post("/api/v1/database/", body)
    db_id = res["id"]
    print(f"[db] Created (id={db_id})")
    return db_id


# ─── Step 2: Datasets ────────────────────────────────────────────────────────
def ensure_dataset(client: SupersetClient, db_id: int, table_name: str) -> int:
    print(f"[dataset] Memastikan dataset '{SCHEMA}.{table_name}'")
    existing = client.get(
        "/api/v1/dataset/",
        params={"q": json.dumps({"filters": [
            {"col": "table_name", "opr": "eq", "value": table_name},
            {"col": "schema", "opr": "eq", "value": SCHEMA},
        ]})},
    )
    if existing.get("count", 0) > 0:
        ds_id = existing["result"][0]["id"]
        print(f"[dataset] Sudah ada (id={ds_id}), skip.")
        return ds_id

    body = {
        "database": db_id,
        "schema": SCHEMA,
        "table_name": table_name,
    }
    res = client.post("/api/v1/dataset/", body)
    ds_id = res["id"]
    print(f"[dataset] Created (id={ds_id})")
    return ds_id


# ─── Step 3: Charts ──────────────────────────────────────────────────────────
def _metric_simple(column_name: str, col_type: str, aggregate: str) -> Dict[str, Any]:
    """Helper untuk membangun "simple metric" Superset (struktur adhoc metric)."""
    label = f"{aggregate}({column_name})"
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": column_name, "type": col_type},
        "aggregate": aggregate,
        "label": label,
        "optionName": f"metric_{column_name}_{aggregate}",
    }


def _find_chart(client: SupersetClient, name: str) -> Optional[int]:
    existing = client.get(
        "/api/v1/chart/",
        params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": name}]})},
    )
    if existing.get("count", 0) > 0:
        return existing["result"][0]["id"]
    return None


def create_chart(
    client: SupersetClient,
    name: str,
    viz_type: str,
    dataset_id: int,
    params: Dict[str, Any],
) -> Optional[int]:
    """Buat chart. Idempoten: skip kalau sudah ada."""
    cid = _find_chart(client, name)
    if cid:
        print(f"[chart] '{name}' sudah ada (id={cid}), skip.")
        return cid

    params_with_ds = {**params, "datasource": f"{dataset_id}__table", "viz_type": viz_type}
    body = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(params_with_ds),
        # Set owner eksplisit -> hindari fallback ke Anonymous user (crash).
        "owners": [client.user_id],
    }
    try:
        res = client.post("/api/v1/chart/", body)
        cid = res["id"]
        print(f"[chart] '{name}' created (id={cid})")
        return cid
    except Exception as e:
        print(f"[chart] '{name}' GAGAL dibuat: {e}. Lanjut chart berikutnya.")
        return None


def build_charts(client: SupersetClient, ds_daily: int, ds_enriched: int) -> List[int]:
    chart_ids: List[int] = []

    # ── 1) SCATTER 4-KUADRAN EISENHOWER (WAJIB) ─────────────────────────────
    # x=avg_urgency, y=avg_importance, size=complaint_count, color=quadrant
    scatter_params = {
        "x": _metric_simple("avg_urgency", "DOUBLE", "AVG"),
        "y": _metric_simple("avg_importance", "DOUBLE", "AVG"),
        "size": _metric_simple("complaint_count", "BIGINT", "SUM"),
        "entity": "kecamatan",
        "series": "quadrant",
        "adhoc_filters": [],
        "row_limit": 1000,
        "x_axis_label": "Avg Urgency",
        "y_axis_label": "Avg Importance",
        "color_scheme": "supersetColors",
        "show_legend": True,
        "tooltipSizeFormat": "SMART_NUMBER",
    }
    cid = create_chart(
        client, "Scatter 4-Kuadran Eisenhower", "bubble_v2", ds_daily, scatter_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 2) Distribusi Kuadran (Pie) ─────────────────────────────────────────
    pie_params = {
        "groupby": ["quadrant"],
        "metric": _metric_simple("complaint_count", "BIGINT", "SUM"),
        "adhoc_filters": [],
        "row_limit": 10,
        "donut": True,
        "show_legend": True,
        "label_type": "key_percent",
        "color_scheme": "supersetColors",
    }
    cid = create_chart(client, "Distribusi Kuadran", "pie", ds_daily, pie_params)
    if cid:
        chart_ids.append(cid)

    # ── 3) Top Kecamatan (Bar) ──────────────────────────────────────────────
    bar_kec_params = {
        "groupby": ["kecamatan"],
        "metrics": [_metric_simple("complaint_count", "BIGINT", "SUM")],
        "adhoc_filters": [
            {
                "expressionType": "SIMPLE",
                "subject": "kecamatan",
                "operator": "!=",
                "comparator": "Tidak Diketahui",
                "clause": "WHERE",
            }
        ],
        "row_limit": 15,
        "order_desc": True,
        "show_legend": False,
        "color_scheme": "supersetColors",
    }
    cid = create_chart(
        client, "Top Kecamatan (Total Keluhan)", "dist_bar", ds_daily, bar_kec_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 4) Sebaran Kategori (Bar) ───────────────────────────────────────────
    bar_cat_params = {
        "groupby": ["category"],
        "metrics": [_metric_simple("complaint_count", "BIGINT", "SUM")],
        "adhoc_filters": [],
        "row_limit": 20,
        "order_desc": True,
        "show_legend": False,
        "color_scheme": "supersetColors",
    }
    cid = create_chart(
        client, "Sebaran Kategori", "dist_bar", ds_daily, bar_cat_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 5) Tren Harian per Kuadran (Time-Series Line) ───────────────────────
    trend_params = {
        "x_axis": "date",
        "groupby": ["quadrant"],
        "metrics": [_metric_simple("complaint_count", "BIGINT", "SUM")],
        "time_grain_sqla": "P1D",
        "adhoc_filters": [],
        "row_limit": 10000,
        "show_legend": True,
        "color_scheme": "supersetColors",
        "x_axis_title": "Tanggal",
        "y_axis_title": "Jumlah Keluhan",
        "seriesType": "line",
    }
    cid = create_chart(
        client, "Tren Harian per Kuadran", "echarts_timeseries_line", ds_daily, trend_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 6) Big Number: Total Keluhan ────────────────────────────────────────
    big_total_params = {
        "metric": _metric_simple("complaint_count", "BIGINT", "SUM"),
        "adhoc_filters": [],
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "subheader": "Total Keluhan Tercatat",
        "color_picker": {"r": 0, "g": 122, "b": 135, "a": 1},
    }
    cid = create_chart(
        client, "Total Keluhan", "big_number_total", ds_daily, big_total_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 7) Big Number: Q1 Critical ──────────────────────────────────────────
    big_q1_params = {
        "metric": _metric_simple("complaint_count", "BIGINT", "SUM"),
        "adhoc_filters": [
            {
                "expressionType": "SIMPLE",
                "subject": "quadrant",
                "operator": "==",
                "comparator": "Q1",
                "clause": "WHERE",
            }
        ],
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "subheader": "Cluster Kritis Q1",
        "color_picker": {"r": 255, "g": 0, "b": 85, "a": 1},
    }
    cid = create_chart(
        client, "Keluhan Kritis Q1", "big_number_total", ds_daily, big_q1_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 8) Heatmap Kecamatan × Kategori ─────────────────────────────────────
    heatmap_params = {
        "all_columns_x": "kecamatan",
        "all_columns_y": "category",
        "metric": _metric_simple("complaint_count", "BIGINT", "SUM"),
        "adhoc_filters": [
            {
                "expressionType": "SIMPLE",
                "subject": "kecamatan",
                "operator": "!=",
                "comparator": "Tidak Diketahui",
                "clause": "WHERE",
            }
        ],
        "row_limit": 1000,
        "linear_color_scheme": "schemeRdYlBu",
        "xscale_interval": 1,
        "yscale_interval": 1,
        "canvas_image_rendering": "pixelated",
        "normalize_across": "heatmap",
        "left_margin": "auto",
        "bottom_margin": "auto",
        "y_axis_bounds": [None, None],
        "y_axis_format": "SMART_NUMBER",
        "show_legend": True,
        "show_perc": True,
    }
    cid = create_chart(
        client, "Heatmap Kecamatan x Kategori", "heatmap", ds_daily, heatmap_params
    )
    if cid:
        chart_ids.append(cid)

    # ── 9) Tabel Top Prioritas LLM ──────────────────────────────────────────
    table_params = {
        "query_mode": "raw",
        "all_columns": [
            "priority_rank",
            "kecamatan",
            "category",
            "quadrant",
            "complaint_count",
            "complexity",
            "estimated_resolution_days",
            "llm_priority_score",
            "llm_summary",
            "llm_recommendation",
        ],
        "order_by_cols": ['["priority_rank", true]'],
        "row_limit": 50,
        "adhoc_filters": [],
        "table_timestamp_format": "smart_date",
        "show_cell_bars": True,
        "color_pn": True,
    }
    cid = create_chart(
        client, "Top Prioritas LLM-Enriched", "table", ds_enriched, table_params
    )
    if cid:
        chart_ids.append(cid)

    return chart_ids


# ─── Step 4: Dashboard ───────────────────────────────────────────────────────
def _build_position_json(chart_ids: List[int]) -> Dict[str, Any]:
    """Bangun position_json dashboard sederhana: header + grid 2 kolom.

    Format Superset position_json: tree dengan ROOT_ID -> GRID_ID -> rows -> charts.
    Ini implementasi minimal yang valid dan dapat di-render Superset.
    """
    rows: List[str] = []
    children: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": rows, "parents": ["ROOT_ID"]},
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
            "parents": ["ROOT_ID"],
        },
    }

    # Tata letak: 2 chart per row, 6 unit width tiap chart (total 12)
    row_idx = 0
    for i in range(0, len(chart_ids), 2):
        row_id = f"ROW-{row_idx}"
        row_children: List[str] = []
        for j, cid in enumerate(chart_ids[i : i + 2]):
            chart_node_id = f"CHART-{cid}"
            children[chart_node_id] = {
                "type": "CHART",
                "id": chart_node_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": cid,
                    "width": 6,
                    "height": 50,
                    "sliceName": f"chart-{cid}",
                },
            }
            row_children.append(chart_node_id)
        children[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        rows.append(row_id)
        row_idx += 1

    return children


def ensure_dashboard(client: SupersetClient, chart_ids: List[int]) -> int:
    print(f"[dashboard] Memastikan dashboard '{DASHBOARD_TITLE}'")
    existing = client.get(
        "/api/v1/dashboard/",
        params={"q": json.dumps({"filters": [
            {"col": "dashboard_title", "opr": "eq", "value": DASHBOARD_TITLE}
        ]})},
    )
    if existing.get("count", 0) > 0:
        dash_id = existing["result"][0]["id"]
        print(f"[dashboard] Sudah ada (id={dash_id}), skip create.")
        return dash_id

    position_json = _build_position_json(chart_ids)
    body = {
        "dashboard_title": DASHBOARD_TITLE,
        "published": True,
        "slug": "surabaya-ews-analitik",
        "owners": [client.user_id],
        "position_json": json.dumps(position_json),
        "css": "",
        "json_metadata": json.dumps({
            "color_scheme": "supersetColors",
            "refresh_frequency": 30,
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "label_colors": {
                "Q1": "#ff0055",
                "Q2": "#ff7700",
                "Q3": "#ffcc00",
                "Q4": "#00aaff",
            },
        }),
    }
    res = client.post("/api/v1/dashboard/", body)
    dash_id = res["id"]
    print(f"[dashboard] Created (id={dash_id})")

    # Tambahkan charts ke dashboard via PUT
    try:
        # Endpoint: PUT /api/v1/dashboard/{id} dengan field `charts` (list of chart ids)
        upd_body = {"json_metadata": body["json_metadata"]}
        # Setiap chart juga perlu di-update ownership dashboard-nya
        for cid in chart_ids:
            try:
                client.session.put(
                    f"{client.base}/api/v1/chart/{cid}",
                    headers=client._headers(with_csrf=True),
                    json={"dashboards": [dash_id]},
                    timeout=30,
                )
            except Exception as e:
                print(f"[dashboard] Gagal link chart {cid} -> dashboard: {e}")
    except Exception as e:
        print(f"[dashboard] Warning saat link chart: {e}")

    return dash_id


# ─── Main ────────────────────────────────────────────────────────────────────
def main() -> int:
    print("=" * 70)
    print("Superset Bootstrap — Surabaya EWS")
    print(f"  Superset: {SUPERSET_URL}")
    print(f"  Trino:    trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{TRINO_CATALOG}")
    print("=" * 70)

    client = SupersetClient(SUPERSET_URL, USERNAME, PASSWORD)
    client.wait_until_ready(max_wait=600, interval=5)
    client.login()

    db_id = ensure_database(client)
    ds_ids = {name: ensure_dataset(client, db_id, name) for name in DATASETS}

    chart_ids = build_charts(client, ds_ids["complaint_daily"], ds_ids["complaint_enriched"])
    print(f"[summary] {len(chart_ids)} chart berhasil dibuat/sudah ada.")

    if chart_ids:
        dash_id = ensure_dashboard(client, chart_ids)
        print(f"[summary] Dashboard ready: {SUPERSET_URL}/superset/dashboard/{dash_id}/")
    else:
        print("[summary] Tidak ada chart -> dashboard di-skip.")

    print("\nSelesai. Buka Superset di http://localhost:8088 (admin/admin)")
    print(f"Dashboard: '{DASHBOARD_TITLE}'")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"\n[FATAL] Bootstrap gagal: {exc}")
        # Exit non-zero supaya user tahu, tapi jangan crash container restart
        sys.exit(1)
