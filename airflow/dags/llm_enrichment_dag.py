"""
llm_enrichment_dag  (Pipeline + LLM Engineer)
Kirim cluster Q1+Q2 dari Gold ke LLM (Gemini->Groq->Cerebras->rule-based),
simpan hasil ke gold.complaint_enriched (LLM-Gold).
Jadwal: setiap hari 10.00 WIB (setelah etl_pipeline_dag selesai jam 08.00).

API key LLM diambil dari Airflow Variable bila tersedia, lalu diteruskan ke
container spark-master sebagai environment variable saat spark-submit.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark_submit_helper import run_spark_job

LLM_SCRIPT = "/opt/spark/work-dir/llm/llm_enrichment.py"


def _enrich(**_):
    # ambil API key dari Airflow Variable (set lewat UI/CLI); kosong -> rule-based fallback
    try:
        from airflow.models import Variable
        env = {
            "GEMINI_API_KEY":   Variable.get("GEMINI_API_KEY", default_var=""),
            "NVIDIA_API_KEY":   Variable.get("NVIDIA_API_KEY", default_var=""),
            "GROQ_API_KEY":     Variable.get("GROQ_API_KEY", default_var=""),
            "CEREBRAS_API_KEY": Variable.get("CEREBRAS_API_KEY", default_var=""),
        }
    except Exception:
        env = {}
    run_spark_job(LLM_SCRIPT, env={k: v for k, v in env.items() if v})


with DAG(
    dag_id="llm_enrichment_dag",
    description="Gold Q1+Q2 -> LLM -> LLM-Gold (gold.complaint_enriched)",
    schedule="0 10 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["llm", "sprint5", "pipeline"],
) as dag:

    enrich_with_llm = PythonOperator(
        task_id="enrich_with_llm",
        python_callable=_enrich,
    )

    enrich_with_llm
