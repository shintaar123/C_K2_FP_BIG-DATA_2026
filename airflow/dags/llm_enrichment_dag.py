from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="llm_enrichment_dag",
    description="Kirim masalah Q1+Q2 ke LLM, simpan ke LLM-Gold",
    schedule="0 10 * * *",         # setiap hari 10.00
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["llm", "sprint5"],
) as dag:
    # TODO (Pipeline+LLM): panggil llm_enrichment.py
    enrich_with_llm = EmptyOperator(task_id="enrich_with_llm")

    enrich_with_llm