"""
etl_pipeline_dag  (Pipeline + LLM Engineer)
Bronze -> Silver -> predict (scoring model) -> Gold (agregasi + 4-kuadran).
Jadwal: setiap hari 08.00 WIB.

Tiap task = spark-submit di container spark-master (lihat spark_submit_helper).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark_submit_helper import run_spark_job

APP = "/opt/spark/work-dir/app"

default_args = {"retries": 1}

with DAG(
    dag_id="etl_pipeline_dag",
    description="Bronze -> Silver (NLP+scoring) -> predict -> Gold (agregasi+4-kuadran)",
    schedule="0 8 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "sprint3", "sprint4", "pipeline"],
) as dag:

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{APP}/silver_transform.py"},
    )

    predict_scores = PythonOperator(
        task_id="predict_scores",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{APP}/ml/predict_batch.py"},
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{APP}/gold_aggregate.py"},
    )

    bronze_to_silver >> predict_scores >> silver_to_gold
