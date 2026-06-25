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
    schedule="15 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "sprint3", "sprint4", "pipeline"],
) as dag:

    # Package tambahan Spark untuk membaca dari Kafka
    KAFKA_PKG = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

    ingest_to_bronze = PythonOperator(
        task_id="ingest_to_bronze",
        python_callable=run_spark_job,
        op_kwargs={
            "script_path": f"{APP}/bronze_ingest_once.py",
            "packages": KAFKA_PKG
        },
    )

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

    ingest_to_bronze >> bronze_to_silver >> predict_scores >> silver_to_gold
