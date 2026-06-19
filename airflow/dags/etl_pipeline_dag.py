from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="etl_pipeline_dag",
    description="Bronze -> Silver (NLP+scoring) -> Gold (agregasi+4-kuadran)",
    schedule="0 8 * * *",          # setiap hari 08.00
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["etl", "sprint3", "sprint4"],
) as dag:
    # TODO (Pipeline/ML Engineer): hubungkan ke spark-submit bronze/silver/gold
    bronze_to_silver = EmptyOperator(task_id="bronze_to_silver")
    silver_to_gold = EmptyOperator(task_id="silver_to_gold")

    bronze_to_silver >> silver_to_gold