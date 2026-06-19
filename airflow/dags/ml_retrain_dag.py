from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="ml_retrain_dag",
    description="Retrain semua model Spark MLlib dengan data terbaru",
    schedule="0 2 * * 1",          # setiap Senin 02.00
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ml", "retrain"],
) as dag:
    # TODO (ML Engineer): retrain kategori/importance/urgency/anomaly + log MLflow
    retrain_models = EmptyOperator(task_id="retrain_models")

    retrain_models