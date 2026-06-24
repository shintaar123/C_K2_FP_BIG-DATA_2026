"""
ml_retrain_dag
Retrain model Spark MLlib (kategori / importance / urgency) dengan data terbaru
dan log ulang ke MLflow. Jadwal: setiap Senin 02.00 WIB.

Catatan: tugas labeling & desain model tetap milik ML Engineer; DAG ini hanya
mengorkestrasi eksekusi ulang script training yang sudah ada.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from spark_submit_helper import run_spark_job

ML = "/opt/spark/work-dir/app/ml"

with DAG(
    dag_id="ml_retrain_dag",
    description="Retrain kategori/importance/urgency classifier + log MLflow",
    schedule="0 2 * * 1",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ml", "retrain"],
) as dag:

    retrain_category = PythonOperator(
        task_id="retrain_category",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{ML}/train_classifier.py"},
    )

    retrain_importance = PythonOperator(
        task_id="retrain_importance",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{ML}/train_importance.py"},
    )

    retrain_urgency = PythonOperator(
        task_id="retrain_urgency",
        python_callable=run_spark_job,
        op_kwargs={"script_path": f"{ML}/train_urgency.py"},
    )

    # model independen -> boleh paralel
    [retrain_category, retrain_importance, retrain_urgency]
