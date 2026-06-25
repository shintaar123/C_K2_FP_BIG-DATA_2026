from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

INGESTION_DIR = "/opt/airflow/ingestion"
PYTHON = "python"

with DAG(
    dag_id="daily_scraping_dag",
    description="Scraping semua sumber (RSS, YouTube, X+Reddit generated) -> Kafka",
    schedule="0 * * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ingestion", "sprint2"],
) as dag:

    scrape_rss = BashOperator(
        task_id="scrape_rss",
        bash_command=f"cd {INGESTION_DIR} && {PYTHON} run_rss_to_kafka.py",
    )

    scrape_social = BashOperator(
        task_id="scrape_social",
        bash_command=f"cd {INGESTION_DIR} && {PYTHON} run_social_to_kafka.py",
    )

    # RSS dan social berjalan paralel (tidak saling bergantung)
    [scrape_rss, scrape_social]