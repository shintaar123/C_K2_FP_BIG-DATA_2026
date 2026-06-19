from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="daily_scraping_dag",
    description="Scraping semua sumber (RSS, X, Reddit, YouTube) -> Kafka",
    schedule="0 6 * * *",          # setiap hari 06.00
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["ingestion", "sprint2"],
) as dag:
    # TODO (Ingestion Engineer): ganti EmptyOperator dengan task scraper sungguhan
    scrape_rss = EmptyOperator(task_id="scrape_rss")
    scrape_x = EmptyOperator(task_id="scrape_x")
    scrape_reddit = EmptyOperator(task_id="scrape_reddit")
    scrape_youtube = EmptyOperator(task_id="scrape_youtube")

    [scrape_rss, scrape_x, scrape_reddit, scrape_youtube]