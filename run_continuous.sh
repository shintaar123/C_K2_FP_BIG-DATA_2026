#!/bin/bash
# Script untuk menjalankan pipeline Big Data EWS secara terus-menerus tanpa Airflow

echo "======================================================"
echo "  Surabaya Complaint EWS - Continuous Pipeline"
echo "======================================================"

# Install dependensi ingestion & MLFlow di spark-master jika belum ada
echo -e "\nMemastikan dependensi terinstall di spark-master..."
docker compose exec spark-master bash -c "pip install --upgrade pip -q && pip install -q feedparser==6.0.11 kafka-python-ng==2.2.3 requests==2.32.3 beautifulsoup4==4.12.3 python-dotenv==1.0.1 mlflow-skinny==2.17.2 boto3"

# Load .env untuk LLM
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

PKG="io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
KAFKA_PKG="${PKG},org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

LLM_ENV_ARGS=""
[ -n "$NVIDIA_API_KEY" ] && LLM_ENV_ARGS="-e NVIDIA_API_KEY=$NVIDIA_API_KEY $LLM_ENV_ARGS"
[ -n "$NVIDIA_MODEL" ] && LLM_ENV_ARGS="-e NVIDIA_MODEL=$NVIDIA_MODEL $LLM_ENV_ARGS"
[ -n "$GEMINI_API_KEY" ] && LLM_ENV_ARGS="-e GEMINI_API_KEY=$GEMINI_API_KEY $LLM_ENV_ARGS"
[ -n "$GROQ_API_KEY" ] && LLM_ENV_ARGS="-e GROQ_API_KEY=$GROQ_API_KEY $LLM_ENV_ARGS"
[ -n "$CEREBRAS_API_KEY" ] && LLM_ENV_ARGS="-e CEREBRAS_API_KEY=$CEREBRAS_API_KEY $LLM_ENV_ARGS"

spark_submit() {
    local packages=$1
    local py_file=$2
    shift 2
    local env_args="$@"

    echo -e "\n\033[1;36m=== spark-submit: $py_file ===\033[0m"
    
    local container_file="/opt/spark/work-dir/app/$py_file"
    if [[ "$py_file" == llm/* ]]; then
        container_file="/opt/spark/work-dir/$py_file"
    fi

    docker compose exec $env_args spark-master /opt/spark/bin/spark-submit \
        --packages "$packages" \
        --conf "spark.jars.ivy=/tmp/.ivy" \
        "$container_file"
}

run_ingestion() {
    echo -e "\n\033[1;33m[1/6] Ingestion (Scraping & Social) ke Kafka...\033[0m"
    docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_rss_to_kafka.py
    docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_social_to_kafka.py
}

# Jeda antar siklus pipeline (misal: 1 jam = 3600 detik)
SLEEP_INTERVAL=${1:-3600}

while true; do
    echo -e "\n\033[1;35m======================================================"
    echo "  MEMULAI SIKLUS PIPELINE PADA $(date)"
    echo -e "======================================================\033[0m"

    run_ingestion
    
    echo -e "\n\033[1;33m[2/6] Bronze Ingest - Kafka ke Delta Lake...\033[0m"
    spark_submit "$KAFKA_PKG" "bronze_ingest_once.py"
    
    echo -e "\n\033[1;33m[3/6] Silver Transform...\033[0m"
    spark_submit "$PKG" "silver_transform.py"
    
    echo -e "\n\033[1;33m[4/6] Predict Batch...\033[0m"
    spark_submit "$PKG" "ml/predict_batch.py"
    
    echo -e "\n\033[1;33m[5/6] Gold Aggregate...\033[0m"
    spark_submit "$PKG" "gold_aggregate.py"
    
    echo -e "\n\033[1;33m[6/6] LLM Enrichment...\033[0m"
    spark_submit "$PKG" "llm/llm_enrichment.py" $LLM_ENV_ARGS
    
    echo -e "\n\033[1;35m======================================================"
    echo "  SIKLUS SELESAI. Menunggu $SLEEP_INTERVAL detik..."
    echo "  (Tekan Ctrl+C untuk berhenti)"
    echo -e "======================================================\033[0m"
    
    sleep $SLEEP_INTERVAL
done
