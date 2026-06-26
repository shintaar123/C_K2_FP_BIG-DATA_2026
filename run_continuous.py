#!/usr/bin/env python3
"""
run_continuous.py - Menjalankan pipeline Big Data EWS secara terus menerus (loop) via Python.
Orchestrator ini dijalankan dari venv lokal kamu untuk memicu task di dalam Docker.
"""

import os
import sys
import time
import subprocess
from datetime import datetime

# ANSI Colors untuk output premium
CYAN = "\033[1;36m"
GREEN = "\033[1;32m"
YELLOW = "\033[1;33m"
MAGENTA = "\033[1;35m"
RED = "\033[1;31m"
RESET = "\033[0m"

# Set target paket Spark
PKG = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
KAFKA_PKG = f"{PKG},org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

def run_command(cmd, shell=False, check=True):
    try:
        result = subprocess.run(cmd, shell=shell, check=check)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"{RED}Error executing command: {cmd}\nDetail: {e}{RESET}")
        return False

def spark_submit(packages, py_file, env_args=[]):
    print(f"\n{CYAN}=== spark-submit: {py_file} ==={RESET}")
    
    container_file = f"/opt/spark/work-dir/app/{py_file}"
    if py_file.startswith("llm/"):
        container_file = f"/opt/spark/work-dir/{py_file}"

    # Gabungkan argumen docker compose
    docker_cmd = ["docker", "compose", "exec"] + env_args + [
        "spark-master", "/opt/spark/bin/spark-submit",
        "--packages", packages,
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        container_file
    ]
    run_command(docker_cmd)

def main():
    print(f"{CYAN}======================================================")
    print("  Surabaya Complaint EWS - Continuous Pipeline (venv)")
    print(f"======================================================{RESET}\n")

    # 1. Cek status docker
    print(f"{CYAN}[*] Memeriksa status container Docker...{RESET}")
    # Pastikan container berjalan
    docker_cmd = ["docker", "compose", "up", "-d", "kafka", "minio", "mlflow", "spark-master", "spark-worker", "hive-postgres", "hive-metastore"]
    run_command(docker_cmd)

    # 2. Pastikan library di spark-master terinstall
    print(f"\n{CYAN}[*] Sinkronisasi dependensi di dalam spark-master...{RESET}")
    pip_cmd = [
        "docker", "compose", "exec", "spark-master", "bash", "-c",
        "pip install --upgrade pip -q --default-timeout=300 && pip install -q --default-timeout=300 feedparser==6.0.11 kafka-python-ng==2.2.3 requests==2.32.3 beautifulsoup4==4.12.3 python-dotenv==1.0.1 mlflow==2.17.2 boto3 urllib3==1.26.20"
    ]
    run_command(pip_cmd)

    # 3. Load .env untuk LLM
    env_args = []
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if line.strip() and not line.startswith("#") and "=" in line:
                    key, val = line.strip().split("=", 1)
                    if key in ["NVIDIA_API_KEY", "NVIDIA_MODEL", "GEMINI_API_KEY", "GROQ_API_KEY", "CEREBRAS_API_KEY"]:
                        env_args.extend(["-e", f"{key}={val}"])

    # Durasi jeda (default 1 jam / 3600 detik)
    sleep_interval = 3600
    if len(sys.argv) > 1:
        try:
            sleep_interval = int(sys.argv[1])
        except ValueError:
            pass

    print(f"{GREEN}[*] Konfigurasi selesai. Jeda per siklus: {sleep_interval} detik.{RESET}")

    # 4. Loop Utama
    try:
        while True:
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{MAGENTA}======================================================")
            print(f"  MEMULAI SIKLUS PIPELINE PADA {start_time}")
            print(f"======================================================{RESET}")

            # -- TAHAP 1: Ingestion
            print(f"\n{YELLOW}[1/6] Ingestion (Scraping RSS & Social) ke Kafka...{RESET}")
            run_command(["docker", "compose", "exec", "spark-master", "python3", "/opt/spark/work-dir/ingestion/run_rss_to_kafka.py"])
            run_command(["docker", "compose", "exec", "spark-master", "python3", "/opt/spark/work-dir/ingestion/run_social_to_kafka.py"])

            # -- TAHAP 2: Bronze Ingest
            print(f"\n{YELLOW}[2/6] Bronze Ingest - Kafka ke Delta Lake...{RESET}")
            spark_submit(KAFKA_PKG, "bronze_ingest_once.py")

            # -- TAHAP 3: Silver Transform
            print(f"\n{YELLOW}[3/6] Silver Transform...{RESET}")
            spark_submit(PKG, "silver_transform.py")

            # -- TAHAP 4: Predict Batch
            print(f"\n{YELLOW}[4/6] Predict Batch...{RESET}")
            spark_submit(PKG, "ml/predict_batch.py")

            # -- TAHAP 5: Gold Aggregate
            print(f"\n{YELLOW}[5/6] Gold Aggregate...{RESET}")
            spark_submit(PKG, "gold_aggregate.py")

            # -- TAHAP 6: LLM Enrichment
            print(f"\n{YELLOW}[6/6] LLM Enrichment...{RESET}")
            spark_submit(PKG, "llm/llm_enrichment.py", env_args)

            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n{MAGENTA}======================================================")
            print(f"  SIKLUS SELESAI PADA {end_time}")
            print(f"  Menunggu {sleep_interval} detik sebelum siklus berikutnya...")
            print("  (Tekan Ctrl+C untuk berhenti)")
            print(f"======================================================{RESET}")
            
            time.sleep(sleep_interval)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}[*] Pipeline dihentikan oleh pengguna. Sampai jumpa!{RESET}")

if __name__ == "__main__":
    # Aktifkan mode warna ANSI di Windows Command Prompt
    if sys.platform == "win32":
        os.system("color")
    main()
