<#
.SYNOPSIS
    run_continuous.ps1 - Menjalankan pipeline Big Data EWS secara terus menerus (loop).
.DESCRIPTION
    Script otomatis: Ingestion -> Bronze Ingest -> Silver Transform ->
    Predict Batch -> Gold Aggregate -> LLM Enrichment.
    
    Karena berjalan terus-menerus, script ini memberikan jeda waktu (delay) antar siklus
    agar tidak over-limit API (terutama LLM seperti Gemini/NVIDIA).
#>

param(
    [int]$IntervalSeconds = 3600
)

$ErrorActionPreference = "Stop"
$PKG       = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
$KAFKA_PKG = "$PKG,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# ─── INSTALL DEPENDENSI INGESTION & MLFLOW DI SPARK-MASTER ─────────────────────────
Write-Host "Memastikan dependensi terinstall di spark-master..." -ForegroundColor Cyan
docker compose exec spark-master bash -c "pip install --upgrade pip -q && pip install -q feedparser==6.0.11 kafka-python-ng==2.2.3 requests==2.32.3 beautifulsoup4==4.12.3 python-dotenv==1.0.1 mlflow==2.17.2 boto3 urllib3==1.26.20"

# ─── BACA .env UNTUK API KEY LLM ─────────────────────────────────────────────
$envVars = @{}
if (Test-Path ".env") {
    Get-Content ".env" | ForEach-Object {
        if ($_ -match '^\s*([A-Za-z0-9_]+)\s*=\s*(.*)\s*$') {
            $envVars[$matches[1]] = $matches[2].Trim()
        }
    }
}

$nvidiaKey   = $envVars["NVIDIA_API_KEY"]
$nvidiaModel = if ($envVars["NVIDIA_MODEL"]) { $envVars["NVIDIA_MODEL"] } else { "meta/llama-3.3-70b-instruct" }
$geminiKey   = $envVars["GEMINI_API_KEY"]
$groqKey     = $envVars["GROQ_API_KEY"]
$cerebrasKey = $envVars["CEREBRAS_API_KEY"]

# Siapkan parameter environment untuk Docker
$llmEnvArgs = @()
if ($nvidiaKey)   { $llmEnvArgs += @("-e", "NVIDIA_API_KEY=$nvidiaKey") }
if ($nvidiaModel) { $llmEnvArgs += @("-e", "NVIDIA_MODEL=$nvidiaModel") }
if ($geminiKey)   { $llmEnvArgs += @("-e", "GEMINI_API_KEY=$geminiKey") }
if ($groqKey)     { $llmEnvArgs += @("-e", "GROQ_API_KEY=$groqKey") }
if ($cerebrasKey) { $llmEnvArgs += @("-e", "CEREBRAS_API_KEY=$cerebrasKey") }

# Tampilkan informasi LLM
Write-Host ""
if ($nvidiaKey) {
    Write-Host "[*] LLM Utama: NVIDIA NIM dengan Model: $nvidiaModel" -ForegroundColor Green
} elseif ($geminiKey) {
    Write-Host "[*] LLM Utama: Google Gemini" -ForegroundColor Green
} else {
    Write-Host "[!] Peringatan: NVIDIA_API_KEY/GEMINI_API_KEY tidak terdeteksi di .env." -ForegroundColor Yellow
    Write-Host "    LLM Enrichment akan berjalan menggunakan rule-based fallback (tanpa API)." -ForegroundColor Yellow
}

# ─── FUNGSI SUBMIT SPARK (DENGAN DOCKER ENV) ──────────────────────────────────
function Spark-Submit {
    param(
        [string]$packages, 
        [string]$pyFile,
        [string[]]$envArgs = @()
    )
    Write-Host "=== spark-submit: $pyFile ===" -ForegroundColor Cyan
    
    # Tentukan path di dalam container Spark
    $containerFile = "/opt/spark/work-dir/app/$pyFile"
    if ($pyFile.StartsWith("llm/")) {
        $containerFile = "/opt/spark/work-dir/$pyFile"
    }

    # Susun argumen lengkap untuk docker compose
    $dockerArgs = @("compose", "exec") + $envArgs + @(
        "spark-master", "/opt/spark/bin/spark-submit",
        "--packages", $packages,
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        $containerFile
    )

    & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Peringatan: GAGAL di tahap: $pyFile" -ForegroundColor Red
    }
}

function Run-Ingestion {
    Write-Host ""
    Write-Host "[1/6] Ingestion (Scraping & Social) ke Kafka..." -ForegroundColor Yellow
    
    # Jalankan run_rss_to_kafka.py
    & docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_rss_to_kafka.py
    
    # Jalankan run_social_to_kafka.py
    & docker compose exec spark-master python3 /opt/spark/work-dir/ingestion/run_social_to_kafka.py
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  Surabaya Complaint EWS - Continuous Pipeline" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

while ($true) {
    $startTime = Get-Date
    Write-Host "======================================================" -ForegroundColor Magenta
    Write-Host "  MEMULAI SIKLUS PIPELINE PADA $startTime" -ForegroundColor Magenta
    Write-Host "======================================================" -ForegroundColor Magenta

    # -- TAHAP 1: Ingestion ------------------------------------------------
    Run-Ingestion

    # -- TAHAP 2: Bronze Ingest ------------------------------------------------
    Write-Host ""
    Write-Host "[2/6] Bronze Ingest - Kafka ke Delta Lake..." -ForegroundColor Yellow
    Spark-Submit $KAFKA_PKG "bronze_ingest_once.py"

    # -- TAHAP 3: Silver Transform -----------------------------------
    Write-Host ""
    Write-Host "[3/6] Silver Transform..." -ForegroundColor Yellow
    Spark-Submit $PKG "silver_transform.py"

    # -- TAHAP 4: Predict Batch -----------------------------------
    Write-Host ""
    Write-Host "[4/6] Predict Batch..." -ForegroundColor Yellow
    Spark-Submit $PKG "ml/predict_batch.py"

    # -- TAHAP 5: Gold Aggregate -----------------------------------------------
    Write-Host ""
    Write-Host "[5/6] Gold Aggregate - Kuadran Eisenhower..." -ForegroundColor Yellow
    Spark-Submit $PKG "gold_aggregate.py"

    # -- TAHAP 6: LLM Enrichment -----------------------------------------------
    Write-Host ""
    Write-Host "[6/6] LLM Enrichment..." -ForegroundColor Yellow
    Spark-Submit $PKG "llm/llm_enrichment.py" $llmEnvArgs

    # -- SELESAI SIKLUS --------------------------------------------------------
    $endTime = Get-Date
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Magenta
    Write-Host "  SIKLUS SELESAI PADA $endTime" -ForegroundColor Magenta
    Write-Host "  Menunggu $IntervalSeconds detik sebelum siklus berikutnya..." -ForegroundColor Magenta
    Write-Host "  (Tekan Ctrl+C untuk berhenti)" -ForegroundColor Magenta
    Write-Host "======================================================" -ForegroundColor Magenta
    
    Start-Sleep -Seconds $IntervalSeconds
}
