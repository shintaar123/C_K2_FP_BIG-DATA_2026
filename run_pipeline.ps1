<#
.SYNOPSIS
    run_pipeline.ps1 - Menjalankan pipeline Big Data EWS end-to-end.
.DESCRIPTION
    Script otomatis: Bronze Ingest -> Train Models -> Silver Transform ->
    Predict Batch -> Gold Aggregate -> Anomaly Detection -> LLM Enrichment.
.PARAMETER Ingest
    Jalankan Bronze Ingest dari Kafka ke Bronze Delta Lake.
.PARAMETER Train
    Jalankan training model ML (Classifier, Importance, Urgency).
.EXAMPLE
    .\run_pipeline.ps1 -Ingest -Train      # Semua tahap dari awal
    .\run_pipeline.ps1                      # Silver -> Gold -> Anomaly -> LLM saja
#>

param(
    [switch]$Ingest,
    [switch]$Train
)

$ErrorActionPreference = "Stop"
$PKG       = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
$KAFKA_PKG = "$PKG,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

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
$nvidiaModel = if ($envVars["NVIDIA_MODEL"]) { $envVars["NVIDIA_MODEL"] } else { "z-ai/glm-5.1" }
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
        Write-Host "GAGAL di tahap: $pyFile" -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Surabaya Complaint EWS - Full Pipeline" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# -- TAHAP 1: Bronze Ingest ------------------------------------------------
if ($Ingest) {
    Write-Host "[1/7] Bronze Ingest - Kafka ke Delta Lake..." -ForegroundColor Yellow
    Spark-Submit $KAFKA_PKG "bronze_ingest_once.py"
    Write-Host "[1/7] Bronze Ingest SELESAI." -ForegroundColor Green
    Write-Host ""
}

# -- TAHAP 2-4: Train Models -----------------------------------------------
if ($Train) {
    Write-Host "[2/7] Train Classifier - Kategori..." -ForegroundColor Yellow
    Spark-Submit $PKG "ml/train_classifier.py"
    Write-Host "[2/7] Train Classifier SELESAI." -ForegroundColor Green
    Write-Host ""

    Write-Host "[3/7] Train Importance..." -ForegroundColor Yellow
    Spark-Submit $PKG "ml/train_importance.py"
    Write-Host "[3/7] Train Importance SELESAI." -ForegroundColor Green
    Write-Host ""

    Write-Host "[4/7] Train Urgency..." -ForegroundColor Yellow
    Spark-Submit $PKG "ml/train_urgency.py"
    Write-Host "[4/7] Train Urgency SELESAI." -ForegroundColor Green
    Write-Host ""
}

# -- TAHAP 5: Silver Transform + Predict -----------------------------------
Write-Host "[5/7] Silver Transform..." -ForegroundColor Yellow
Spark-Submit $PKG "silver_transform.py"
Write-Host "[5/7] Silver Transform SELESAI." -ForegroundColor Green
Write-Host ""

Write-Host "[5b/7] Predict Batch..." -ForegroundColor Yellow
Spark-Submit $PKG "ml/predict_batch.py"
Write-Host "[5b/7] Predict Batch SELESAI." -ForegroundColor Green
Write-Host ""

# -- TAHAP 6: Gold Aggregate -----------------------------------------------
Write-Host "[6/7] Gold Aggregate - Kuadran Eisenhower..." -ForegroundColor Yellow
Spark-Submit $PKG "gold_aggregate.py"
Write-Host "[6/7] Gold Aggregate SELESAI." -ForegroundColor Green
Write-Host ""

# -- TAHAP 6b: Anomaly Detection -------------------------------------------
Write-Host "[6b/7] Anomaly Detection - Isolation Forest..." -ForegroundColor Yellow
Spark-Submit $PKG "ml/train_anomaly.py"
Write-Host "[6b/7] Anomaly Detection SELESAI." -ForegroundColor Green
Write-Host ""

# -- TAHAP 7: LLM Enrichment -----------------------------------------------
Write-Host "[7/7] LLM Enrichment..." -ForegroundColor Yellow
# Kirimkan semua API key yang ada di .env ke dalam container Spark
Spark-Submit $PKG "llm/llm_enrichment.py" $llmEnvArgs
Write-Host "[7/7] LLM Enrichment SELESAI." -ForegroundColor Green
Write-Host ""

# -- SELESAI ----------------------------------------------------------------
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  PIPELINE SELESAI!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Verifikasi di Trino:" -ForegroundColor White
Write-Host "  docker compose exec trino trino" -ForegroundColor Gray
Write-Host "  SELECT * FROM delta.gold.complaint_daily;" -ForegroundColor Gray
Write-Host "  SELECT * FROM delta.gold.complaint_enriched;" -ForegroundColor Gray
Write-Host ""
