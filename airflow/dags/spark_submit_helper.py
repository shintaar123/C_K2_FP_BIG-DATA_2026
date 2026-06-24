"""
spark_submit_helper.py
Helper bersama untuk semua DAG: menjalankan spark-submit DI DALAM container
spark-master lewat Docker SDK (socket /var/run/docker.sock di-mount ke Airflow).

Kenapa lewat docker exec, bukan SparkSubmitOperator?
- Image Airflow tidak punya binary spark-submit, jar Delta, maupun koneksi MinIO.
- spark-master sudah punya semua itu + folder ./spark & ./llm ter-mount.
Jadi cara paling andal di stack docker-compose ini: exec ke spark-master.

Prasyarat (sudah diatur di docker-compose.yml):
- /var/run/docker.sock di-mount ke container airflow-scheduler
- paket python `docker` ter-install (via _PIP_ADDITIONAL_REQUIREMENTS)
"""

DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4"
SPARK_CONTAINER = "spark-master"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"


def run_spark_job(script_path, packages=DELTA_PACKAGES, env=None):
    """Jalankan spark-submit di container spark-master dan stream log-nya.

    script_path : path absolut script di dalam container spark-master
                  (mis. /opt/spark/work-dir/app/gold_aggregate.py)
    packages    : argumen --packages
    env         : dict environment var tambahan (mis. API key LLM)
    Raise RuntimeError jika exit code != 0 supaya task Airflow gagal dengan benar.
    """
    import docker  # diimpor di dalam fungsi supaya DagBag tidak error saat parsing

    client = docker.from_env()
    container = client.containers.get(SPARK_CONTAINER)

    cmd = [
        SPARK_SUBMIT,
        "--packages", packages,
        "--conf", "spark.jars.ivy=/tmp/.ivy",
        script_path,
    ]

    print(f">>> exec di {SPARK_CONTAINER}: {' '.join(cmd)}")
    exit_code, output = container.exec_run(cmd, environment=env or {}, stream=False, demux=False)

    log = output.decode("utf-8", errors="replace") if isinstance(output, (bytes, bytearray)) else str(output)
    print(log)

    if exit_code != 0:
        raise RuntimeError(f"spark-submit gagal (exit {exit_code}) untuk {script_path}")
    print(f">>> OK: {script_path} selesai (exit 0)")
