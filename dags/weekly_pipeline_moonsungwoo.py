import hashlib
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

import boto3
import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


def s3_client():
    return boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE", "week7"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"),
    ).client("s3")


def download_bronze(run_id, params):
    run_key = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    directory = Path("/opt/airflow/data/q9") / run_key
    directory.mkdir(parents=True, exist_ok=True)
    source = directory / "netflix_titles.csv"
    bucket = params["bucket"]
    s3_client().download_file(bucket, "bronze/netflix_titles.csv", str(source))
    logging.info("Q9 DOWNLOAD: s3://%s/bronze/netflix_titles.csv -> %s (%s bytes)", bucket, source, source.stat().st_size)
    return {
        "bucket": bucket,
        "run_key": run_key,
        "directory": str(directory),
        "input_path": str(source),
        "upload_date": pendulum.now("Asia/Seoul").to_date_string(),
        "min_year": params["min_year"],
    }


def run_spark_transform(ti):
    context = ti.xcom_pull(task_ids="download_bronze")
    directory = Path(context["directory"])
    summary_path = directory / "summary.json"
    executable = shutil.which("spark-submit")
    if executable is None:
        raise RuntimeError("spark-submit is not installed in the Airflow worker")
    command = [
        executable, "--master", "local[2]", "--driver-memory", "1g",
        "--conf", "spark.driver.host=127.0.0.1",
        "--conf", "spark.driver.bindAddress=127.0.0.1",
        "--conf", "spark.sql.shuffle.partitions=2",
        "/opt/airflow/dags/jobs/transform.py",
        "--input", context["input_path"],
        "--output", str(directory / "parquet"),
        "--summary", str(summary_path),
        "--min-year", str(context["min_year"]),
    ]
    logging.info("Q9 SPARK COMMAND: %s", " ".join(command))
    environment = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1")
    # Stream the real spark-submit output into the Airflow task log.
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=environment) as process:
        for line in process.stdout:
            logging.info("%s", line.rstrip())
        if process.wait() != 0:
            raise RuntimeError("spark-submit failed; inspect the preceding Spark log")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    logging.info("Q9 SPARK SUMMARY: %s", summary)
    return dict(context, **summary)


def upload_silver(ti):
    context = ti.xcom_pull(task_ids="spark_transform")
    files = sorted(Path(context["output_path"]).glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No parquet part files to upload")
    s3 = s3_client()
    prefix = "silver/{}/".format(context["upload_date"])
    uploaded = []
    for path in files:
        # Unique names avoid replacing outputs from another DAG run.
        key = prefix + context["run_key"] + "-" + path.name
        s3.upload_file(str(path), context["bucket"], key)
        uploaded.append(key)
    listed = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=context["bucket"], Prefix=prefix):
        for item in page.get("Contents", []):
            listed[item["Key"]] = item["Size"]
            logging.info("Q9 S3 OBJECT: %s (%s bytes)", item["Key"], item["Size"])
    for path, key in zip(files, uploaded):
        if listed.get(key) != path.stat().st_size:
            raise ValueError("Uploaded object missing or size mismatch: " + key)
    logging.info("Q9 COMPLETE: input_rows=%s aggregate_rows=%s min_year=%s uploaded_files=%s prefix_objects=%s", context["input_rows"], context["aggregate_rows"], context["min_year"], len(uploaded), len(listed))
    logging.info("Q9 DESTINATION: s3://%s/%s", context["bucket"], prefix)
    return {"prefix": prefix, "uploaded_files": len(uploaded), "aggregate_rows": context["aggregate_rows"]}


with DAG(
    dag_id="weekly_pipeline_moonsungwoo",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "moonsungwoo"},
    params={"bucket": Param("de-7-moonsungwoo", type="string"), "min_year": Param(2015, type="integer", minimum=1900, maximum=2100)},
    tags=["week7", "Q9"],
) as dag:
    download = PythonOperator(task_id="download_bronze", python_callable=download_bronze)
    transform_task = PythonOperator(task_id="spark_transform", python_callable=run_spark_transform)
    upload = PythonOperator(task_id="upload_silver", python_callable=upload_silver)
    download >> transform_task >> upload
