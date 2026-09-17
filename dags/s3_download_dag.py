import csv
import logging
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def download_csv():
    bucket = Variable.get("week7_bucket_name")
    destination = Path("/opt/airflow/data/data.csv")
    destination.parent.mkdir(parents=True, exist_ok=True)
    hook = S3Hook(aws_conn_id="week7_aws")
    hook.get_conn().download_file(bucket, "data.csv", str(destination))
    with destination.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    logging.info("Q1 S3 DOWNLOAD: s3://%s/data.csv -> %s", bucket, destination)
    logging.info("Q1 CSV: rows=%s bytes=%s", len(rows), destination.stat().st_size)
    for row in rows:
        logging.info("Q1 CSV ROW: name=%s age=%s", row["name"], row["age"])
    return str(destination)


with DAG(
    dag_id="s3_download_dag",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q1"],
) as dag:
    download = PythonOperator(task_id="download_csv", python_callable=download_csv)
