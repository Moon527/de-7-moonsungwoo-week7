import logging
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


OUTPUT_DIRECTORY = Path("/opt/airflow/logs/q7-backfill")


def write_logical_date(ds, logical_date, run_id):
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIRECTORY / "{}.txt".format(ds)
    content = "ds={} logical_date={} run_id={}\n".format(
        ds, logical_date.isoformat(), run_id
    )
    path.write_text(content, encoding="utf-8")
    logging.info("Q7 wrote %s: %s", path, content.strip())
    return str(path)


with DAG(
    dag_id="backfill_demo_moonsungwoo",
    # Fixed seven days before the actual exercise date (2026-09-17 KST).
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=True,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q7"],
) as dag:
    write_date = PythonOperator(
        task_id="write_date_file",
        python_callable=write_logical_date,
    )
