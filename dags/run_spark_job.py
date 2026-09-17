import pendulum

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="run_spark_job",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q2"],
) as dag:
    analyze = SparkSubmitOperator(
        task_id="analyze_csv",
        conn_id="week7_spark",
        application="/opt/airflow/q2/analyze_csv.py",
        application_args=["--input", "/opt/airflow/data/data.csv"],
        name="Q2-analyze-moonsungwoo",
        driver_memory="1g",
        conf={"spark.driver.host": "127.0.0.1", "spark.driver.bindAddress": "127.0.0.1", "spark.sql.shuffle.partitions": "2"},
        env_vars={"SPARK_LOCAL_IP": "127.0.0.1"},
    )
