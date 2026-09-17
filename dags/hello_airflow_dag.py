import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_airflow_dag",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q1"],
) as dag:
    hello = BashOperator(task_id="say_hello", bash_command="echo 'Hello Airflow'")
