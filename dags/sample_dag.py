import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperato


def first_message():
    return "Hello from task1 - moonsungwoo"


def second_message():
    return "Hello from task2 - moonsungwoo"


with DAG(
    dag_id="sample_dag",
    start_date=pendulum.datetime(2026, 9, 16, tz="Asia/Seoul"),
    schedule="@daily",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q3"],
) as dag:
    start = EmptyOperator(task_id="start")
    task1 = PythonOperator(
        task_id="task1",
        python_callable=first_message,
        show_return_value_in_logs=True,
    )
    task2 = PythonOperator(
        task_id="task2",
        python_callable=second_message,
        show_return_value_in_logs=True,
    )
    end = EmptyOperator(task_id="end")

    start >> task1 >> task2 >> end
