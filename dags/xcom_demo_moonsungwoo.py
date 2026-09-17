import logging
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def calculate_total(ti):
    numbers = list(range(1, 11))
    total = sum(numbers)
    logging.info("Q6 attempt=%s; sum(%s)=%s", ti.try_number, numbers, total)
    # Use the task attempt, not a local file, so retries work across workers.
    if ti.try_number == 1:
        raise ValueError("Q6 intentional first-attempt failure; retry should succeed")
    logging.info("Q6 calculation succeeded on attempt=%s; returning %s", ti.try_number, total)
    return total


def receive_total(ti):
    total = ti.xcom_pull(task_ids="calculate_total", key="return_value")
    expected = sum(range(1, 11))
    if total != expected:
        raise ValueError("Unexpected XCom value: {!r}".format(total))
    logging.info("Q6 xcom_pull: calculate_total.return_value=%s", total)
    logging.info("Q6 received value verified; doubled=%s", total * 2)
    return total * 2


with DAG(
    dag_id="xcom_demo_moonsungwoo",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q6"],
) as dag:
    calculate = PythonOperator(
        task_id="calculate_total",
        python_callable=calculate_total,
        retries=2,
        retry_delay=timedelta(seconds=30),
        show_return_value_in_logs=True,
    )
    receive = PythonOperator(
        task_id="receive_total",
        python_callable=receive_total,
        show_return_value_in_logs=True,
    )
    calculate >> receive
