import logging
from contextlib import closing

import pendulum
from psycopg2 import sql

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_people():
    table_name = Variable.get("week7_table_name")
    hook = PostgresHook(postgres_conn_id="week7_postgres")
    with closing(hook.get_conn()) as connection:
        query = sql.SQL("SELECT name, age FROM {} ORDER BY name").format(
            sql.Identifier(table_name)
        ).as_string(connection)
    records = hook.get_records(query)
    logging.info("Q1 QUERY: %s", query)
    for name, age in records:
        logging.info("Q1 DB ROW: name=%s age=%s", name, age)
    logging.info("Q1 QUERY_RESULT_ROWS=%s", len(records))
    return records


with DAG(
    dag_id="db_query_dag",
    start_date=pendulum.datetime(2026, 9, 17, tz="Asia/Seoul"),
    schedule="*/5 * * * *",
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"owner": "moonsungwoo"},
    tags=["week7", "Q1"],
) as dag:
    query = PythonOperator(task_id="query_people", python_callable=query_people)
