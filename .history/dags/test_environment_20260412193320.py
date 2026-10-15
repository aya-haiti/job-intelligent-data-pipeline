from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_environment",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "setup"],
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo Airflow fonctionne"
    )

    test_dwh = PostgresOperator(
        task_id="test_dwh_connection",
        postgres_conn_id="postgres_dwh",
        sql="SELECT version();"
    )

    hello >> test_dwh