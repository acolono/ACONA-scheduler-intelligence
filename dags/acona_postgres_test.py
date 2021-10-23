import datetime

from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="acona_postgres_dag",
    start_date=days_ago(2),
    schedule_interval="@once",
    catchup=False,
) as dag:

    get_success_scores = PostgresOperator(
        task_id="get_success_score", postgres_conn_id="acona_data_warehouse", sql="SELECT * FROM api.metric_success_score_ratio limit 10;"
    )

    get_success_scores