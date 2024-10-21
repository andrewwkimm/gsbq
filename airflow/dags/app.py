"""The Airflow DAGs."""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="gsbq",
    default_args=default_args,
    description="DAG to move data from Google Sheets to BigQuery.",
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 1),
    catchup=False,
) as dag:

    gsbq_script_path = os.path.join(
        os.getenv("AIRFLOW_HOME", "/usr/local/airflow"), "gsbq", "app.py"
    )

    run_script: BashOperator = BashOperator(
        task_id="run_script", bash_command=f"python {gsbq_script_path}"
    )
