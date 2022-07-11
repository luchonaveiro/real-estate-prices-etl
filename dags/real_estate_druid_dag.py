"""Real Estate Druid data DAG."""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from airflow.utils.dates import datetime, timedelta

DAG_ID = "real_estate_druid_data_dag"
DAG_OWNER = "luciano.naveiro"

INPUT_NOTEBOOK_PATH = "/opt/notebooks/input"
OUTPUT_NOTEBOOK_PATH = "/opt/notebooks/output"

default_args = {
    "owner": DAG_OWNER,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    DAG_ID,
    start_date=datetime(2022, 7, 1),
    description="Real Estate ETL DAG that ingests the data to Druid.",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False,
    max_active_tasks=1,
) as dag:

    houses_data_sensor = ExternalTaskSensor(
        task_id="houses_data_sensor",
        external_dag_id="real_estate_prices_dag",
        external_task_id="vacuum_tables",
        mode="reschedule",
    )

    get_druid_data = BashOperator(
        task_id=f"get_druid_data",
        bash_command=f"""
        papermill {INPUT_NOTEBOOK_PATH}/get_druid_data.ipynb {OUTPUT_NOTEBOOK_PATH}/get_druid_data_output.ipynb --log-output --log-level INFO
        """,
    )

    post_druid_data = BashOperator(
        task_id=f"post_druid_data",
        bash_command=f"""
        papermill {INPUT_NOTEBOOK_PATH}/post_druid_data.ipynb {OUTPUT_NOTEBOOK_PATH}/post_druid_data_output.ipynb --log-output --log-level INFO
        """,
    )

    houses_data_sensor >> get_druid_data >> post_druid_data
