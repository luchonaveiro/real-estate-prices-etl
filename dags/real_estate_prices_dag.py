"""Real Estate DAG."""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from airflow.utils.dates import datetime, timedelta

DAG_ID = "real_estate_prices_dag"
DAG_OWNER = "luciano.naveiro"

INPUT_NOTEBOOK_PATH = "/opt/notebooks/input"
OUTPUT_NOTEBOOK_PATH = "/opt/notebooks/output"

CITIES = ["bern", "basel", "geneve", "zuerich"]

default_args = {
    "owner": DAG_OWNER,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    DAG_ID,
    start_date=datetime(2022, 7, 1),
    description="Real Estate ETL DAG that executes parametrized notebooks using papermill.",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False,
    max_active_tasks=1,
) as dag:

    start_etl = DummyOperator(task_id="start_etl")
    finish_etl = DummyOperator(task_id="finish_etl")

    for city in CITIES:

        get_houses_data = BashOperator(
            task_id=f"get_house_data_{city}",
            bash_command=f"""
            papermill {INPUT_NOTEBOOK_PATH}/get_houses_data.ipynb {OUTPUT_NOTEBOOK_PATH}/get_houses_data_output.ipynb --log-output --log-level INFO -p date {{{{ ds }}}} -p city {city} -p radius 40
            """,
        )

        merge_houses_prices = BashOperator(
            task_id=f"merge_house_prices_{city}",
            bash_command=f"""
            papermill {INPUT_NOTEBOOK_PATH}/merge_data.ipynb {OUTPUT_NOTEBOOK_PATH}/merge_data_output.ipynb --log-output --log-level INFO -p date {{{{ ds }}}} -p city {city}
            """,
        )

        clean_houses_prices = BashOperator(
            task_id=f"clean_house_prices_{city}",
            bash_command=f"""
            papermill {INPUT_NOTEBOOK_PATH}/create_golden_table.ipynb {OUTPUT_NOTEBOOK_PATH}/create_golden_table_output.ipynb --log-output --log-level INFO -p city {city}
            """,
        )

        (
            start_etl
            >> get_houses_data
            >> merge_houses_prices
            >> clean_houses_prices
            >> finish_etl
        )

    train_model = BashOperator(
        task_id=f"train_houses_prices_model",
        bash_command=f"""
        papermill {INPUT_NOTEBOOK_PATH}/house_prices_model.ipynb {OUTPUT_NOTEBOOK_PATH}/house_prices_model_output.ipynb --log-output --log-level INFO
        """,
    )

    vacuum_tables = BashOperator(
        task_id=f"vacuum_tables",
        bash_command=f"""
        papermill {INPUT_NOTEBOOK_PATH}/vacuum_tables.ipynb {OUTPUT_NOTEBOOK_PATH}/vacuum_tables_output.ipynb --log-output --log-level INFO
        """,
    )

    finish_etl >> train_model >> vacuum_tables
