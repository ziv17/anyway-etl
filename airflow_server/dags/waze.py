from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        "owner": "airflow",
    },
    schedule_interval="@daily",
    catchup=False,
    start_date=days_ago(2),
)


with DAG("waze", **dag_kwargs) as waze_dag:
    (
        CliBashOperator("anyway-etl waze get-data", task_id="get-waze-data")
        >> CliBashOperator(
            "anyway-etl waze import-to-db", task_id="import-waze-data-to-db"
        )
    )
