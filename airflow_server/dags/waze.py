from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        "owner": "airflow",
    },
    # disabled due to extreme load on DB and infrastructure
    # do not enable a schedule before fixing this dag logic to be more efficient
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(2),
)


with DAG("waze", **dag_kwargs) as waze_dag:
    (
        CliBashOperator(cmd="anyway-etl waze get-data", task_id="get-waze-data")
        >> CliBashOperator(
            cmd="anyway-etl waze import-to-db", task_id="import-waze-data-to-db"
        )
    )
