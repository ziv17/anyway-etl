from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(1),
)


with DAG('load-suburban-junctions', **dag_kwargs) as load_suburban_junctions:
    CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process suburban-junctions',
        task_id='load-suburban-junctions'
    )
