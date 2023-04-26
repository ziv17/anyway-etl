from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(2),
)


with DAG('fill-infographics-cache-for-streets', **dag_kwargs) as fill_infographics_cache_dag_for_streets:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py process cache update-street',
        task_id='fill-infographics-cache-for-streets'
    )
