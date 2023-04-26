from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval=None,
    catchup=False,
    start_date=days_ago(2)
)


with DAG('test-logs', **dag_kwargs) as test_logs:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py scripts test-airflow',
        task_id='test_logs'
    )
