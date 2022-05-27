from airflow import DAG

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval=None,
    catchup=False,
)


with DAG('test_logs', **dag_kwargs) as test_logs:
    CliBashOperator(
        'anyway-etl anyway-kubectl-exec python3 main.py scripts test-airflow'
        task_id='test_logs'
    )
