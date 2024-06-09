from airflow import DAG
import pendulum

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval='@weekly',
    catchup=False,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jerusalem"),
)


with DAG('load-streets', **dag_kwargs) as load_streets:
    CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process streets',
                    task_id='load-streets'
                    )
