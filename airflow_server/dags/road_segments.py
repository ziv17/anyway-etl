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


with DAG('load-road-segments', **dag_kwargs) as load_road_segments:
    CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process road-segments',
        task_id='load-road-segments'
    )
