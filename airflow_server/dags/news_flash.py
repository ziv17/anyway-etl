from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    catchup=False,
    start_date=days_ago(2),
)


with DAG('process-news-flash', **dag_kwargs, schedule_interval='0,30 * * * *') as process_news_flash_dag:
    CliBashOperator(
        'anyway-etl anyway-kubectl-exec python3 main.py process news-flash',
        task_id='process-news-flash'
    )


with DAG('update-news-flash', **dag_kwargs, schedule_interval=None,
         description='Update a single news flash item based on id, must run manually with json, example:'
                     '{"news_flash_id": "65516"}') as update_news_flash_dag:
    CliBashOperator(
        'anyway-etl anyway-kubectl-exec python3 main.py '
        'update-news-flash update --news_flash_id {{ dag_run.conf["news_flash_id"] }}',
        task_id='update-news-flash'
    )
