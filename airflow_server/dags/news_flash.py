from textwrap import dedent

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


with DAG('process-news-flash', **dag_kwargs, schedule_interval='*/5 * * * *') as process_news_flash_dag:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py process news-flash',
        task_id='process-news-flash'
    )


with DAG('update-news-flash', **dag_kwargs, schedule_interval=None,
         description='Update a single news flash item based on id, must run manually with json, example:'
                     '{"news_flash_id": "65516"}') as update_news_flash_dag:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py '
        'update-news-flash update --news_flash_id {{ dag_run.conf["news_flash_id"] }}',
        task_id='update-news-flash'
    )

with DAG('update-all-news-flash', **dag_kwargs, schedule_interval=None,
         description='Update all news flash') as update_all_news_flash_dag:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py '
        'update-news-flash update --update_cbs_location_only',
        task_id='update-all-news-flash'
    )


with DAG('test-anyway-kubectl-exec', **dag_kwargs, schedule_interval=None) as test_anyway_kubectl_Exec:
    CliBashOperator(
        cmd='''anyway-etl anyway-kubectl-exec -- python3 -c "{}"'''.format(dedent("""
        import logging, time
        logging.basicConfig(level=logging.DEBUG)
        for i in range(20):
          logging.info(str(i))
          time.sleep(2)
        """)),
        task_id='test-anyway-kubectl-exec'
    )
