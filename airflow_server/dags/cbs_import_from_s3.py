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


with DAG('cbs-import-from-s3', **dag_kwargs) as cbs_import_from_s3:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py process cbs --source s3'
        '{% if dag_run.conf.get("load_start_year") %} --load_start_year {{ dag_run.conf["load_start_year"] }}{% endif %}',
        task_id='cbs-import-from-s3'
    )