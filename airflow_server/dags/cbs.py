from airflow import DAG
from airflow.utils.dates import days_ago

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval='@weekly',
    catchup=False,
    start_date=days_ago(2),
)


with DAG('cbs', **dag_kwargs) as cbs_dag:
    CliBashOperator(
        'anyway-etl cbs import-emails', task_id='import-emails'
    ) >> CliBashOperator(
        'anyway-etl cbs process-files', task_id='process-files'
    ) >> [
        # for local development you can use the following command to parse all types sequentially:
        #   anyway-etl cbs parse-all
        CliBashOperator('anyway-etl cbs parse-accidents', task_id='parse-accidents'),
        CliBashOperator('anyway-etl cbs parse-involved', task_id='parse-involved'),
        CliBashOperator('anyway-etl cbs parse-vehicles', task_id='parse-vehicles'),
    ] >> CliBashOperator(
        'anyway-etl cbs import-to-datastore', task_id='import-to-datastore'
    ) >> CliBashOperator(
        'anyway-etl cbs check-data-in-datastore', task_id='check-data-in-datastore'
    )
