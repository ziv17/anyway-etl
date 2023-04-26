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


with DAG('cbs', **dag_kwargs,
         description='by default imports emails and processes data for (current year - 1). '
                     'For back-fill do a manual run with following example json: '
                     '{"load_start_year": 2019}') as cbs_dag:
    CliBashOperator(
        cmd='anyway-etl cbs import-emails',
        skip_if=lambda context: context['dag_run'].conf.get('load_start_year'),
        task_id='import-emails'
    ) >> CliBashOperator(
        cmd='anyway-etl cbs process-files',
        skip_if=lambda context: context['dag_run'].conf.get('load_start_year'),
        task_id='process-files'
    ) >> [
        # for local development you can use the following command to parse all types sequentially:
        #   anyway-etl cbs parse-all
        CliBashOperator(
            cmd='anyway-etl cbs parse-accidents'
            '{% if dag_run.conf.get("load_start_year") %} --load-start-year {{ dag_run.conf["load_start_year"] }}{% endif %}',
            task_id='parse-accidents'
        ),
        CliBashOperator(
            cmd='anyway-etl cbs parse-involved'
            '{% if dag_run.conf.get("load_start_year") %} --load-start-year {{ dag_run.conf["load_start_year"] }}{% endif %}',
            task_id='parse-involved'
        ),
        CliBashOperator(
            cmd='anyway-etl cbs parse-vehicles'
            '{% if dag_run.conf.get("load_start_year") %} --load-start-year {{ dag_run.conf["load_start_year"] }}{% endif %}',
            task_id='parse-vehicles'
        ),
    ] >> CliBashOperator(
        cmd='anyway-etl cbs import-to-datastore'
        '{% if dag_run.conf.get("load_start_year") %} --load-start-year {{ dag_run.conf["load_start_year"] }}{% endif %}',
        task_id='import-to-datastore'
    ) >> CliBashOperator(
        cmd='anyway-etl cbs check-data-in-datastore',
        task_id='check-data-in-datastore'
    )
