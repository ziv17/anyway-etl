from airflow import DAG
import pendulum
from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    schedule_interval='@weekly',
    catchup=False,
    start_date=pendulum.datetime(2022, 10, 1, tz="Asia/Jerusalem"),
)


with DAG('import-email-to-s3-and-update-data', **dag_kwargs) as import_email_to_s3_and_update_data:
    CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py scripts importemail',
        task_id='import-email-to-s3'
    ) >> CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process cbs --source s3'
        '{% if dag_run.conf.get("load_start_year") %} --load_start_year {{ dag_run.conf["load_start_year"] }}{% endif %}',
        task_id='cbs-import-from-s3'
    ) >> CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process infographics-data-cache-for-road-segments',
        task_id='fill-infographics-cache-for-road-segments'
    ) >> CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 main.py process cache update-street',
        task_id='fill-infographics-cache-for-streets'
    )
