from airflow import DAG
from airflow.models.param import Param
import pendulum
from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Jerusalem")
)


with DAG('generate-infographics-images', **dag_kwargs,      params={
    "newsflash_id": Param(0, type="integer"),
},) as fill_infographics_cache_dag:
    CliBashOperator(cmd='anyway-etl anyway-kubectl-exec python3 \
            main.py generate infographics-pictures --id {dag_kwargs[params][newsflash_id]}',
        task_id='generate-infographics-images',
    )
