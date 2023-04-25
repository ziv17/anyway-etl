from airflow import DAG
from airflow.models.param import Param

from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator


dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
    },
)


with DAG('generate-infographics-images', **dag_kwargs,      params={
    "newsflash_id": Param(0, type="integer"),
},) as fill_infographics_cache_dag:
    CliBashOperator(
        'anyway-etl anyway-kubectl-exec python3 \
            main.py generate infographics_pictures --id {dag_kwargs[params][newsflash_id]}',
        task_id='generate-infographics-images',
    )
