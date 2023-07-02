from airflow import DAG
import pendulum
from anyway_etl_airflow.operators.cli_bash_operator import CliBashOperator
import pendulum

dag_kwargs = dict(
    default_args={
        'owner': 'airflow',
        'max_active_runs': 1
    },
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Jerusalem")
)

with DAG('generate-and-send-infographics-images', **dag_kwargs, schedule_interval=None,
         description='Generates infographics for newsflash id and sends images to Telegram. '
                     'Must run manually with json, example:'
                     '{"news_flash_id": "65516"}') as generate_and_send_infographics_images_dag:
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py '
            'process infographics-pictures --id {{ dag_run.conf["news_flash_id"] }}',
        task_id='generate-infographics-images',
        retries=8
    ) >> \
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py '
            'upload generated_infographics --id {{ dag_run.conf["news_flash_id"] }}',
        task_id='upload-infographics-images'
    ) >> \
    CliBashOperator(
        cmd='anyway-etl anyway-kubectl-exec python3 main.py '
            'telegram publish-notification --id {{ dag_run.conf["news_flash_id"] }}',
        task_id='send-infographics-to-telegram'
    )