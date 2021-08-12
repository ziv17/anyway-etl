import os

ANYWAY_AIRFLOW_SERVER_ROOTDIR = os.environ.get('ANYWAY_AIRFLOW_SERVER_ROOTDIR')
if not ANYWAY_AIRFLOW_SERVER_ROOTDIR:
    ANYWAY_AIRFLOW_SERVER_ROOTDIR = os.path.dirname(os.path.dirname(__file__))

ANYWAY_ETL_VENV = os.environ.get('ANYWAY_ETL_VENV')
if not ANYWAY_ETL_VENV:
    ANYWAY_ETL_VENV = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'venv')

ANYWAY_ETL_AIRFLOW_PIP_INSTALL_DEPS = os.environ.get('ANYWAY_ETL_AIRFLOW_PIP_INSTALL_DEPS') == 'yes'

ANYWAY_BRANCH = os.environ.get('ANYWAY_BRANCH', 'dev')
