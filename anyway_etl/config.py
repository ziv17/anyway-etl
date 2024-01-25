import os


ANYWAY_ETL_DATA_ROOT_PATH = os.environ.get('ANYWAY_ETL_DATA_ROOT_PATH')
if not ANYWAY_ETL_DATA_ROOT_PATH:
    ANYWAY_ETL_DATA_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.data')

ANYWAY_ETL_STATIC_DATA_ROOT_PATH = os.environ.get('ANYWAY_ETL_STATIC_DATA_ROOT_PATH')
if not ANYWAY_ETL_STATIC_DATA_ROOT_PATH:
    ANYWAY_ETL_STATIC_DATA_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'static_data')

SQLALCHEMY_URL = os.environ.get('SQLALCHEMY_URL', 'postgresql://anyway:anyway@localhost:9876/anyway')

ANYWAY_MAIN_CONTAINER_NAME = os.environ.get('ANYWAY_MAIN_CONTAINER_NAME')
