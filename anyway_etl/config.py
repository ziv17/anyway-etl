import os


ANYWAY_ETL_DATA_ROOT_PATH = os.environ.get('ANYWAY_ETL_DATA_ROOT_PATH', './.data')
SQLALCHEMY_URL = os.environ.get('SQLALCHEMY_URL', 'postgresql://anyway:anyway@localhost:9876/anyway')
