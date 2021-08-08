from airflow.operators.bash import BashOperator

from anyway_etl_airflow.config import ANYWAY_ETL_VENV, ANYWAY_ETL_AIRFLOW_PIP_INSTALL_DEPS, ANYWAY_AIRFLOW_SERVER_ROOTDIR


def get_pip_install_deps():
    if not ANYWAY_ETL_AIRFLOW_PIP_INSTALL_DEPS:
        return ''
    return '{}/pip_install_deps.sh && '.format(ANYWAY_AIRFLOW_SERVER_ROOTDIR)


def get_print_dag_run():
    return 'cat << EOF\n{{ dag_run.conf | tojson }}\nEOF\n'


class CliBashOperator(BashOperator):

    def __init__(self, cmd, **kwargs):
        assert not kwargs.get('bash_command')
        kwargs['bash_command'] = '{print_dag_run}{pip_install_deps}{ANYWAY_ETL_VENV}/bin/{cmd}'.format(
            ANYWAY_ETL_VENV=ANYWAY_ETL_VENV,
            cmd=cmd,
            print_dag_run=get_print_dag_run(),
            pip_install_deps=get_pip_install_deps()
        )
        super(CliBashOperator, self).__init__(**kwargs)
