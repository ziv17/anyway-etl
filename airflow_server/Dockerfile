# Pulled August 8, 2021
FROM python:3.8@sha256:caa7d8d6bfaa181f30c5a5074b81b6963246615f0140dca1d86e1e98efa99dc6
RUN apt-get update && apt-get install -y --no-install-recommends freetds-bin \
        ldap-utils libffi6 libsasl2-2 libsasl2-modules libssl1.1 \
        locales lsb-release sasl2-bin sqlite3 unixodbc brotli jq
RUN wget https://download.docker.com/linux/static/stable/x86_64/docker-25.0.1.tgz &&\
    tar xzf docker-25.0.1.tgz &&\
    mv docker/docker /usr/local/bin/ &&\
    rm -rf docker docker-25.0.1.tgz
WORKDIR /srv
COPY pip_install_airflow.sh ./pip_install_airflow.sh
RUN pip install --upgrade pip && ./pip_install_airflow.sh
RUN python3.8 -m venv /usr/local/lib/anyway-etl &&\
    /usr/local/lib/anyway-etl/bin/pip install --upgrade pip
COPY pip_install_deps.sh ./
COPY dags/ dags/
COPY anyway_etl_airflow/ anyway_etl_airflow/
COPY setup.py ./
RUN pip install -e .

ENV AIRFLOW_HOME=/var/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/srv/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql://postgres:123456@airflow-db
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__PARALLELISM=4
ENV AIRFLOW__CORE__DAG_CONCURRENCY=1
ENV AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE=False
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
ENV ANYWAY_ETL_VENV=/usr/local/lib/anyway-etl
ENV ANYWAY_ETL_AIRFLOW_PIP_INSTALL_DEPS=yes
ENV ANYWAY_ETL_DATA_ROOT_PATH=/var/anyway-etl-data
ENV SQLALCHEMY_URL=postgresql://anyway:anyway@db/anyway

COPY entrypoint.sh ./
ENTRYPOINT ["/srv/entrypoint.sh"]
