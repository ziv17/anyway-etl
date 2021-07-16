# Airflow local development

This document described how to run the airflow server locally for development.
For most use-cases it's not required, and you should instead use the
Docker Compose environment which will be easier to set up.

## Install

Prerequisites:

* [Setup anyway-etl for local development](ANYWAY_ETL_LOCAL_DEVELOPMENT.md)
* [Install Airflow system dependencies](https://airflow.apache.org/docs/apache-airflow/stable/installation.html#system-dependencies)

Create and install airflow virtualenv and dependencies

```
python3.8 -m venv venv/airflow &&\
. venv/airflow/bin/activate &&\
pip install --upgrade pip &&\
airflow_server/pip_install_airflow.sh &&\
pip install -e airflow_server
```

Create a file `.airflow.env` with the following contents:

```
. venv/airflow/bin/activate
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/airflow_server/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
```

Initialize the Airflow DB and create an admin user:

```
. .airflow.env &&\
airflow db init &&\
airflow users create --username admin --firstname Admin --lastname Adminski \
    --role Admin --password 12345678 --email admin@localhost
```

## Use

Start the Airflow web server:

```
. .airflow.env && airflow webserver --port 8080
```

In a new terminal, start the Airflow scheduler:

```
. .airflow.env && airflow scheduler
```

Access the airflow webserver at http://localhost:8080 login using admin / 12345678

Add any required env vars to `.airflow.env` and restart the scheduler.

Start the anyway DB if needed, see the Anyway-etl local development doc for details.
