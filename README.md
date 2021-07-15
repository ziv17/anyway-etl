# Anyway ETL

A Python library and CLI which handles ETL processing tasks for the Anyway project. 


## Local development

Prerequisites

* [Python 3.8 or higher](https://www.python.org/downloads/)

Create a virtualenv
(The Python binary name might be just Python, depending on how you installed it,
just make sure you use Python 3.8 or higher)

```
python3.8 -m venv venv
```

Activate the virtualenv

```
. venv/bin/activate
```

Upgrade pip

```
pip install --upgrade pip
```

Install the requirements and the anyway-etl module:

```
pip install -r requirements.txt
pip install -e .
```

See the CLI help message for available commands:

```
anyway-etl --help
```

For commands which require a DB, run the following from the Anyway repository:

```
docker-compose up -d db
```
