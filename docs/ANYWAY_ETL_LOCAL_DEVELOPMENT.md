# anyway-etl local development

This document described how to run the anyway-etl library and CLI locally
for development. For most use-cases it's not required, and instead you should 
use the Docker Compose environment for easier local development.

## Install

Prerequisites:

* [Python 3.8](https://www.python.org/downloads/)
* A clone of [anyway](https://github.com/data-for-change/anyway) repository at `../anyway`

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

Install the anyway module:

```
pip install -e ../anyway
```

## Usage

Activate the virtualenv

```
. venv/bin/activate
```

See the CLI help message for available commands:

```
anyway-etl --help
```

Some commands require additional environment variables, see the error message for details.
You can see all possible configurable values in the config.py files.
You should set the required env vars in a file called `.etl.env` and source it before running commands.

For commands which require a DB, run the following from the Anyway repository:

```
docker-compose up -d db
```
