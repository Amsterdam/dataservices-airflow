# Airflow for Dataservices

Airflow setup for Dataservices, running in a Docker container.

# Requirements

    Python >= 3.9

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -U wheel pip
    cd src
    make install  # installs src/requirements_dev.txt

    cd ..
    docker-compose build
    docker-compose up -d database

The docker-compose.yml uses some variables that need to be available in a `.env` file
next to the docker-compose.yaml file in the following format:

    VARIABLE_NAME=value-of-the-variable

The `FERNET_KEY` variable is used to encrypt secrets that are store in the PostgreSQL db.
The `SLACK_WEBHOOK` is used to adress the Slack API.

Furthermore, connections are provided as environment variables with a specific name,
as requested by Airflow. E.g `AIRFLOW_CONN_POSTGRES_DEFAULT` provides the DSN for
the default postgresql database. There are also `AIRFLOW_CONN_OBJECTSTORE_[name-of-objectstore]`
variable for connections with the objectstore. The `s3` dsn format is being used
for objectstore connections. The `host` field in the Airflow connection is
used for the clientId that is needed for the Objectstore.

# Run airflow

    docker-compose up airflow

Airflow is running as two separate processes, a webserver with the Airflow UI and
a scheduler. The script `scripts/run.sh` is preparing some Airflow configuration
(variables and connections) and then spawn the webserver and scheduler process using
supervisor.

The airflow webserver is running at http://localhost:8080/
The webserver UI is protected with a password, admin user is created automatically during startup and can be used right away:

    Username: `admin`
    Password: `admin`

Extra users can be created using airflow shell:

    1. log in the airflow container: docker exec -it airflow bash
    2. airflow users create -r User -u <username> -e <email> -f <first_name> -l <last_name> -p <password>

Dags can only be seen in the UI by the owner, or by the admin. The default owner is dataservices.
In order to see the dataservices dags you have to use admin or the dataservices user.

# Managing requirements.txt

This project uses [pip-tools](https://pypi.org/project/pip-tools/)
and [pur](https://pypi.org/project/pur/) to manage the `requirements.txt` file.

To add a Python dependency to the project:

* Add the dependency to `requirements.in`
* Run `make requirements`

To upgrade the project, run:

    make upgrade
    make install


# Airflow directory layout

The Airflow configuration file is in `src/config/airflow.cfg`.

The airflow DAGs (Directed Acyclic Graphs) are in the `src/dags` directory.
This directory also contains common python modules or DAG-specific import scripts and SQL files.

Dataservices-specific Airflow operators are in `src/plugins`.

# Setting up a new DAG

First create a python module in `src/dags`. Then stich together the different steps
in the data pipeline using Airflow Operators or Dataservices specific operator.

Make sure the Operators are connected into a DAG (using the `>>` Airflow syntax).

Airflow polls the `src/dags` directory at regular intervals, so it will pick up the new DAG.
In the Airflow web UI, the new DAG can be activated using (On/Off) switch in column 2 of DAG overview.

In the tree view of a DAG the status of the Operator steps are visible and the logs can be checked.

# Testing and debugging

Sometimes it can be convenient to test the Operator steps separately. That can be done using
the Airflow command-line. Log in to the container:

    docker-compose exec airflow bash

Inside the container:

    airflow test <dag_id> <task_id> 2020-01-01   # Just 2020 also works for a date

The output is sent to stdout. These test runs are not visible in the web UI.
When the PythonOperator is used, it is even possible to use the python pdb debugger.

# Updating of DAGs

When the Airflow container is re-deployed, all DAGs are included in the Docker container.
However, to update the DAGs it is not needed to re-deploy the full Airflow container.
There is one special DAG called `update_dags` that periodically pulls the latest DAGs
from the github repo into the container.


# Variables

Configuration that is needed for the DAGs can be stored in variables. These variable can be
changed through the web UI, however, that is only relevant for testing.

Variables are imported from a json file `vars/vars.json` during startup of the container. These json file
is generated from a yaml file in `vars/vars.yaml`.

In a running containers, the following commands can update the variables:

    python scripts/mkvars.py
    airflow variables import vars/vars.json

# Connections

Airflow maintains a list of connections (DB connections, http connections etc.).
These connections can be maintained through the web UI.
During startup a set of connections is created/updated (see `scripts/run.sh`)
The preferred way to add new connections is by using environment variables of the
format `AIRFLOW_CONN_[conn-type]_[conn_id]` (see above). These environment variable
can be define in the `docker-compose.yml` config locally and in the ansible provisioning
configuration for the acceptance and production server.

# Accessing a remote datasource from within a docker container
https://git.data.amsterdam.nl/Datapunt/dataservicesutilities/blob/master/docker/README.md

# Caveats

Airflow is running DAG python code frequently (default twice every minute).
So, the heavy lifting should occur only inside the operator code. Some of the
operator parameters can be parametrized using jinja2. These templated variables
are lazily evaluated when de DAG is run. This parametrization can also be used to
postpone the heavy lifting. For the same reason it is better to not use variables
outside of the operators, because access to variables means access to the postgres database.

# PyCharm

PyCharm has a great source code formatter that is extremely fast.
But not everyone uses PyCharm.
To ensure Python code is formatted consistently,
regardless of the development environment used,
we use `black` and `isort`.
PyCharm can easily be configured to run these those external programs.
First create a shell script `fmt_code.sh`:


```sh
#!/usr/bin/env sh

PY_INTERPRETER_PATH=$1
FILE_PATH=$2

${PY_INTERPRETER_PATH} -m black ${FILE_PATH}
${PY_INTERPRETER_PATH} -m isort ${FILE_PATH}
```

There are two options to use this script from within PyCharm:

- Run it as an external tool with a keyboard shortcut assigned to it
- Configure a file watcher to have it run automatically on file save

Configuring it as an external tool is detailed below.
Configuring as a file watcher should be very similar.

Go to:

- `File | Settings | Tools | External Tools`
- Click on the `+` icon
- Fill out the fields:
    - Name: `Black + isort`
    - Program: `$ProjectFileDir$/fmt_code.sh`
    - Arguments: `$JDKPath$ $FilePath$`
    - Output paths to refresh: `$FilePath$`
    - Working directory: `$ProjectFileDir$`
    - Untick option *Open console for tool output*
    - Click `OK`  (Edit Tool dialog)
    - Click `Apply` (Settings dialog)
- Still in the Setting dialog, go to `Keymap`
- In search field type: `Black + isort`
- Right click on the entry found and select `Add keyboard shortcut`
- Press `Ctrl + Alt + L`  (or whatever you deem convenient)
- Click `OK` (Keyboard Shortcut dialog)
- Click `OK` (Settings dialog)

If you regularly reformat the Python module under development using `Ctrl + Alt + L`,
the Git pre-commit hook will notcomplain about the layout of your code.
