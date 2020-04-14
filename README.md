# Airflow for Dataservices

Airflow setup for Dataservices, running in a Docker container.

# Requirements

    Python >= 3.7

    python3 -m venv .venv
    source .venv/bin/activate
    cd src
    make install  # installs src/requirements_dev.txt

    cd ..
    docker-compose build
    docker-compose up -d database

The docker-compose.yml uses some variables that need to be available in a `.env` file
next to the docker-compose.yaml file in the following format:

    VARIABLE_NAME=value-of-the-variabl

The `FERNET_KEY` variable is used to encrypt secrets that are store in the PostgreSQL db.
The `SLACK_WEBHOOK` is used to adress the Slack API.

# Run airflow

    docker-compose up airflow

Airflow is running as two separate processes, a webserver with the Airflow UI and
a scheduler. The script `src/scripts/run.sh` is preparing some Airflow configuration
(variables and connections) and then spawn the webserver and scheduler process using
supervisor.

The airflow webserver is running at http://localhost:8080/
The webserver UI is protected with a password, the associated admin user needs to be created
once inside of the PostgreSQL database using this command:

    docker-compose exec airflow python scripts/mkuser.py <username> <e-mail address user>

This script prompts for a password and stores the credentials in the PostgreSQL database.



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

Airflow polls the `src/dags` directory at regular interval, so it will pick up the new DAG.
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


