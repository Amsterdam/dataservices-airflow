from datetime import timedelta
from airflow import DAG

from swift_operator import SwiftOperator

# from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "dataservices",
    "depends_on_past": False,
    "start_date": "2020-04-01",
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "catchup": False,  # do not backfill
}

with DAG("testdag", default_args=default_args,) as dag:

    swift_task = SwiftOperator(
        task_id="swift_task",
        container="afval",
        object_id="acceptance/afval_cluster.zip",
        output_path="/tmp/blaat/out2.zip",
    )

swift_task

# This needs a working connection object
# and volume connection to the docker socket
# docker_task = DockerOperator(
#     task_id="docker_command",
#     docker_conn_id="docker",
#     image="dcatd:production",
#     api_version="auto",
#     auto_remove=True,
#     command="/bin/sleep 30",
#     # docker_url="unix://var/run/docker.sock",
#     network_mode="bridge",
# )

# docker-registry.data.amsterdam.nl/datapunt/dcatd:production
