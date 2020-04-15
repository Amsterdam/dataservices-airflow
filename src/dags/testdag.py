from airflow import DAG

# from swift_operator import SwiftOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# from airflow.operators.dummy_operator import DummyOperator

from common import default_args
from common import pg_params

# from airflow.operators.docker_operator import DockerOperator


def create_error(*args, **kwargs):
    raise Exception


with DAG("testdag", default_args=default_args,) as dag:

    # swift_task = SwiftOperator(
    #     task_id="swift_task",
    #     container="afval",
    #     object_id="acceptance/afval_cluster.zip",
    #     output_path="/tmp/blaat/out2.zip",
    # )

    # swift_task
    sqls = [
        "delete from biz_data where biz_id = 123456789",
        "insert into biz_data (biz_id, naam) values (123456789, 'testje')",
    ]
    pgtest = PostgresOperator(task_id="pgtest", sql=sqls)

    bashtest = BashOperator(
        task_id="bashtest", bash_command=f"psql {pg_params} < /tmp/doit.sql",
    )

    failing_task = PythonOperator(
        task_id="failing_task", python_callable=create_error, provide_context=True,
    )


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
