import operator
from airflow import DAG

from swift_operator import SwiftOperator

# from airflow.operators.postgres_operator import PostgresOperator

# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    COLNAMES_CHECK,
    GEO_CHECK,
)

# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator

# from airflow.operators.dummy_operator import DummyOperator

from common import default_args
from check_helpers import make_params

# from common import pg_params

# from airflow.operators.docker_operator import DockerOperator


def create_error(*args, **kwargs):
    raise Exception


with DAG("testdag", default_args=default_args,) as dag:

    swift_task = SwiftOperator(
        task_id="swift_task",
        container="Dataservices",
        object_id="beschermde_stads_en_dorpsgezichten/acceptance/beschermde_stadsdorpsgezichten.zip",
        output_path="/tmp/bsd.zip",
        # container="afval",
        # object_id="acceptance/afval_cluster.zip",
        # output_path="/tmp/blaat/out2.zip",
        # conn_id="afval",
        swift_conn_id="objectstore_dataservices",
    )

    count_check = COUNT_CHECK.make_check(
        check_id="count_check",
        pass_value=1587,
        params=dict(table_name="fietspaaltjes"),
        result_checker=operator.ge,
    )

    colname_check = COLNAMES_CHECK.make_check(
        check_id="colname_check",
        parameters=["fietspaaltjes"],
        pass_value=set(["id"]),
        result_checker=operator.ge,
    )

    geo_check = GEO_CHECK.make_check(
        check_id="geo_check",
        params=dict(table_name="fietspaaltjes", geotype="POINT"),
        pass_value=1,
    )

    checks = [count_check, colname_check, geo_check]
    multi = PostgresMultiCheckOperator(
        task_id="multi", checks=checks, params=make_params(checks)
    )

    # swift_task
    # sqls = [
    #     "delete from biz_data where biz_id = {{ params.tba }}",
    #     "insert into biz_data (biz_id, naam) values (123456789, 'testje')",
    # ]
    # pgtest = PostgresOperator(task_id="pgtest", sql=sqls)

    # bashtest = BashOperator(
    #     task_id="bashtest", bash_command=f"psql {pg_params} < /tmp/doit.sql",
    # )

    # failing_task = PythonOperator(
    #     task_id="failing_task", python_callable=create_error, provide_context=True,
    # )


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
