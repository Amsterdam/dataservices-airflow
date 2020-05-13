from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import PostgresCheckOperator
from http_fetch_operator import HttpFetchOperator

from common import (
    vsd_default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
)

from importscripts.import_openbare_verlichting import import_openbare_verlichting

dag_id = "openbare_verlichting"

with DAG(dag_id, default_args=vsd_default_args, template_searchpath=["/"]) as dag:

    tmp_dir = f"/tmp/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    objects_fetch = HttpFetchOperator(
        task_id="objects_fetch",
        endpoint="{{ var.json.openbare_verlichting.objects_endpoint }}",
        http_conn_id="verlichting_conn_id",
        tmp_file=f"{tmp_dir}/objects-source.json",
    )

    types_fetch = HttpFetchOperator(
        task_id="types_fetch",
        endpoint="{{ var.json.openbare_verlichting.types_endpoint }}",
        http_conn_id="verlichting_conn_id",
        tmp_file=f"{tmp_dir}/objects-types.json",
    )

    import_data = PythonOperator(
        task_id="import_data",
        python_callable=import_openbare_verlichting,
        op_args=[
            f"{tmp_dir}/objects-source.json",
            f"{tmp_dir}/objects-types.json",
            f"{tmp_dir}/objects.geo.json",
        ],
    )

    extract_geojson = BashOperator(
        task_id="extract_geojson",
        bash_command=f"ogr2ogr --config PG_USE_COPY YES -f 'PGDump' "
        f"-nln {dag_id}_new "
        f"{tmp_dir}/objects.sql {tmp_dir}/objects.geo.json",
    )

    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/objects.sql",
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_new", mincount=129410),
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename=f"{dag_id}"),
    )

(
    slack_at_start
    >> objects_fetch
    >> types_fetch
    >> import_data
    >> extract_geojson
    >> create_table
    >> check_count
    >> rename_table
)
