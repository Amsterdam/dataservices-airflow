import pathlib

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import PostgresCheckOperator

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from common.sql import (
    SQL_TABLE_RENAME,
    SQL_CHECK_COUNT,
    SQL_CHECK_GEO,
)

from common.dataschema import get_column_names_for_ogr2ogr_sql_select


def quote(instr):
    return f"'{instr}'"


dag_id = "winkelgebieden"
variables = Variable.get(dag_id, deserialize_json=True)
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"

dataschema = variables["data_schema"]
source_filename = variables["source_filename"]
source_missing_cols = variables["source_missing_columns"]
skip_cols = variables["skip_columns"]

tmp_dir = f"/tmp/{dag_id}"
tmp_column_file = f"{tmp_dir}/alias_columns.file"

with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
) as dag:

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    get_schema_columns = PythonOperator(
        task_id="get_schema_columns",
        python_callable=get_column_names_for_ogr2ogr_sql_select,
        op_args=[
            f"{dataschema}",
            f"{tmp_column_file}",
            f"{source_filename}",
            f"{source_missing_cols}",
            f"{skip_cols}",
        ],
    )

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 -nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {data_path}/{dag_id}/winkgeb2018.TAB "
        # the option -lco is added to rename the automated creation of the primairy key column (ogc fid) - due to use of ogr2ogr
        f"-sql @{tmp_column_file} -lco FID=ID -lco GEOMETRY_NAME=geometry",
    )

    convert_data = BashOperator(
        task_id="convert_data",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.utf8.sql",
    )

    add_category = BashOperator(
        task_id="add_category",
        bash_command=f"psql {pg_params()} < {sql_path}/add_categorie.sql",
    )

    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(tablename=f"{dag_id}_{dag_id}_new", mincount=75),
    )

    check_geo = PostgresCheckOperator(
        task_id="check_geo",
        sql=SQL_CHECK_GEO,
        params=dict(
            tablename=f"{dag_id}_{dag_id}_new",
            geotype=["ST_Polygon", "ST_MultiPolygon"],
            geo_column="geometry",
            check_valid=False,
        ),
    )

    rename_table = PostgresOperator(
        task_id="rename_table",
        sql=SQL_TABLE_RENAME,
        params=dict(tablename=f"{dag_id}_{dag_id}", geo_column="geometry",),
    )


(
    slack_at_start
    >> mkdir
    >> get_schema_columns
    >> extract_data
    >> convert_data
    >> create_table
    >> add_category
    >> [check_count, check_geo]
    >> rename_table
)

dag.doc_md = """
    #### DAG summery
    This DAG containts shopping area's (winkelgebieden)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/winkelgebieden/winkelgebieden/
"""
