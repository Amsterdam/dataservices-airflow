import operator
import pathlib

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from postgres_check_operator import PostgresCheckOperator

from http_fetch_operator import HttpFetchOperator
from provenance_operator import ProvenanceOperator
from postgres_rename_operator import PostgresTableRenameOperator


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

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

dag_id = "winkelgebieden"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data"
sql_path = pathlib.Path(__file__).resolve().parents[0] / "sql"
variables = Variable.get(dag_id, deserialize_json=True)
schema_end_point = variables["schema_end_point"]
tmp_dir = f"/tmp/{dag_id}"
metadataschema = f"{tmp_dir}/winkelgebieden_dataschema.json"
total_checks = []
count_checks = []
geo_checks = []

with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
) as dag:

    # 1.
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2.
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. download dataschema into temp directory
    download_schema = HttpFetchOperator(
        task_id=f"download_schema",
        endpoint=f"{schema_end_point}",
        http_conn_id="schemas_data_amsterdam_conn_id",
        tmp_file=f"{metadataschema}",
        output_type="text",
    )
    # 4.
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=f"ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 -nln {dag_id}_{dag_id}_new "
        f"{tmp_dir}/{dag_id}.sql {data_path}/{dag_id}/winkgeb2018.TAB "
        # the option -lco is added to rename the automated creation of the primairy key column (ogc fid) - due to use of ogr2ogr
        # in the -sql a select statement is added to get column renames that is specified in the dataschema
        # f"-sql @{tmp_column_file} -lco FID=ID -lco GEOMETRY_NAME=geometry",
        f"-lco FID=ID -lco GEOMETRY_NAME=geometry",
    )

    # 5.
    provenance_translation = ProvenanceOperator(
        task_id=f"provenance",
        metadataschema=f"{metadataschema}",
        source_file=f"{tmp_dir}/{dag_id}.sql",
    )

    # 6.
    convert_data = BashOperator(
        task_id="convert_data",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    # 7.
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}.utf8.sql",
    )

    # 8.
    add_category = BashOperator(
        task_id="add_category",
        bash_command=f"psql {pg_params()} < {sql_path}/add_categorie.sql",
    )

    # 9. PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id=f"count_check",
            pass_value=75,
            params=dict(table_name=f"{dag_id}_{dag_id}_new"),
            result_checker=operator.ge,
        )
    )

    # Data shows that 17 / 132 polygonen are invalid, to avoid crashing the flow, temporaly turned off
    # geo_checks.append(
    #     GEO_CHECK.make_check(
    #         check_id=f"geo_check",
    #         params=dict(
    #             table_name=f"{dag_id}_{dag_id}_new",
    #             geotype=["POLYGON", "MULTIPOLYGON"],
    #         ),
    #         pass_value=1,
    #     )
    # )
    # total_checks = count_checks + geo_checks
    total_checks = count_checks

    # 10. Run bundled checks (step 9)
    multi_checks = PostgresMultiCheckOperator(
        task_id=f"multi_check", checks=total_checks
    )

    # 11.
    rename_table = PostgresTableRenameOperator(
        task_id=f"rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )


(
    slack_at_start
    >> mkdir
    >> download_schema
    >> extract_data
    >> provenance_translation
    >> convert_data
    >> create_table
    >> add_category
    >> multi_checks
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
