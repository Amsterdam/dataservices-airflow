import operator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator
from ogr2ogr_operator import Ogr2OgrOperator
from provenance_rename_operator import ProvenanceRenameOperator
from airflow.operators.postgres_operator import PostgresOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator


from common.db import DatabaseEngine

from common import (
    default_args,
    pg_params,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)


dag_id = "historische_onderzoeken"
dataset_name="historischeonderzoeken"
variables = Variable.get(f"{dag_id}", deserialize_json=True)
files_to_download = variables["files_to_download"]
db_conn = DatabaseEngine()
tmp_dir = f"/tmp/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"

SQL_DROP_UNNECESSARY_COLUMNS_TMP_TABLE = """
    ALTER TABLE {{ params.tablename }}
    DROP COLUMN IF EXISTS dateringtot,
    DROP COLUMN IF EXISTS dateringvan,
    DROP COLUMN IF EXISTS bestandsnaam,
    DROP COLUMN IF EXISTS openbaarna,
    DROP COLUMN IF EXISTS "datum toegevoegd",
    DROP COLUMN IF EXISTS "datum rapport",
    DROP COLUMN IF EXISTS nummer,
    DROP COLUMN IF EXISTS "opmerkingen",
    DROP COLUMN IF EXISTS fid;
"""

SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

with DAG(
    dag_id,
    description="uitgevoerde onderzoeken per locatie, bijv. Archeologische verwachtingen (A), Bodemkwaliteit (B), Conventionele explosieven (C) kademuren Dateren (D) en Ondergrondse Obstakels (OO).",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dataset_name} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = SwiftOperator(
            task_id="download",
            swift_conn_id="objectstore_dataservices",
            container="Dataservices",
            object_id=f"historische_onderzoeken/{files_to_download}",
            output_path=f"{tmp_dir}/{files_to_download}",
        )

    # 4. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = SqlAlchemyCreateObjectOperator(
            task_id="create_db_table_based_upon_schema",
            data_schema_name=f"{dataset_name}",
            data_table_name=f"{dataset_name}_{dataset_name}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )

    # 5.create the SQL for creating the table using ORG2OGR PGDump
    GEOJSON_to_DB = Ogr2OgrOperator(
            task_id="import_data",
            target_table_name=f"{dataset_name}_{dataset_name}_new",
            input_file=f"{tmp_dir}/{files_to_download}",
            s_srs=None,
            t_srs="EPSG:28992",
            geometry_name="geometrie",
            ind_sql=False,
            db_conn=db_conn,
        )

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dataset_name}",
        prefix_table_name=f"{dataset_name}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # prepare the checks and added them per source to a dictionary
    total_checks.clear()
    count_checks.clear()
    geo_checks.clear()

    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=10,
            params=dict(table_name=f"{dataset_name}_{dataset_name}_new"),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params=dict(
                table_name=f"{dataset_name}_{dataset_name}_new",
                geotype=["MULTIPOLYGON",],
                geo_column="geometrie",
            ),
                pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks
    check_name["{dataset_name}"] = total_checks

    # 7. Execute bundled checks on database
    multi_checks = PostgresMultiCheckOperator(
            task_id="multi_check", checks=check_name["{dataset_name}"]
        )

    # 8. Drop cols - that do not show up in the API
    drop_unnecessary_cols = PostgresOperator(
            task_id="drop_unnecessary_cols_tmp_table",
            sql=SQL_DROP_UNNECESSARY_COLUMNS_TMP_TABLE,
            params=dict(tablename=f"{dataset_name}_{dataset_name}_new"),
        )

    # 9. Check for changes to merge in target table
    change_data_capture = PgComparatorCDCOperator(
            task_id="change_data_capture",
            source_table=f"{dataset_name}_{dataset_name}_new",
            target_table=f"{dataset_name}_{dataset_name}",
        )

    # 10. Clean up
    clean_up = PostgresOperator(
            task_id="clean_up",
            sql=SQL_DROP_TMP_TABLE,
            params=dict(tablename=f"{dataset_name}_{dataset_name}_new"),
        )

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> create_tables
    >> GEOJSON_to_DB
    >> provenance_translation
    >> multi_checks
    >> drop_unnecessary_cols
    >> change_data_capture
    >> clean_up
)

# Mark down
dag.doc_md = """
    #### DAG summery
    This DAG containts info about conducted research on a specific location.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/onderzoeken.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/onderzoeken.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=onderzoeken/onderzoeken&x=106434&y=488995&radius=10
"""
