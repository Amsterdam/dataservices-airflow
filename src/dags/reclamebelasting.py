import operator
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.reclamebelasting import SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

env: object = Env()


dag_id: str = "reclamebelasting"
schema_name: str = "belastingen"
table_name: str = "reclame"
variables: dict = Variable.get(dag_id, deserialize_json=True)
files_to_download: dict = variables["files_to_download"]
zip_file: str = files_to_download["zip_file"]
shp_file: str = files_to_download["shp_file"]
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
db_conn: object = DatabaseEngine()
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    dag_id,
    default_args=default_args,
    description="""Reclamebelastingjaartarieven per belastinggebied voor (reclame)uitingen
    met oppervlakte >= 0,25 m2 en > 10 weken in een jaar zichtbaar zijn.""",
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id="belastingen"),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = SwiftOperator(
        task_id=f"download_{zip_file}",
        swift_conn_id="SWIFT_DEFAULT",
        container="reclame",
        object_id=zip_file,
        output_path=f"{tmp_dir}/{zip_file}",
    )

    # 4. Extract zip file
    extract_zip = BashOperator(
        task_id="extract_zip",
        bash_command=f"unzip -o {tmp_dir}/{zip_file} -d {tmp_dir}",
    )

    # 5. Load data
    load_data = Ogr2OgrOperator(
        task_id=f"import_{shp_file}",
        target_table_name=f"{schema_name}_{table_name}_new",
        input_file=f"{tmp_dir}/{shp_file}",
        s_srs=None,
        t_srs="EPSG:28992",
        auto_detect_type="YES",
        geometry_name="geometrie",
        mode="PostgreSQL",
        fid="id",
        db_conn=db_conn,
    )

    # 6. RENAME columns based on PROVENANCE
    provenance_trans = ProvenanceRenameOperator(
        task_id="provenance_rename",
        dataset_name=schema_name,
        prefix_table_name=f"{schema_name}_",
        postfix_table_name="_new",
        subset_tables=["".join(table_name)],
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    total_checks.clear()
    count_checks.clear()

    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=1,
            params={"table_name": f"{schema_name}_{table_name}_new"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{schema_name}_{table_name}_new",
                "geotype": [
                    "POLYGON",
                    "MULTIPOLYGON",
                ],
            },
            pass_value=1,
        )
    )

    check_name[dag_id] = count_checks

    # 7. Execute bundled checks on database (in this case just a count check)
    multi_checks = PostgresMultiCheckOperator(task_id="count_check", checks=check_name[dag_id])

    # 8. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table_based_upon_schema",
        data_schema_name=schema_name,
        data_table_name=f"{schema_name}_{table_name}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 9. Check for changes to merge in target table
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name=schema_name,
        source_table_name=f"{schema_name}_{table_name}_new",
        target_table_name=f"{schema_name}_{table_name}",
        drop_target_if_unequal=True,
    )

    # 10. Clean up (remove temp table _new)
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{schema_name}_{table_name}_new"},
    )

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> extract_zip
    >> load_data
    >> provenance_trans
    >> multi_checks
    >> create_table
    >> change_data_capture
    >> clean_up
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains advertising tax areas and rates.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/belastingen/reclame.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=belastingen/reclame&x=106434&y=488995&radius=10
"""
