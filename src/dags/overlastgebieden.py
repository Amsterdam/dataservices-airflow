import operator
from pathlib import Path
from typing import Final, Union, cast

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

DAG_ID: Final = "overlastgebieden"

variables_overlastgebieden: dict[str, Union[list[str], dict[str, str]]] = Variable.get(
    "overlastgebieden", deserialize_json=True
)
files_to_download = cast(list[str], variables_overlastgebieden["files_to_download"])
tables_to_create = cast(dict[str, str], variables_overlastgebieden["tables_to_create"])
# Note: Vuurwerkvrijezones (VVZ) data is temporaly! not processed due to covid19 national measures
tables_to_check: dict[str, str] = {
    k: v for k, v in tables_to_create.items() if k != "vuurwerkvrij"
}
TMP_PATH: Final = Path(SHARED_DIR) / DAG_ID
total_checks: list[int] = []
count_checks: list[int] = []
geo_checks: list[int] = []
check_name: dict[str, list[int]] = {}
db_conn = DatabaseEngine()

with DAG(
    DAG_ID,
    description="""alcohol-, straatartiest-, aanleg- en parkenverbodsgebieden,
        mondmaskerverplichtinggebieden, e.d.""",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(TMP_PATH, clean_if_exists=False)

    # 3. Download data
    download_data = [
        SFTPOperator(
            task_id=f"download_{file}",
            ssh_conn_id="OOV_BRIEVENBUS_GEBIEDEN",
            local_filepath=TMP_PATH / file,
            remote_filepath=file,
            operation="get",
            create_intermediate_dirs=True,
        )
        for file in files_to_download
    ]

    # 4. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Create SQL
    SHP_to_SQL = [
        Ogr2OgrOperator(
            task_id=f"create_SQL_{key}",
            target_table_name=f"{DAG_ID}_{key}_new",
            input_file=TMP_PATH / "OOV_gebieden_totaal.shp",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            auto_detect_type="YES",
            mode="PostgreSQL",
            fid="id",
            db_conn=db_conn,
            geometry_name="geometry",
            promote_to_multi=True,
            sql_statement=f"""
                SELECT *
                  FROM OOV_gebieden_totaal
                 WHERE TYPE = {quote_string(code)}
            """,  # noqa S608
        )
        for key, code in tables_to_create.items()
    ]

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. Revalidate invalid geometry records
    # the source has some invalid records
    # to do: inform the source maintainer
    remove_null_geometry_records = [
        PostgresOperator(
            task_id=f"remove_null_geom_records_{key}",
            sql="""UPDATE {{ params.table_id }}
                    SET geometry = ST_CollectionExtract((st_makevalid(geometry)),3)
                    WHERE ST_IsValid(geometry) is false;
                    COMMIT;""",
            params={"table_id": f"{DAG_ID}_{key}_new"},
        )
        for key in tables_to_create.keys()
    ]

    # Prepare the checks and added them per source to a dictionary
    for key in tables_to_check.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{key}",
                pass_value=2,
                params={"table_name": f"{DAG_ID}_{key}_new"},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{key}",
                params={
                    "table_name": f"{DAG_ID}_{key}_new",
                    "geotype": ["MULTIPOLYGON"],
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[key] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{key}", checks=check_name[key])
        for key in tables_to_check.keys()
    ]

    # 9. Dummy operator acts as an interface between parallel tasks to another parallel
    #     tasks with different number of lanes
    # (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 10. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{key}",
            old_table_name=f"{DAG_ID}_{key}_new",
            new_table_name=f"{DAG_ID}_{key}",
        )
        for key in tables_to_create.keys()
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

slack_at_start >> mkdir >> download_data

for data in zip(download_data):

    data >> Interface

Interface >> SHP_to_SQL

for (create_SQL, remove_null_geometry_record,) in zip(
    SHP_to_SQL,
    remove_null_geometry_records,
):

    [create_SQL >> remove_null_geometry_record] >> provenance_translation

provenance_translation >> multi_checks >> Interface2 >> rename_tables

rename_tables >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG contains data about nuisance areas (overlastgebieden) i.e. vuurwerkvrijezones,
    dealeroverlastgebieden, barbecueverbodgebiedeb, etc.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/overlastgebieden.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/overlastgebieden.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=overlastgebieden/vuurwerkvrij&x=106434&y=488995&radius=10
"""
