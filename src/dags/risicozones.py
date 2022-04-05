import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import DatabaseEngine
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_risicozones import (
    cleanse_misformed_data_iter,
    merge_files_iter,
    unify_geometry_data_iter,
    union_files_iter,
)
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.risicozones import SET_GEOM, SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

dag_id = "risicozones"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
files_to_download = variables["files_to_download"]
files_to_merge = variables["files_to_merge"]
files_to_union = variables["files_to_union"]
files_to_cleanse = variables["files_to_cleanse"]
files_to_fix_geom = variables["files_to_fix_geom"]
db_conn = DatabaseEngine()
total_checks: list[int] = []
count_checks: list[int] = []
geo_checks: list[int] = []
check_name: dict[str, list[int]] = {}


with DAG(
    dag_id,
    description="risicozones",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. create download temp directory to store the data
    mk_tmp_dir = BashOperator(task_id="mk_tmp_dir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file.split('.')[0]}",
            swift_conn_id="OBJECTSTORE_MILIEUTHEMAS",
            container="Milieuthemas",
            object_id=file,
            output_path=f"{tmp_dir}/{file}",
        )
        for key, files in files_to_download.items()
        for file in files
    ]

    # 4. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Merge data
    # data from different files are merged together (in some cases)
    merge_data = [
        PythonOperator(
            task_id=f"merge_{subject}",
            python_callable=merge_files_iter,
            op_kwargs={
                "target_file": f"{tmp_dir}/{params['target_file']}",
                "source_file": f"{tmp_dir}/{params['source_file']}",
                "mutual_key": params["mutual_key"],
                "map_source_field_to_target": params["map_source_field_to_target"],
                "source_filter": params.get("source_filter", None),
                "target_filter": params.get("target_filter", None),
            },
        )
        for subject, params in files_to_merge.items()
    ]

    # 6. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 7. Union data
    # data from different files are unioned together (in some cases)
    union_data = [
        PythonOperator(
            task_id=f"union_{subject}",
            python_callable=union_files_iter,
            op_kwargs={
                "target_file": f"{tmp_dir}/{params['target_file']}",
                "source_file": params["source_file"],
                "source_file_content_type": params["source_file_content_type"],
                "source_file_content_column": params["source_file_content_column"],
                "source_file_dir_path": tmp_dir,
                "row_unique_cols": params.get("row_unique_cols", None),
            },
        )
        for subject, params in files_to_union.items()
    ]

    # 8. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface3 = DummyOperator(task_id="interface3")

    # 9. Cleanse data
    # One source 'Bronnen' is malformed: id is not unique and there are empty rows
    cleanse_data = [
        PythonOperator(
            task_id=f"cleanse_{subject}",
            python_callable=cleanse_misformed_data_iter,
            op_kwargs={
                "source_file": f"{tmp_dir}/{params['source_file']}",
                "row_unique_cols": params["row_unique_cols"],
                "extra_cols": params.get("extra_cols", None),
            },
        )
        for subject, params in files_to_cleanse.items()
    ]

    # 10. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface4 = DummyOperator(task_id="interface4")

    # 11. Fix geom
    # Files can contain different geometry types.
    # To proces the geometry to the geometry type in the DB the geometries must be equalized.
    fix_geometry = [
        PythonOperator(
            task_id=f"fix_geom_{subject}",
            python_callable=unify_geometry_data_iter,
            op_kwargs={
                "source_file": f"{tmp_dir}/{params['source_file']}",
                "geom_data_type_to_use": params["geom_data_type_to_use"],
            },
        )
        for subject, params in files_to_fix_geom.items()
    ]

    # 12. Dummy operator acts as an interface between parallel tasks
    # to another parallel tasks with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface5 = DummyOperator(task_id="interface5")

    # 13. Transform seperator from pipeline to semicolon
    # and set code schema to UTF-8
    change_seperator = [
        BashOperator(
            task_id=f"change_seperator_{key}",
            bash_command=f"cat {tmp_dir}/{file} | sed 's/|/;/g' > {tmp_dir}/seperator_{file} ;"
            f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/seperator_{file} > "
            f"{tmp_dir}/utf-8_{file}",
        )
        for key, files in files_to_download.items()
        for file in files
        if file == files[0]
    ]

    # 14. Import data into DB with ogr2ogr
    # It is not possible to use own id column for ID.
    # To still use a own identifier column the fid is set to fid instead of id.
    to_sql = [
        Ogr2OgrOperator(
            task_id=f"import_{key}",
            target_table_name=f"{dag_id}_{key}_new",
            input_file=f"{tmp_dir}/utf-8_{file}",
            s_srs="EPSG:28992",
            t_srs="EPSG:28992",
            geometry_name="geometrie",
            auto_detect_type="YES",
            fid="id",
            mode="PostgreSQL",
            db_conn=db_conn,
        )
        for key, files in files_to_download.items()
        for file in files
        if file == files[0]
    ]

    # 15. RE-define GEOM type (because ogr2ogr cannot set geom with any .csv source file)
    redefine_geoms = [
        PostgresOperator(
            task_id=f"set_geomtype_{key}",
            sql=SET_GEOM,
            params={"tablename": f"{dag_id}_{key}_new"},
        )
        for key, files in files_to_download.items()
        for file in files
        if file == files[0]
    ]

    # 16. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_table_{key}",
            data_schema_name=dag_id,
            data_table_name=f"{dag_id}_{key}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for key, files in files_to_download.items()
        for file in files
        if file == files[0]
    ]

    # 17. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # Prepare the checks and added them per source to a dictionary
    for table_name in files_to_download.keys():

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{table_name}",
                pass_value=1,
                params={"table_name": f"{dag_id}_{table_name}_new"},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{table_name}",
                params={
                    "table_name": f"{dag_id}_{table_name}_new",
                    "geotype": ["POLYGON", "MULTIPOLYGON", "MULTILINESTRING", "POINT"],
                    "geo_column": "geometrie",
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[table_name] = total_checks

    # 18. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{table_name}", checks=check_name[table_name]
        )
        for table_name in files_to_download.keys()
    ]

    # 19. Check for changes to merge in target table by using CDC
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{table_name}",
            dataset_name=dag_id,
            source_table_name=f"{dag_id}_{table_name}_new",
            target_table_name=f"{dag_id}_{table_name}",
        )
        for table_name in files_to_download.keys()
    ]

    # 20. Clean up; delete temp table
    clean_up = [
        PostgresOperator(
            task_id=f"clean_up_{table_name}",
            sql=SQL_DROP_TMP_TABLE,
            params={"tablename": f"{dag_id}_{table_name}_new"},
        )
        for table_name in files_to_download.keys()
    ]

    # 21. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


# FLOW. define flow with parallel executing of serial tasks for each file
slack_at_start >> mk_tmp_dir >> download_data

for download in zip(download_data):

    download >> Interface

Interface >> merge_data >> Interface2

Interface2 >> union_data

for stack_data in zip(union_data):

    stack_data >> Interface3

Interface3 >> cleanse_data

for cleanse_file in zip(cleanse_data):

    cleanse_file >> Interface4

Interface4 >> fix_geometry

for fix_geom in zip(fix_geometry):

    fix_geom >> Interface5

Interface5 >> change_seperator

for change_seperator, to_sql, set_geom, create_table in zip(
    change_seperator, to_sql, redefine_geoms, create_tables
):

    op_chain = [change_seperator >> to_sql >> set_geom >> create_table]  # type: ignore[operator]
    op_chain >> provenance_translation

provenance_translation >> multi_checks

for data_check, detect_changes, del_tmp_table in zip(multi_checks, change_data_capture, clean_up):

    [data_check >> detect_changes >> del_tmp_table]

clean_up >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains data about risk area's due to potential hazards objects in that area.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/risicogebieden.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/risicogebieden.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=risicogebieden/lpgtank&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=risicogebieden/lpgafleverzuil&x=106434&y=488995&radius=10
    https://api.data.amsterdam.nl/geosearch?datasets=risicogebieden/aardgasgebied&x=106434&y=488995&radius=10
"""
