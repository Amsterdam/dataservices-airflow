import operator
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_bekendmakingen import import_data_batch
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

DAG_ID: Final = "bekendmakingen"
TABLE_ID: Final = f"{DAG_ID}_{DAG_ID}"
TMP_DIR: Final = f"{SHARED_DIR}/{DAG_ID}"
TMP_TABLE_POSTFIX: Final = "_new"

variables = Variable.get(DAG_ID, deserialize_json=True)
total_checks = []
count_checks = []
geo_checks = []

with DAG(
    DAG_ID,
    description="bekendmakingen en kennisgevingen from overheid.nl",
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DAG_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(TMP_DIR))

    # 3. Create TARGET table based on Amsterdam Schema (if not present)
    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema",
        data_schema_name=DAG_ID,
        ind_extra_index=True,
    )

    # 4. Create TEMP tables in database based on TARGET
    create_temp_table = PostgresTableCopyOperator(
        task_id="create_temp_table",
        dataset_name=DAG_ID,
        source_table_name=TABLE_ID,
        target_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        # truncate TEMP table if exists and copy table definitions. Don't do anything else.
        truncate_target=True,
        copy_data=False,
        drop_source=False,
    )

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name=TMP_TABLE_POSTFIX,
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. Import source data into TEMP table
    import_data = PythonOperator(
        task_id="import_data",
        python_callable=import_data_batch,
        op_kwargs={"tablename": f"{TABLE_ID}{TMP_TABLE_POSTFIX}"},
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=50,
            params={"table_name": f"{TABLE_ID}{TMP_TABLE_POSTFIX}"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            pass_value=1,
            params={
                "table_name": f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
                "geotype": ["POINT", "MULTIPOINT"],
            },
        )
    )

    total_checks = count_checks + geo_checks

    # 7. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 8. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{TABLE_ID}{TMP_TABLE_POSTFIX}",
        new_table_name=TABLE_ID,
    )

    # 9. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

(
    slack_at_start
    >> mkdir
    >> sqlalchemy_create_objects_from_schema
    >> create_temp_table
    >> provenance_translation
    >> import_data
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summery
    This DAG containts official announcements about licence applications (vergunningaanvragen e.d.)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/bekendmakingen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/bekendmakingen.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=bekendmakingen/bekendmakingen&x=111153&y=483288&radius=10
"""
