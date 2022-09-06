from typing import Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from common import DATASTORE_TYPE, MessageOperator, default_args
from common.sql import SQL_CHECK_COUNT
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_afvalinzamelingplanning import load_from_dwh
from postgres_check_operator import PostgresCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from provenance_rename_operator import ProvenanceRenameOperator
from schematools.utils import to_snake_case
from sql.afvalinzamelingplanning import SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swap_schema_operator import SwapSchemaOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

# owner: Only needed for CloudVPS. On Azure
# each team has its own Airflow instance.
owner = "team_ruimte"
dag_id: str = "huishoudelijkafval"
tables: dict[str, Union[list[str], str]] = {
    "dump_file": [
        "container",
        "containerlocatie",
        "cluster",
        "containertype",
        "weging",
        "clusterfractie",
        "loopafstandCategorie",
        "bagObjectLoopafstand",
        "adresLoopafstand",
        "bijplaatsingen",
    ],
    "dwh_stadsdelen": "planningVoertuigen",
}


with DAG(
    dag_id,
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    description="Huishoudelijkafval objecten, loopafstanden en planning afvalinzamelingvoertuigen",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. DUMP FILE SOURCE
    # Drop tables in target schema PTE
    # (schema which orginates from the DB dump file, see next step)
    # based upon presence in the Amsterdam schema definition
    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables",
        dataset_name="huishoudelijkafval",
        pg_schema="pte",
    )

    # 3. DUMP FILE SOURCE
    # load the dump file
    load_file = SwiftLoadSqlOperator(
        task_id="load_dump_file",
        container="Dataservices",
        object_id=f"afval_huishoudelijk/{DATASTORE_TYPE}/afval_api.zip",
        dataset_name=dag_id,
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
    )

    # 4. DUMP FILE SOURCE
    # Make the provenance translations
    provenance_file_data = ProvenanceRenameOperator(
        task_id="provenance_file",
        dataset_name="huishoudelijkafval",
        subset_tables=tables["dump_file"],
        pg_schema="pte",
        rename_indexes=True,
    )

    # 5. DUMP FILE SOURCE
    # Swap tables to target schema public
    swap_schema = SwapSchemaOperator(
        task_id="swap_schema", subset_tables=tables["dump_file"], dataset_name=dag_id
    )

    # 6. DWH STADSDELEN SOURCE
    # Load voertuigenplanning data into DB
    load_dwh = PythonOperator(
        task_id="load_from_dwh_stadsdelen",
        python_callable=load_from_dwh,
        provide_context=True,
        op_args=[f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new", dag_id],
    )

    # 7. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={
            "tablename": f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new",
            "mincount": 1000,
        },
    )

    # 8. DWH STADSDELEN SOURCE
    # Rename COLUMNS based on provenance (if specified)
    provenance_dwh_data = ProvenanceRenameOperator(
        task_id="provenance_dwh",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        subset_tables=["".join(f"{tables['dwh_stadsdelen']}")],
        rename_indexes=False,
        pg_schema="public",
    )

    # 9. DWH STADSDELEN SOURCE
    # Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = SqlAlchemyCreateObjectOperator(
        task_id="create_table",
        data_schema_name=dag_id,
        data_table_name=f"{dag_id}_{tables['dwh_stadsdelen']}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 10. DWH STADSDELEN SOURCE
    # Check for changes to merge in target table by using CDC
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name_lookup=dag_id,
        source_table_name=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new",
        target_table_name=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}",
        drop_target_if_unequal=True,
    )

    # 11. DWH STADSDELEN SOURCE
    # Clean up; delete temp table
    clean_up = PostgresOnAzureOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new"},
    )

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


# FLOW
slack_at_start >> [drop_tables, load_dwh]
# Path 1
[drop_tables >> load_file >> provenance_file_data >> swap_schema]
# Path 2
[
    load_dwh
    >> check_count
    >> provenance_dwh_data
    >> create_tables
    >> change_data_capture
    >> clean_up
]

[swap_schema, clean_up] >> grant_db_permissions

dag.doc_md = """
    #### DAG summary
    This DAG processes data about waste objects and related context execution planning
    of waste collection.
    A part of the data orginates from team Ruimte by means of a dump file
    A part of the data orginates from DWH stadsdelen by means of a database connection
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/huishoudelijkafval.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/huishoudelijkafval.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch/?x=130917&y=486064&datasets=huishoudelijkafval/container
"""
