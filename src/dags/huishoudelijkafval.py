from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from importscripts.import_afvalinzamelingplanning import load_from_dwh
from swift_load_sql_operator import SwiftLoadSqlOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from schematools.utils import to_snake_case
from swap_schema_operator import SwapSchemaOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from sql.afvalinzamelingplanning import SQL_DROP_TMP_TABLE
from typing import List, Dict, Union

from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
    slack_webhook_token,
    MessageOperator,
)
from common.sql import SQL_CHECK_COUNT

DATASTORE_TYPE: str = (
    "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT
)

owner = "team_ruimte"
dag_id: str = "huishoudelijkafval"
tables: Dict[str, Union[List[str], str]] = {
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
    ],
    "dwh_stadsdelen": "planningVoertuigen",
}


with DAG(
    dag_id,
    default_args={**default_args, **{"owner": owner}},
    description="Huishoudelijkafval objecten, loopafstanden en planning afvalinzamelingvoertuigen",
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
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
        object_id=f"afval_huishoudelijk/{DATASTORE_TYPE}/" "afval_api.zip",
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
        subset_tables=f"{tables['dump_file']}",
        pg_schema="pte",
        rename_indexes=True,
    )

    # 5. DUMP FILE SOURCE
    # Swap tables to target schema public
    swap_schema = SwapSchemaOperator(
        task_id="swap_schema", dataset_name="huishoudelijkafval", subset_tables=tables['dump_file']
    )

    # 6. DWH STADSDELEN SOURCE
    # Load voertuigenplanning data into DB
    load_dwh = PythonOperator(
        task_id="load_from_dwh_stadsdelen",
        python_callable=load_from_dwh,
        op_args=[f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new"],
    )

    # 7. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params=dict(
            tablename=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new", mincount=1000
        ),
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
    change_data_capture = PgComparatorCDCOperator(
        task_id="change_data_capture",
        source_table=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new",
        target_table=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}",
        use_pg_copy=True,
        key_column="id",
        use_key=True,
    )

    # 11. DWH STADSDELEN SOURCE
    # Clean up; delete temp table
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params=dict(tablename=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new"),
    )

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(
        task_id="grants",
        dag_name=dag_id
    )


# FLOW
slack_at_start >> [drop_tables, load_dwh]
# Path 1
[
    drop_tables 
    >> load_file 
    >> provenance_file_data 
    >> swap_schema
]
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
