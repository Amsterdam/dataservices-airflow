from pathlib import Path
from typing import Any

from airflow import DAG
from bash_env_operator import BashEnvOperator
from common import DATASTORE_TYPE, EPHEMERAL_DIR, OTAP_ENVIRONMENT, MessageOperator, default_args
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from dcat_swift_operator import DCATSwiftOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from provenance_rename_operator import ProvenanceRenameOperator
from schematools.naming import to_snake_case
from sql.beheerkaart_basis import RENAME_COLS
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swap_schema_operator import SwapSchemaOperator
from swift_load_sql_operator import SwiftLoadSqlOperator

dag_id = "beheerkaart"
export_dir = f"{EPHEMERAL_DIR}/{dag_id}"
tables = {
    "beheerkaart_basis_bgt": "bkt_bgt",
    "beheerkaart_basis_eigendomsrecht": "bkt_eigendomsrecht",
    "beheerkaart_basis_kaart": "bkt_beheerkaart_basis",
}

# Dataset name as specified in Amsterdamsschema
dataset_name = "beheerkaartBasis"
dataset_name_database = to_snake_case(dataset_name)
gpkg_path = f"{export_dir}/{dataset_name_database}.gpkg"

owner = "team_ruimte"


def fetch_env_vars(*args: Any) -> Any:
    """Get the Postgres default DSN connection info as a dictionary."""
    pg_env_vars = DatabaseEngine(context=args).fetch_pg_env_vars()
    return pg_env_vars


with DAG(
    dag_id,
    default_args=default_args | {"owner": owner},
    # the access_control defines perms on DAG level. Not needed in Azure
    # since each datateam will get its own instance.
    access_control={owner: {"can_dag_read", "can_dag_edit"}},
    # New data is delivered every wednesday and friday evening,
    # So we schedule the import on friday and saturday morning
    schedule_interval="0 0 * * 4,6",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dataset_name),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Drop tables in target schema PTE
    # (schema which orginates from the DB dump file, see next step)
    #    based upon presence in the Amsterdam schema definition
    drop_tables = ProvenanceDropFromSchemaOperator(
        task_id="drop_tables",
        dataset_name=dataset_name,
        pg_schema="pte",
    )

    # 3. Create tables in target schema PTE
    # based upon presence in the Amsterdam schema definition
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{db_table_name}",
            data_schema_name=dataset_name,
            pg_schema="pte",
            data_table_name=data_table_name,
            db_table_name=db_table_name,
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=True,
        )
        for data_table_name, db_table_name in tables.items()
    ]

    # 4. Rename COLUMNS to source name before insert data
    rename_cols = PostgresOnAzureOperator(
        task_id="rename_cols",
        sql=RENAME_COLS,
    )

    # 5. load the dump file
    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"beheerkaart_pr/{dataset_name_database}/{DATASTORE_TYPE}/bkt.zip",
        dataset_name=dag_id,
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
    )

    # 6. Make the provenance translations
    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name=dataset_name,
        pg_schema="pte",
        prefix_table_name=f"{dataset_name_database}_",
        postfix_table_name="_new",
        rename_indexes=True,
    )

    # 7. Swap tables to target schema public
    swap_schema = SwapSchemaOperator(task_id="swap_schema", dataset_name=dataset_name)

    # 8. Create temporary directory
    # Due to a network mount (/tmp) on Azure AKS, the creating of
    # a geopackage (see: next step) fails.
    # We need an ephemeral non mount volume to create the GPKG file.
    mkdir = mk_dir(Path(export_dir))

    # 9. Create geopackage
    create_geopackage = BashEnvOperator(
        task_id="create_geopackage",
        env={},
        env_expander=fetch_env_vars,
        bash_command=f"rm -f {gpkg_path} &&"
        f'ogr2ogr -f GPKG {gpkg_path} PG:"tables={",".join(tables)}"',
    )

    # 10. Zip geopackage
    zip_geopackage = BashEnvOperator(
        task_id="zip_geopackage",
        env={},
        env_expander=fetch_env_vars,
        bash_command=f"zip -j {gpkg_path}.zip {gpkg_path}",
    )

    # 11. Upload geopackage to datacatalog
    upload_data = DCATSwiftOperator(
        environment=OTAP_ENVIRONMENT,
        task_id="upload_data",
        input_path=f"{gpkg_path}.zip",
        dataset_title="Beheerkaart publieke ruimte",
        distribution_id="1",
    )

    # 12. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dataset_name)

# FLOW
slack_at_start >> drop_tables >> create_tables

for table in create_tables:
    table >> rename_cols

(
    rename_cols
    >> swift_load_task
    >> provenance_renames
    >> swap_schema
    >> mkdir
    >> create_geopackage
    >> zip_geopackage
    >> upload_data
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summery
    This DAG processes data about maintaince public space and its objects.
    The formal (approved by the city council) public space naming does not suffix in this case.
    To cope with ommisions the objects are classified by the use of basisregistraties
    grootschalige topografie (BGT) en Kadaster (BRK).
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the productowner at team Omgevingswet.
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/beheerkaart/index.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/beheerkaart/index.html
    Example geosearch:
    N.A.
"""
