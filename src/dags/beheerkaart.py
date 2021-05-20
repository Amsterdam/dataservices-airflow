from airflow import DAG
from airflow.operators.bash import BashOperator

from bash_env_operator import BashEnvOperator
from swift_load_sql_operator import SwiftLoadSqlOperator
from provenance_rename_operator import ProvenanceRenameOperator
from provenance_drop_from_schema_operator import ProvenanceDropFromSchemaOperator
from swap_schema_operator import SwapSchemaOperator
from dcat_swift_operator import DCATSwiftOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator


from common import (
    default_args,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    slack_webhook_token,
    MessageOperator,
)

from common.db import fetch_pg_env_vars

DATASTORE_TYPE = "acceptance" if DATAPUNT_ENVIRONMENT == "development" else DATAPUNT_ENVIRONMENT

dag_id = "beheerkaart"
tmp_dir = f"{SHARED_DIR}/{dag_id}"
tables = {
    "beheerkaart_basis_bgt": "bkt_bgt",
    "beheerkaart_basis_eigendomsrecht": "bkt_eigendomsrecht",
    "beheerkaart_basis_kaart": "bkt_beheerkaart_basis",
}

dataset_name = f"{dag_id}_basis"
gpkg_path = f"{tmp_dir}/{dataset_name}.gpkg"

owner = "team_ruimte"
with DAG(
    dag_id,
    default_args={**default_args, **{"owner": owner}},
    # New data is delivered every wednesday and friday evening,
    # So we schedule the import on friday and saturday morning
    schedule_interval="0 0 * * 4,6",
) as dag:
    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
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

    # 4. load the dump file
    swift_load_task = SwiftLoadSqlOperator(
        task_id="swift_load_task",
        container="Dataservices",
        object_id=f"beheerkaart_pr/{dataset_name}/{DATASTORE_TYPE}/bkt.zip",
        swift_conn_id="objectstore_dataservices",
        # optionals
        # db_target_schema will create the schema if not present
        db_target_schema="pte",
    )

    # 5. Make the provenance translations
    provenance_renames = ProvenanceRenameOperator(
        task_id="provenance_renames",
        dataset_name=dataset_name,
        pg_schema="pte",
        prefix_table_name=f"{dataset_name}_",
        postfix_table_name="_new",
        rename_indexes=True,
    )

    # 6. Swap tables to target schema public
    swap_schema = SwapSchemaOperator(task_id="swap_schema", dataset_name=dataset_name)

    # 7. Create temporary directory
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 8. Create geopackage
    create_geopackage = BashEnvOperator(
        task_id="create_geopackage",
        env={},
        env_expander=fetch_pg_env_vars,
        bash_command=f'ogr2ogr -f GPKG {gpkg_path} PG:"tables={",".join(tables)}"',
    )

    # 9. Zip geopackage
    zip_geopackage = BashEnvOperator(
        task_id="zip_geopackage",
        env={},
        env_expander=fetch_pg_env_vars,
        bash_command=f"zip -j {gpkg_path}.zip {gpkg_path}",
    )

    # 10. Upload geopackage to datacatalog
    upload_data = DCATSwiftOperator(
        environment=DATAPUNT_ENVIRONMENT,
        task_id="upload_data",
        input_path=f"{gpkg_path}.zip",
        dataset_title="Beheerkaart basis",
        distribution_id="1",
    )

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dataset_name)

# FLOW
slack_at_start >> drop_tables >> create_tables

for table in create_tables:
    table >> swift_load_task

swift_load_task >> provenance_renames >> swap_schema >> mkdir >> create_geopackage >> zip_geopackage >> upload_data >> grant_db_permissions  # noqa
