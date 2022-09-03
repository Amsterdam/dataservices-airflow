import operator
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from postgres_on_azure_operator import PostgresOnAzureOperator
from airflow.utils.task_group import TaskGroup
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator

# These seemingly unused imports are actually used, albeit indirectly. See the code with
# `globals()[select_statement]`. Hence the `noqa: F401` comments.
from sql.leidingeninfrastructuur import SQL_KABELSBOVEN_OR_ONDERGRONDS_TABLE  # noqa: F401
from sql.leidingeninfrastructuur import SQL_MANTELBUIZEN_TABLE  # noqa: F401
from sql.leidingeninfrastructuur import SQL_PUNTEN_TABLE  # noqa: F401
from sql.leidingeninfrastructuur import SQL_ALTER_DATATYPES, SQL_GEOM_CONVERT, SQL_REMOVE_TABLE
from swift_operator import SwiftOperator

DAG_ID: Final = "leidingeninfrastructuur"
TMP_DIR: Final = Path(SHARED_DIR) / DAG_ID
variables: dict[str, dict[str, str]] = Variable.get(
    "leidingeninfrastuctuur", deserialize_json=True
)
files_to_download: dict[str, str] = variables["files_to_download"]
data_file: str = files_to_download["wibon"]
source_tables: str = files_to_download["source_tables"]
target_tables: str = files_to_download["target_tables"]
sql_to_execute: dict[str, str] = variables["sql_to_execute"]
env = Env()
total_checks = []
count_checks = []
geo_checks = []

TMP_TABLE_POSTFIX: Final = "new"


@dataclass
class Table:
    """Simple dataclass to represent various table ids and a select statement."""

    id: str  # noqa: A003
    base_id: str
    tmp_id: str
    sql_name: str


TABLES: Final[list[Table]] = [
    Table(
        id=f"{DAG_ID}_{table_name}",
        base_id=table_name,
        tmp_id=f"{DAG_ID}_{table_name}_{TMP_TABLE_POSTFIX}",
        sql_name=sql_name,
    )
    for table_name, sql_name in sql_to_execute.items()
]

with DAG(
    DAG_ID,
    description="""maintained by OVL (openbare verlichtingen)
    complementairy to source GOconnectIT a.k.a. WIBON.""",
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
    mkdir = mk_dir(TMP_DIR, clean_if_exists=False)

    # 3. Download data
    download_data = SwiftOperator(
        task_id="download_file",
        swift_conn_id="OBJECTSTORE_OPENBAREVERLICHTING",
        container="WIBON",
        object_id=data_file,
        output_path=Path(TMP_DIR) / data_file,
    )

    # 4. DROP temp tables
    # Start Task Group definition, combining tasks into one group for better visualisation
    group_tasks = []
    with TaskGroup(group_id="drop_tables_before") as drop_tables_before:
        for key in source_tables:
            task = [
                PostgresOnAzureOperator(
                    task_id=f"drop_table_{key}_before",
                    sql=SQL_REMOVE_TABLE,
                    params={"tablename": key},
                )
            ]
            group_tasks.append(task)
        chain(*group_tasks)

    # 5. Import data
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        sqlite_source=True,
        source_table_name=source_tables,
        input_file=Path(TMP_DIR) / data_file,
        s_srs="EPSG:28992",
        fid="id",
        auto_detect_type="YES",
        mode="PostgreSQL",
        twodimenional=False,
    )

    # 6. CREATE MANTELBUIZEN table (composite of multiple tables)
    create_tables = [
        PostgresOnAzureOperator(
            task_id=f"create_table_{table.base_id}",
            sql=globals()[table.sql_name],
            params={
                "tablename": table.tmp_id,
                "filter": "ondergronds" if "ondergronds" in table.tmp_id else "bovengronds",
            },
        )
        for table in TABLES
    ]

    # 7. Rename COLUMNS based on provenance (if specified)
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DAG_ID,
        prefix_table_name=f"{DAG_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        subset_tables=target_tables,
        pg_schema="public",
    )

    # 8. ALTER DATATYPES
    alter_data_types = [
        PostgresOnAzureOperator(
            task_id=f"alter_data_types_{table.base_id}",
            sql=SQL_ALTER_DATATYPES,
            params={"tablename": table.tmp_id},
        )
        for table in TABLES
    ]

    # 9. CONVERT GEOM
    converting_geom = [
        PostgresOnAzureOperator(
            task_id=f"converting_geom_{table.base_id}",
            sql=SQL_GEOM_CONVERT,
            params={"tablename": table.tmp_id},
        )
        for table in TABLES
    ]

    # 10. Drop Exisiting TABLE
    drop_tables = [
        PostgresOnAzureOperator(
            task_id=f"drop_existing_table_{table.id}",
            sql="DROP TABLE IF EXISTS {{ params.table_id }} CASCADE",
            params={"table_id": table.id},
        )
        for table in TABLES
    ]

    # 11. Rename TABLE
    rename_tables = [
        PostgresTableRenameOperator(
            task_id=f"rename_table_{table.base_id}",
            old_table_name=table.tmp_id,
            new_table_name=table.id,
        )
        for table in TABLES
    ]

    # Check minimum number of records
    # PREPARE CHECKS
    for table in TABLES:
        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{table.base_id}",
                pass_value=20,
                params={"table_name": table.id},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{table.base_id}",
                params={
                    "table_name": table.id,
                    "geotype": ["MULTIPOLYGON", "MULTILINESTRING", "POINT"],
                    "geo_column": "geometry",
                },
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks

    # 12. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 13. DROP temp tables
    # Start Task Group definition, combining tasks into one group for better visualisation
    group_tasks = []
    with TaskGroup(group_id="drop_tables_after") as drop_tables_after:
        for key in source_tables:
            task = [
                PostgresOnAzureOperator(
                    task_id=f"drop_table_{key}_after",
                    sql=SQL_REMOVE_TABLE,
                    params={"tablename": key},
                )
            ]
            group_tasks.append(task)
        chain(*group_tasks)

    # 14. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DAG_ID)

    # FLOW
    (
        slack_at_start
        >> mkdir
        >> download_data
        >> drop_tables_before
        >> import_data
        >> create_tables
    )

    for table in create_tables:
        (table >> provenance_translation >> converting_geom)

    for geom, data_type, target_table, temp_table in zip(
        converting_geom, alter_data_types, drop_tables, rename_tables
    ):

        (
            [geom >> data_type >> target_table >> temp_table]
            >> multi_checks
            >> drop_tables_after
            >> grant_db_permissions
        )

dag.doc_md = """
    #### DAG summary
    This DAG contains data about above ground underground cables
    The source is maintained by OVL (openbare verlichtingen) in a sqllite file.
    This data is complementairy to source GOconnectIT a.k.a. WIBON (which is
    provided by datateam omgevingswet/beeldschoon by pushing the data self to the referentie DB.)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at r.leicht@amsterdam.nl (René Leicht)
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/leidingeninfrastructuur/
    https://api.data.amsterdam.nl/v1/docs/datasets/leidingeninfrastructuur.html
"""
