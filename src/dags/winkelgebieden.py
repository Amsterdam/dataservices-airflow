import operator
from functools import partial
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.db import pg_params
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import COUNT_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.winkelgebieden_add import ADD_CATEGORIE_CATEGORIENAAM

dag_id = "winkelgebieden"
data_path = Path(__file__).resolve().parents[1] / "data"
sql_path = Path(__file__).resolve().parents[0] / "sql"
variables = Variable.get(dag_id, deserialize_json=True)
schema_end_point = variables["schema_end_point"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
metadataschema = f"{tmp_dir}/winkelgebieden_dataschema.json"
total_checks = []
count_checks = []
geo_checks = []

# prefill pg_params method with dataset name so
# it can be used for the database connection as a user.
# only applicable for Azure connections.
db_conn_string = partial(pg_params, dataset_name=dag_id)

with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. TEMP DIR
    mkdir = mk_dir(Path(tmp_dir))

    # 3. EXTRACT data based on TAB definition
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="ogr2ogr -f 'PGDump' "
        f"-t_srs EPSG:28992 -nln {dag_id} "
        f"{tmp_dir}/{dag_id}.sql {data_path}/{dag_id}/winkgeb2018.TAB "
        # the option -lco is added to rename the automated creation of the primairy key column (ogc fid) - due to use of ogr2ogr
        # in the -sql a select statement is added to get column renames that is specified in the dataschema
        # f"-sql @{tmp_column_file} -lco FID=ID -lco GEOMETRY_NAME=geometry",
        "-lco FID=ID -lco GEOMETRY_NAME=geometry",
    )

    # 4. CONVERT data to UTF8
    convert_data = BashOperator(
        task_id="convert_data",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    # 5. CREATE TABLE
    create_table = BashOperator(
        task_id="create_table",
        bash_command=f"psql {db_conn_string()} < {tmp_dir}/{dag_id}.utf8.sql",
    )

    # 6. DROP Exisiting TABLE
    drop_table = PostgresOnAzureOperator(
        task_id="drop_existing_table",
        sql=[
            f"DROP TABLE IF EXISTS {dag_id}_{dag_id} CASCADE",
        ],
    )

    # 7. RENAME COLUMNS based on PROVENANCE
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns", dataset_name=dag_id, pg_schema="public"
    )

    # 8. RENAME TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=dag_id,
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 8. ADD missing COLUMNS in source
    add_category = PostgresOnAzureOperator(
        task_id="add_columns",
        sql=ADD_CATEGORIE_CATEGORIENAAM,
        params=dict(tablename=f"{dag_id}_{dag_id}"),
    )

    # 9. PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=75,
            params=dict(table_name=f"{dag_id}_{dag_id}"),
            result_checker=operator.ge,
        )
    )

    # Data shows that 17 / 132 polygonen are invalid, to avoid crashing the flow, temporaly turned off
    # geo_checks.append(
    #     GEO_CHECK.make_check(
    #         check_id="geo_check",
    #         params=dict(
    #             table_name=f"{dag_id}_{dag_id}",
    #             geotype=["POLYGON", "MULTIPOLYGON"],
    #         ),
    #         pass_value=1,
    #     )
    # )
    # total_checks = count_checks + geo_checks
    total_checks = count_checks

    # 10. RUN bundled CHECKS (step 9)
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    slack_at_start
    >> mkdir
    >> extract_data
    >> convert_data
    >> create_table
    >> drop_table
    >> provenance_translation
    >> rename_table
    >> add_category
    >> multi_checks
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains shopping area's (winkelgebieden)
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/winkelgebieden/winkelgebieden/
"""
