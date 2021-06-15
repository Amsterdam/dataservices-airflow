import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from contact_point.callbacks import get_contact_point_on_failure_callback
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

SQL_RECREATE_TMP_TABLES = """
    DROP TABLE IF EXISTS {{ params.tablename }}_new CASCADE;
    CREATE TABLE IF NOT EXISTS {{ params.tablename }}_new AS
    (SELECT * FROM {{ params.tablename }} WHERE 1=2);
    ALTER TABLE {{ params.tablename }}_new ADD PRIMARY KEY (id);
"""

SQL_DROP_TMP_TABLES = """
    DROP TABLE IF EXISTS {{ params.tablename }}_new CASCADE;
"""

dag_id = "vergunningen"
variables = Variable.get(dag_id, deserialize_json=True)
files_to_download = variables["files_to_download"]
table_source_names = variables["table_source_names"]
table_target_names = variables["table_target_names"]
table_renames = list(zip(files_to_download, table_source_names, table_target_names))
tmp_dir = f"{SHARED_DIR}/{dag_id}"
total_checks = []
count_checks = []
geo_checks = []
check_name = {}


with DAG(
    dag_id,
    description="Beschikbaarheid Bed & Breakfast- en Omzettingsvergunning per wijk",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Download data from objectstore and store in tmp dir
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            # swift_conn_id default when ommited is the Various Small Datasets objectstore
            container="bed_and_breakfast",
            object_id=f"{DATAPUNT_ENVIRONMENT}/{file}",
            output_path=f"{tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 3. Modify data: remove all but inserts
    remove_owner_alters = [
        BashOperator(
            task_id=f"get_SQL_inserts_{file}",
            bash_command=f"sed -i -r '/INSERT INTO/!d' {tmp_dir}/{file} && "
            f"echo 'COMMIT;' >> {tmp_dir}/{file}",
        )
        for file in files_to_download
    ]

    # 4. Modify data: change table name to tmp name
    replace_tablename = [
        BashOperator(
            task_id=f"replace_tablename_{target_name}",
            bash_command=f'perl -pi -e "s/{source_name}/{dag_id}_{target_name}_new/g" '
            f"{tmp_dir}/{file}",
        )
        for file, source_name, target_name in table_renames
    ]

    # 5. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = [
        SqlAlchemyCreateObjectOperator(
            task_id=f"create_{target_name}_based_upon_schema",
            data_schema_name=dag_id,
            data_table_name=f"{dag_id}_{target_name}",
            ind_table=True,
            # when set to false, it doesn't create indexes; only tables
            ind_extra_index=False,
        )
        for _, _, target_name in table_renames
    ]

    # 6. Recreate tmp table in DB
    recreate_tmp_tables = [
        PostgresOperator(
            task_id=f"recreate_{target_name}_new",
            sql=SQL_RECREATE_TMP_TABLES,
            params=dict(tablename=f"{dag_id}_{target_name}"),
        )
        for file, _, target_name in table_renames
    ]

    # 7. Load data into DB (execute source sql)
    import_table = [
        PostgresFilesOperator(
            task_id=f"insert_data_into_{target_name}_new", sql_files=[f"{tmp_dir}/{file}"]
        )
        for file, _, target_name in table_renames
    ]

    # prepare the checks and added them per source to a dictionary
    for _, _, target_name in table_renames:

        total_checks.clear()
        count_checks.clear()
        geo_checks.clear()

        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{target_name}",
                pass_value=10,
                params=dict(table_name=f"{dag_id}_{target_name}_new"),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{target_name}",
                params=dict(
                    table_name=f"{dag_id}_{target_name}_new",
                    geotype=[
                        "MULTIPOLYGON",
                    ],
                    geo_column="geometrie",
                ),
                pass_value=1,
            )
        )

        total_checks = count_checks + geo_checks
        check_name[target_name] = total_checks

    # 8. Execute bundled checks on database
    multi_checks = [
        PostgresMultiCheckOperator(
            task_id=f"multi_check_{target_name}", checks=check_name[target_name]
        )
        for _, _, target_name in table_renames
    ]

    # 9. Check for changes to merge in target table
    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{target_name}",
            source_table=f"{dag_id}_{target_name}_new",
            target_table=f"{dag_id}_{target_name}",
        )
        for _, _, target_name in table_renames
    ]
    # 10. Clean up
    drop_tmp_tables = [
        PostgresOperator(
            task_id=f"drop_tmp_{target_name}_new",
            sql=SQL_DROP_TMP_TABLES,
            params=dict(tablename=f"{dag_id}_{target_name}"),
        )
        for _, _, target_name in table_renames
    ]

    # 11. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW
slack_at_start >> download_data

for (
    download,
    remove,
    replace,
    create_table,
    recreate_tmp_table,
    insert_data,
    multi_check,
    detection_modifications,
    drop_tmp_table,
) in zip(
    download_data,
    remove_owner_alters,
    replace_tablename,
    create_tables,
    recreate_tmp_tables,
    import_table,
    multi_checks,
    change_data_capture,
    drop_tmp_tables,
):

    [
        download
        >> remove
        >> replace
        >> create_table
        >> recreate_tmp_table
        >> insert_data
        >> multi_check
        >> detection_modifications
        >> drop_tmp_table
    ]

drop_tmp_tables >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains data about Bed & Breakfast and Conversion permit availability
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/vergunningen.html
"""
