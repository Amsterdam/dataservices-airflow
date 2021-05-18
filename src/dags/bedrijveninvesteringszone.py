import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from provenance_rename_operator import ProvenanceRenameOperator

from postgres_rename_operator import PostgresTableRenameOperator

from swift_operator import SwiftOperator

from common import (
    pg_params,
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)

from importscripts.convert_bedrijveninvesteringszones_data import convert_biz_data

from sql.bedrijveninvesteringszones import CREATE_TABLE, UPDATE_TABLE

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

from postgres_permissions_operator import PostgresPermissionsOperator

dag_id = "bedrijveninvesteringszones"
variables = Variable.get(dag_id, deserialize_json=True)
tmp_dir = f"{SHARED_DIR}/{dag_id}"
files_to_download = variables["files_to_download"]
total_checks = []
count_checks = []
geo_checks = []
check_name = {}

# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr):
    return f"'{instr}'"


with DAG(
    dag_id,
    description="tariefen, locaties en overige contextuele gegevens over bedrijveninvesteringszones.",
    default_args=default_args,
    user_defined_filters=dict(quote=quote),
    template_searchpath=["/"],
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    # 3. Download data
    download_data = [
        SwiftOperator(
            task_id=f"download_{file}",
            # if conn is ommitted, it defaults to Objecstore Various Small Datasets
            # swift_conn_id="SWIFT_DEFAULT",
            container="bedrijveninvesteringszones",
            object_id=str(file),
            output_path=f"{tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
    ]

    # 4. Dummy operator acts as an interface between parallel tasks to another parallel tasks (i.e. lists or tuples) with different number of lanes
    #  (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. get SQL from SHAPE files
    SHP_to_SQL = [
        BashOperator(
            task_id="SHP_to_SQL",
            bash_command=f"ogr2ogr -f 'PGDump' " f"{tmp_dir}/{dag_id}.sql {tmp_dir}/{file}",
        )
        for files in files_to_download.values()
        for file in files
        if "shp" in file
    ]

    # 6. Convert SQL to store all karakters between the code points 128 - 255 (wich are different in iso-8859-1) in UTF-8
    SQL_convert_UTF8 = BashOperator(
        task_id="convert_to_UTF8",
        bash_command=f"iconv -f iso-8859-1 -t utf-8  {tmp_dir}/{dag_id}.sql > "
        f"{tmp_dir}/{dag_id}.utf8.sql",
    )

    # 7. Update data (source shp) with the content of the separate xslx file
    SQL_update_data = [
        PythonOperator(
            task_id="update_SQL_data",
            python_callable=convert_biz_data,
            op_args=[
                f"{dag_id}_{dag_id}_new",
                f"{tmp_dir}/{dag_id}.utf8.sql",
                f"{tmp_dir}/{file}",
                f"{tmp_dir}/{dag_id}_updated_data_insert.sql",
            ],
        )
        for files in files_to_download.values()
        for file in files
        if "xlsx" in file
    ]

    # 8. CREATE target TABLE (the ogr2ogr output is not used to create the table)
    create_table = PostgresOperator(
        task_id=f"create_target_table",
        sql=CREATE_TABLE,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 9. Import data
    import_data = BashOperator(
        task_id="import_data",
        bash_command=f"psql {pg_params()} < {tmp_dir}/{dag_id}_updated_data_insert.sql",
    )

    # 10. UPDATE target TABLE (add display field content)
    update_table = PostgresOperator(
        task_id=f"update_target_table",
        sql=UPDATE_TABLE,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 11. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id=f"count_check",
            pass_value=50,
            params=dict(table_name=f"{dag_id}_{dag_id}_new "),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id=f"geo_check",
            params=dict(
                table_name=f"{dag_id}_{dag_id}_new",
                geotype=["POLYGON"],
            ),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 12. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 13. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 14. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


slack_at_start >> mkdir >> download_data

for data in download_data:
    data >> Interface

(
    Interface
    >> SHP_to_SQL
    >> SQL_convert_UTF8
    >> SQL_update_data
    >> create_table
    >> import_data
    >> update_table
    >> provenance_translation
    >> multi_checks
    >> rename_table
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains data about business investment zones
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    https://data.amsterdam.nl/datasets/bl6Wf85K8CfnwA/bedrijfsinvesteringszones-biz/
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/bedrijveninvesteringszones.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/bedrijveninvesteringszones.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=bedrijveninvesteringszones/bedrijveninvesteringszones&x=106434&y=488995&radius=10
"""
