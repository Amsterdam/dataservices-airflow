import operator
import requests
from requests.auth import HTTPBasicAuth
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from common.db import DatabaseEngine

from ogr2ogr_operator import Ogr2OgrOperator
from provenance_rename_operator import ProvenanceRenameOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

from more_ds.network.url import URL

from common import (
    default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    logger,
    env,
)

from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    GEO_CHECK,
)

from sql.wior import (
    DROP_COLS,
    SQL_DROP_TMP_TABLE,
    SQL_GEOM_VALIDATION,
    SQL_ADD_PK,
    SQL_SET_DATE_DATA_TYPES,
)

dag_id = "wior"
variables = Variable.get(dag_id, deserialize_json=True)
data_endpoint = variables["data_endpoints"]["wfs"]
tmp_dir = f"{SHARED_DIR}/{dag_id}"
data_file = f"{tmp_dir}/{dag_id}.geojson"
db_conn = DatabaseEngine()
password = env("AIRFLOW_CONN_WIOR_PASSWD")
user = env("AIRFLOW_CONN_WIOR_USER")
base_url = URL(env("AIRFLOW_CONN_WIOR_BASE_URL"))
total_checks = []
count_checks = []
geo_checks = []


# needed to put quotes on elements in geotypes for SQL_CHECK_GEO
def quote(instr: str) -> str:
    return f"'{instr}'"


class DataSourceError(Exception):
    pass


# data connection
def get_data() -> None:
    """calling the data endpoint"""
    # get data
    data_url = base_url / data_endpoint
    data_request = requests.get(data_url, auth=HTTPBasicAuth(user, password))
    # store data
    if data_request.status_code == 200:
        try:
            data = data_request.json()
        except json.decoder.JSONDecodeError as jde:
            logger.exception(f"Failed to convert request output to json for url {data_url}")
            raise json.decoder.JSONDecodeError from jde
        with open(f"{data_file}", "w") as file:
            file.write(json.dumps(data))
    else:
        logger.exception(f"Failed to call {data_url}")
        raise DataSourceError(f"HTTP status code: {data_request.status_code}")


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
    description="Werken (projecten) in de openbare ruimte.",
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
    download_data = PythonOperator(task_id="download_data", python_callable=get_data)

    # 4. Import data
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{dag_id}_{dag_id}_new",
        input_file=f"{tmp_dir}/{dag_id}.geojson",
        s_srs="EPSG:28992",
        t_srs="EPSG:28992",
        auto_detect_type="YES",
        geometry_name="geometry",
        fid="fid",
        mode="PostgreSQL",
        db_conn=db_conn,
    )

    # 5. Drop unnecessary cols
    drop_cols = PostgresOperator(
        task_id="drop_unnecessary_cols",
        sql=DROP_COLS,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 6. geometry validation
    geom_validation = PostgresOperator(
        task_id="geom_validation",
        sql=SQL_GEOM_VALIDATION,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 6. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=f"{dag_id}",
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. Add primary key to temp table (for cdc check)
    add_pk = PostgresOperator(
        task_id="add_pk",
        sql=SQL_ADD_PK,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # 8. Set date datatypes
    set_dates = PostgresOperator(
        task_id="set_dates",
        sql=SQL_SET_DATE_DATA_TYPES,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=25,
            params=dict(table_name=f"{dag_id}_{dag_id}_new"),
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params=dict(
                table_name=f"{dag_id}_{dag_id}_new",
                geotype=["MULTIPOLYGON", "POLYGON", "POINT", "MULTILINESTRING", "LINESTRING"],
                geo_column="geometrie",
            ),
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 9. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 10. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table_based_upon_schema",
        data_schema_name=f"{dag_id}",
        data_table_name=f"{dag_id}_{dag_id}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 11. Check for changes to merge in target table
    change_data_capture = PgComparatorCDCOperator(
        task_id="change_data_capture",
        source_table=f"{dag_id}_{dag_id}_new",
        target_table=f"{dag_id}_{dag_id}",
    )

    # 12. Clean up (remove temp table _new)
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params=dict(tablename=f"{dag_id}_{dag_id}_new"),
    )

(
    slack_at_start
    >> mkdir
    >> download_data
    >> import_data
    >> drop_cols
    >> geom_validation
    >> provenance_translation
    >> add_pk
    >> set_dates
    >> multi_checks
    >> create_table
    >> change_data_capture
    >> clean_up
)

dag.doc_md = """
    #### DAG summery
    This DAG containts public construction sites / projects
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/wior.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/wior.html
"""
