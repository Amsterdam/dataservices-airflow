import json
import operator
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Optional

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    OTAP_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
    default_args,
    env,
    logger,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from dateutil import tz
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from requests.auth import HTTPBasicAuth
from sql.wior import (
    DROP_COLS,
    SQL_ADD_PK,
    SQL_DROP_TMP_TABLE,
    SQL_GEOM_VALIDATION,
    SQL_SET_DATE_DATA_TYPES,
)
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

dag_id: str = "wior"
variables: dict = Variable.get(dag_id, deserialize_json=True)
data_endpoint: dict = variables["data_endpoints"]["wfs"]
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
data_file: str = f"{tmp_dir}/{dag_id}.geojson"
db_conn: DatabaseEngine = DatabaseEngine()
password: str = env("AIRFLOW_CONN_WIOR_PASSWD")
user: str = env("AIRFLOW_CONN_WIOR_USER")
base_url: str = URL(env("AIRFLOW_CONN_WIOR_BASE_URL"))
total_checks: list = []
count_checks: list = []
geo_checks: list = []
to_zone: Optional[tzinfo] = tz.gettz("Europe/Amsterdam")


class DataSourceError(Exception):
    """Custom exeception for not available data source."""

    pass


# data connection
def get_data() -> None:
    """Calling the data endpoint."""
    data_url = base_url / data_endpoint  # type: ignore
    data_request = requests.get(data_url, auth=HTTPBasicAuth(user, password))
    # store data
    if data_request.status_code == 200:
        try:
            data = data_request.json()
        except json.decoder.JSONDecodeError as jde:
            logger.exception("Failed to convert request output to json for url %s", data_url)
            raise json.decoder.JSONDecodeError from jde
        with open(data_file, "w") as file:
            file.write(json.dumps(data))
    else:
        logger.exception("Failed to call %s", data_url)
        raise DataSourceError(f"HTTP status code: {data_request.status_code}")


with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    description="Werken (projecten) in de openbare ruimte.",
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"{SLACK_ICON_START} Starting {dag_id} ({OTAP_ENVIRONMENT})",
        username="admin",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download data
    download_data = PythonOperator(task_id="download_data", python_callable=get_data)

    # 4. Upload data to objectstore
    upload_to_obs = SwiftOperator(
        task_id="upload_to_obs",
        swift_conn_id="OBJECTSTORE_VICTOR",
        action_type="upload",
        container="WIOR",
        output_path=f"{tmp_dir}/{dag_id}.geojson",
        object_id=f"{datetime.now(timezone.utc).astimezone(to_zone).strftime('%Y-%m-%d')}_{dag_id}.geojson",  # noqa: E501
    )

    # 5. Delete files from objectstore (that do not fit given time window)
    delete_from_obs = SwiftOperator(
        task_id="delete_from_obs",
        swift_conn_id="OBJECTSTORE_VICTOR",
        action_type="delete",
        container="WIOR",
        time_window_in_days=100,
    )

    # 6. Import data
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
        sql_statement=f"""SELECT * FROM {dag_id}
                WHERE hoofdstatus NOT ILIKE '%intake%'""",  # noqa: S608
    )

    # 7. Drop unnecessary cols
    drop_cols = PostgresOperator(
        task_id="drop_unnecessary_cols",
        sql=DROP_COLS,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
    )

    # 8. geometry validation
    geom_validation = PostgresOperator(
        task_id="geom_validation",
        sql=SQL_GEOM_VALIDATION,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
    )

    # 9. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 10. Add primary key to temp table (for cdc check)
    add_pk = PostgresOperator(
        task_id="add_pk",
        sql=SQL_ADD_PK,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
    )

    # 11. Set date datatypes
    set_dates = PostgresOperator(
        task_id="set_dates",
        sql=SQL_SET_DATE_DATA_TYPES,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=25,
            params={"table_name": f"{dag_id}_{dag_id}_new"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{dag_id}_{dag_id}_new",
                "geotype": [
                    "MULTIPOLYGON",
                    "POLYGON",
                    "POINT",
                    "MULTILINESTRING",
                    "LINESTRING",
                    "GEOMETRYCOLLECTION",
                ],
                "geo_column": "geometrie",
            },
            pass_value=1,
        )
    )

    total_checks = count_checks + geo_checks

    # 12. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 13. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table_based_upon_schema",
        data_schema_name=dag_id,
        data_table_name=f"{dag_id}_{dag_id}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 14. Check for changes to merge in target table
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name=dag_id,
        source_table_name=f"{dag_id}_{dag_id}_new",
        target_table_name=f"{dag_id}_{dag_id}",
        drop_target_if_unequal=True,
    )

    # 15. Clean up (remove temp table _new)
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
    )

    # 16. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    slack_at_start
    >> mkdir
    >> download_data
    >> upload_to_obs
    >> delete_from_obs
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
    >> grant_db_permissions
)

dag.doc_md = """
    #### DAG summary
    This DAG contains public construction sites / projects (WIOR)
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
