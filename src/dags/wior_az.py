import json
import operator
import os
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Final, Optional

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from common import SHARED_DIR, MessageOperator, default_args, env, logger, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from dateutil import tz
from importscripts.wior import _pick_branch
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from requests.auth import HTTPBasicAuth
from sql.wior import (
    DROP_COLS,
    SQL_ADD_PK,
    SQL_DEL_DUPLICATE_ROWS,
    SQL_DROP_TMP_TABLE,
    SQL_GEOM_VALIDATION,
    SQL_SET_DATE_DATA_TYPES,
)
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator
from swift_operator import SwiftOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_SOEB"]

DAG_ID: Final = "wior_az"
DATASET_ID: Final = "wior"

variables: dict = Variable.get(DATASET_ID, deserialize_json=True)
data_endpoint: dict = variables["data_endpoints"]["wfs"]
tmp_dir: str = f"{SHARED_DIR}/{DAG_ID}"
data_file: str = f"{tmp_dir}/{DATASET_ID}.geojson"
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
    DAG_ID,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters={"quote": quote_string},
    description="Werken (projecten) in de openbare ruimte.",
	schedule_interval="0 2 * * *", # every day at 2 am (temporary: to avoid collision with non _az dags)
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
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
        output_path=f"{tmp_dir}/{DATASET_ID}.geojson",
        object_id=f"{datetime.now(timezone.utc).astimezone(to_zone).strftime('%Y-%m-%d')}_{DATASET_ID}.geojson",  # noqa: E501
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
        target_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        input_file=f"{tmp_dir}/{DATASET_ID}.geojson",
        s_srs="EPSG:28992",
        t_srs="EPSG:28992",
        auto_detect_type="YES",
        geometry_name="geometry",
        fid="fid",
        mode="PostgreSQL",
        sql_statement=f"""SELECT * FROM {DATASET_ID}
                WHERE hoofdstatus NOT ILIKE '%intake%'""",  # noqa: S608
    )

    # 7. Drop unnecessary cols
    drop_cols = PostgresOnAzureOperator(
        task_id="drop_unnecessary_cols",
        sql=DROP_COLS,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 8. Pick next step based on existence of duplicate rows.
    # if duplicates exists, then run task `remove_duplicate_rows` as
    # the next step. Else run the step `geom_validation` and continue.
    pick_branch = BranchPythonOperator(
        task_id="pick_branch",
        python_callable=_pick_branch,
        op_kwargs={"file": data_file},
    )

    # 9. Dummy operator to indicate no duplicates found in source data.
    no_duplicates_found = DummyOperator(task_id="no_duplicates_found")

    # 10. Remove duplicated rows (if applicable)
    remove_duplicate_rows = PostgresOnAzureOperator(
        task_id="remove_duplicate_rows",
        sql=SQL_DEL_DUPLICATE_ROWS,
        params={
            "tablename": f"{DATASET_ID}_{DATASET_ID}_new",
            "dupl_id_col": "_instance_id",
            "geom_col": "geometry",
        },
    )

    # 12. geometry validation
    geom_validation = PostgresOnAzureOperator(
        task_id="geom_validation",
        sql=SQL_GEOM_VALIDATION,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
        trigger_rule="one_success",
    )

    # 13. Rename COLUMNS based on Provenance
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 14. Add primary key to temp table (for cdc check)
    add_pk = PostgresOnAzureOperator(
        task_id="add_pk",
        sql=SQL_ADD_PK,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 15. Set date datatypes
    set_dates = PostgresOnAzureOperator(
        task_id="set_dates",
        sql=SQL_SET_DATE_DATA_TYPES,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # PREPARE CHECKS
    count_checks.append(
        COUNT_CHECK.make_check(
            check_id="count_check",
            pass_value=25,
            params={"table_name": f"{DATASET_ID}_{DATASET_ID}_new"},
            result_checker=operator.ge,
        )
    )

    geo_checks.append(
        GEO_CHECK.make_check(
            check_id="geo_check",
            params={
                "table_name": f"{DATASET_ID}_{DATASET_ID}_new",
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

    # 16. RUN bundled CHECKS
    multi_checks = PostgresMultiCheckOperator(task_id="multi_check", checks=total_checks)

    # 17. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table_based_upon_schema",
        data_schema_name=DATASET_ID,
        data_table_name=f"{DATASET_ID}_{DATASET_ID}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 18. Check for changes to merge in target table
    change_data_capture = PostgresTableCopyOperator(
        task_id="change_data_capture",
        dataset_name_lookup=DATASET_ID,
        source_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        target_table_name=f"{DATASET_ID}_{DATASET_ID}",
        drop_target_if_unequal=True,
    )

    # 19. Clean up (remove temp table _new)
    clean_up = PostgresOnAzureOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 20. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> upload_to_obs
    >> delete_from_obs
    >> import_data
    >> drop_cols
    >> pick_branch
    >> [remove_duplicate_rows, no_duplicates_found]
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
