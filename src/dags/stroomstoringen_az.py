from pathlib import Path
import os
from typing import Final

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from airflow.settings import TIMEZONE
from common import SHARED_DIR, MessageOperator, default_args, quote_string

from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from importscripts.import_stroomstoringen import _pick_branch
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.stroomstoringen import CONVERT_DATE_TIME, DROP_TABLE_IF_EXISTS, NO_DATA_PRESENT_INDICATOR
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

# set connnection to azure with specific account
os.environ["AIRFLOW_CONN_POSTGRES_DEFAULT"] = os.environ["AIRFLOW_CONN_POSTGRES_AZURE_BOR"]

DAG_ID: Final = "stroomstoringen_az"
DATASET_ID: Final = "stroomstoringen"
variables: dict[str, dict[str, str]] = Variable.get("stroomstoringen", deserialize_json=True)
endpoint_url: str = variables["data_endpoints"]["stroomstoringen"]

TODAY: Final = pendulum.now(TIMEZONE).format("MM-DD-YYYY")
TMP_DIR: Final = Path(SHARED_DIR) / DATASET_ID



with DAG(
    DAG_ID,
    description="locaties / gebieden stroomstoringen electra netwerk Liander.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    # schedule_interval="*/10 * * * *",
    schedule_interval="/15 * * * *", # every day at 15 min (temporary: to avoid collision with non _az dags)
    catchup=False,
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=DATASET_ID),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(TMP_DIR, clean_if_exists=False)

    # 3. download the data into temp directories
    download_data = HttpFetchOperator(
        task_id="download",
        endpoint=endpoint_url.format(today=TODAY),
        http_conn_id="STROOMSTORING_BASE_URL",
        tmp_file=f"{TMP_DIR}/stroomstoringen.geojson",
        output_type="text",
    )

    # 4. Import data
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        input_file=f"{TMP_DIR}/stroomstoringen.geojson",
        auto_detect_type="YES",
        mode="PostgreSQL",
        s_srs="EPSG:4326",
        t_srs="EPSG:28992",
        fid="objectid",
    )

    # 5. Pick branch based on amount of power failures.
    # No power failures triggers 'drop_table', generating dummy data.
    # >= 1 power failures triggers 'convert_datetime'
    pick_branch = BranchPythonOperator(
        task_id="pick_branch",
        python_callable=_pick_branch,
        op_kwargs={"file": f"{TMP_DIR}/stroomstoringen.geojson"},
    )

    # 6. Drop table if exists
    # NOTE: This triggers there are currently no power failure.
    drop_table = PostgresOnAzureOperator(
        task_id="drop_table",
        sql=DROP_TABLE_IF_EXISTS,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 7. Create the DB target temp table (based on the JSON data schema)
    # if table not exists yet
    # this ensures that when the source does not contain any data at a particular
    # moment (there can be no electric blackout at the current time) the table is still
    # created and the indication no data is present can be added.
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table_based_upon_schema",
        data_schema_name=DATASET_ID,
        data_table_name=f"{DATASET_ID}_{DATASET_ID}",
        db_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 8. If source has no data add dummy record (indication no data present)
    no_data_indicator = PostgresOnAzureOperator(
        task_id="no_data_indicator",
        sql=NO_DATA_PRESENT_INDICATOR,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 9. convert epoch time
    # NOTE: This triggers only if currently there are power failures.
    convert_datetime = PostgresOnAzureOperator(
        task_id="convert_datetime",
        sql=CONVERT_DATE_TIME,
        params={"tablename": f"{DATASET_ID}_{DATASET_ID}_new"},
    )

    # 10. Rename COLUMNS based on Provenance
    # NOTE: This triggers if one of the parent upstream tasks has been succesful.
    provenance_translation = ProvenanceRenameOperator(
        task_id="rename_columns",
        dataset_name=DATASET_ID,
        prefix_table_name=f"{DATASET_ID}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
        trigger_rule="one_success",
    )

    # 11. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{DATASET_ID}_{DATASET_ID}_new",
        new_table_name=f"{DATASET_ID}_{DATASET_ID}",
    )

    # 12. Grant database permissions
    # set create_roles to False, since ref DB Azure already created them.
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=DATASET_ID, create_roles=False)

# FLOW
(
    slack_at_start
    >> mkdir
    >> download_data
    >> import_data
    >> pick_branch
    >> [drop_table, convert_datetime]
)

# In case drop_table is executed
drop_table >> create_table >> no_data_indicator

# Flow after executing no_data_indicator or convert_datetime
(
    [no_data_indicator, convert_datetime]
    >> provenance_translation
    >> rename_table
    >> grant_db_permissions
)


dag.doc_md = """
    #### DAG summary
    This DAG contains data about electra blackouts (stroomstoringen).
    Source Liander
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/stroomstoringen.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/stroomstoringen.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=stroomstoringen/stroomstoringen&x=106434&y=488995&radius=10
"""
