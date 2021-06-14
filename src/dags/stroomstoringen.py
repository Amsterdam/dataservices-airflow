from datetime import datetime

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
from common.db import DatabaseEngine
from contact_point.callbacks import get_contact_point_on_failure_callback
from http_fetch_operator import HttpFetchOperator
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_rename_operator import PostgresTableRenameOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.stroomstoringen import CONVERT_DATE_TIME

dag_id: str = "stroomstoringen"
variables: dict = Variable.get("stroomstoringen", deserialize_json=True)
endpoint_url: str = variables["data_endpoints"]["stroomstoringen"]


today = datetime.now().strftime("%m-%d-%Y")

tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}
db_conn: object = DatabaseEngine()


with DAG(
    dag_id,
    description="locaties / gebieden stroomstoringen electra netwerk Liander.",
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
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

    # 3. download the data into temp directorys
    download_data = HttpFetchOperator(
        task_id="download",
        endpoint=endpoint_url.format(today=today),
        http_conn_id="STROOMSTORING_BASE_URL",
        tmp_file=f"{tmp_dir}/stroomstoringen.geojson",
        output_type="text",
    )

    # 4. Create SQL
    import_data = Ogr2OgrOperator(
        task_id="import_data",
        target_table_name=f"{dag_id}_{dag_id}_new",
        input_file=f"{tmp_dir}/stroomstoringen.geojson",
        auto_detect_type="YES",
        mode="PostgreSQL",
        s_srs="EPSG:28992",
        t_srs="EPSG:28992",
        fid="objectid",
        db_conn=db_conn,
    )

    # 5. convert epoch time
    convert_datetime = PostgresOperator(
        task_id="convert_datetime",
        sql=CONVERT_DATE_TIME,
        params={"tablename": f"{dag_id}_{dag_id}_new"},
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

    # 7. Rename TABLE
    rename_table = PostgresTableRenameOperator(
        task_id="rename_table",
        old_table_name=f"{dag_id}_{dag_id}_new",
        new_table_name=f"{dag_id}_{dag_id}",
    )

    # 8. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

(
    slack_at_start
    >> mkdir
    >> download_data
    >> import_data
    >> convert_datetime
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
