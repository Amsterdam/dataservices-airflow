import operator
from typing import Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import (
    DATAPUNT_ENVIRONMENT,
    MessageOperator,
    default_args,
    quote_string,
    slack_webhook_token,
)
from common.db import DatabaseEngine
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from importscripts.import_deelmobiliteit import import_auto_data, import_scooter_data
from more_ds.network.url import URL
from postgres_check_operator import COUNT_CHECK, GEO_CHECK, PostgresMultiCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from postgres_table_copy_operator import PostgresTableCopyOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.deelmobiliteit import SQL_DROP_TMP_TABLE, SQL_FLAG_NOT_RECENT_DATA, SQL_SET_GEOM
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

dag_id: str = "deelmobiliteit"
description: str = (
    "locaties en contextuele informatie over deelvoertuigen zoals auto’s, fietsen en scooters."
)
variables: Dict = Variable.get(dag_id, deserialize_json=True)
env: Env = Env()

endpoint_scooters: Dict = variables["scooters"]["data_endpoints"]
felyx_base_url: str = URL(env("AIRFLOW_CONN_FELYX_BASE_URL"))
felyx_api_key: str = env("AIRFLOW_CONN_FELYX_API_KEY")
ridecheck_base_url: str = URL(env("AIRFLOW_CONN_RIDECHECK_BASE_URL"))
ridecheck_token_url: str = URL(env("AIRFLOW_CONN_RIDECHECK_TOKEN_URL"))
ridecheck_token_client_id: str = env("AIRFLOW_CONN_RIDECHECK_CLIENT_ID")
ridecheck_token_client_secret: str = env("AIRFLOW_CONN_RIDECHECK_CLIENT_SECRET")

endpoint_autos: Dict = variables["autos"]["data_endpoints"]
mywheels_base_url: str = URL(env("AIRFLOW_CONN_MYWHEELS_BASE_URL"))
mywheels_api_key: str = env("AIRFLOW_CONN_MYWHEELS_API_KEY")

db_conn: object = DatabaseEngine()
total_checks: list = []
count_checks: list = []
geo_checks: list = []
check_name: dict = {}


with DAG(
    dag_id,
    description=description,
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
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

    # 2. Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_table = SqlAlchemyCreateObjectOperator(
        task_id="create_table",
        data_schema_name=dag_id,
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 3. Flag recent data version
    flag_not_recent = [
        PostgresOperator(
            task_id=f"flag_not_recent_{resource}",
            sql=SQL_FLAG_NOT_RECENT_DATA,
            params={"tablename": f"{dag_id}_{resource}"},
        )
        for resource in variables
    ]

    # 4. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface = DummyOperator(task_id="interface")

    # 5. Load scooter data into DB
    import_scooter_data = PythonOperator(
        task_id="import_scooter_data",
        python_callable=import_scooter_data,
        op_kwargs={
            "table_name": f"{dag_id}_scooters_new",
            "felyx_api_endpoint": f"{felyx_base_url}{endpoint_scooters['felyx']}",
            "felyx_api_header": {
                "content-type": "application/json",
                "x-api-key": felyx_api_key,
            },
            "ridecheck_token_endpoint": ridecheck_token_url,
            "ridecheck_token_payload": {
                "grant_type": "client_credentials",
                "client_id": ridecheck_token_client_id,
                "client_secret": ridecheck_token_client_secret,
                "scope": f"{ridecheck_base_url}/scooter.read",
            },
            "ridecheck_token_header": {"content-type": "application/x-www-form-urlencoded"},
            "ridecheck_data_endpoint": f"{ridecheck_base_url}{endpoint_scooters['ridecheck']}",
            "ridecheck_data_header": {
                "content-type": "application/json",
            },
        },
    )

    # 6. Load auto data into DB
    import_auto_data = PythonOperator(
        task_id="import_auto_data",
        python_callable=import_auto_data,
        op_kwargs={
            "table_name": f"{dag_id}_autos_new",
            "mywheels_api_endpoint": f"{mywheels_base_url}{endpoint_autos['mywheels']}",
            "mywheels_api_header": {
                "content-type": "application/json",
                "X-Simple-Auth-App-Id": mywheels_api_key,
            },
            "mywheels_api_payload": {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "search.map",
                "params": {
                    "locationPoint": {
                        "latitudeMin": 50,
                        "latitudeMax": 54,
                        "longitudeMin": 3,
                        "longitudeMax": 8,
                    },
                    "timeFrame": {"startDate": None, "endDate": None},
                },
            },
        },
    )

    # 7. Dummy operator acts as an Interface between parallel tasks
    # to another parallel tasks (i.e. lists or tuples) with different number
    # of lanes (without this intermediar, Airflow will give an error)
    Interface2 = DummyOperator(task_id="interface2")

    # 8. Check minimum number of records
    # PREPARE CHECKS
    for resource in variables:
        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{resource}",
                pass_value=50,
                params={"table_name": f"{dag_id}_{resource}_new "},
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{resource}",
                params={
                    "table_name": f"{dag_id}_{resource}_new",
                    "geotype": ["POINT"],
                    "geo_column": "geometrie",
                },
                pass_value=1,
            )
        )

    total_checks = count_checks + geo_checks

    # 9. RUN bundled CHECKS
    multi_checks = [
        PostgresMultiCheckOperator(task_id=f"multi_check_{resource}", checks=total_checks)
        for resource in variables
    ]

    # 10. Rename COLUMNS based on provenance (if specified)
    provenance = ProvenanceRenameOperator(
        task_id="provenance_col_rename",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 11. Set GEO type
    set_geom = [
        PostgresOperator(
            task_id=f"set_geom_{resource}",
            sql=SQL_SET_GEOM,
            params={"tablename": f"{dag_id}_{resource}_new"},
        )
        for resource in variables
    ]

    # 12. Check for changes to merge in target table by using CDC
    change_data_capture = [
        PostgresTableCopyOperator(
            task_id=f"change_data_capture_{resource}",
            dataset_name=dag_id,
            source_table_name=f"{dag_id}_{resource}_new",
            target_table_name=f"{dag_id}_{resource}",
            drop_target_if_unequal=True,
        )
        for resource in variables
    ]

    # 13. Clean up; delete temp table
    clean_up = [
        PostgresOperator(
            task_id=f"clean_up_{resource}",
            sql=SQL_DROP_TMP_TABLE,
            params={"tablename": f"{dag_id}_{resource}_new"},
        )
        for resource in variables
    ]

    # 14. Set HISTORY window (keep data from now till one month ago)
    history_window = [
        PostgresOperator(
            task_id=f"history_window_{resource}",
            sql=SQL_SET_GEOM,
            params={"tablename": f"{dag_id}_{resource}"},
        )
        for resource in variables
    ]

    # 15. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW

slack_at_start >> create_table >> flag_not_recent >> Interface
Interface >> import_auto_data >> Interface2
Interface >> import_scooter_data >> Interface2
Interface2 >> set_geom

for (set_geom, multi_checks) in zip(set_geom, multi_checks):

    [set_geom >> multi_checks] >> provenance  # type: ignore[operator]

provenance >> change_data_capture

for (change_data_capture, clean_up, history_window) in zip(
    change_data_capture, clean_up, history_window
):

    [change_data_capture >> clean_up >> history_window >> grant_db_permissions]  # type: ignore


dag.doc_md = """
    #### DAG summary
    This DAG contains data about rentalcars, -bikes and -scooters
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/deelmobiliteit.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/deelmobiliteit.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch?datasets=deelmobiliteit/scooters&x=106434&y=488995&radius=10

"""
