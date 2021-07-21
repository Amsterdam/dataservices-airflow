from typing import Dict, List, Union

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, slack_webhook_token
from common.sql import SQL_CHECK_COUNT
from contact_point.callbacks import get_contact_point_on_failure_callback
from importscripts.import_afvalinzamelingplanning import load_from_dwh
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_check_operator import PostgresCheckOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from schematools.utils import to_snake_case
from sql.afvalinzamelingplanning import SQL_DROP_TMP_TABLE
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

owner = "team_ruimte"
dag_id: str = "huishoudelijkafval"
tables: Dict[str, Union[List[str], str]] = {
    "dwh_stadsdelen": "planningVoertuigen",
}


with DAG(
    dag_id,
    default_args={**default_args, **{"owner": owner}},
    description="Huishoudelijkafval objecten, loopafstanden en planning afvalinzamelingvoertuigen",
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

    # 2. DWH STADSDELEN SOURCE
    # Load voertuigenplanning data into DB
    load_dwh = PythonOperator(
        task_id="load_voertuigenplanning_data",
        python_callable=load_from_dwh,
        op_args=[f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new"],
    )

    # 3. Check minimum number of records
    check_count = PostgresCheckOperator(
        task_id="check_count",
        sql=SQL_CHECK_COUNT,
        params={
            "tablename": f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new",
            "mincount": 1000,
        },
    )

    # 4. DWH STADSDELEN SOURCE
    # Rename COLUMNS based on provenance (if specified)
    provenance_dwh_data = ProvenanceRenameOperator(
        task_id="provenance_dwh",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        subset_tables=["".join(f"{tables['dwh_stadsdelen']}")],
        rename_indexes=False,
        pg_schema="public",
    )

    # 5. DWH STADSDELEN SOURCE
    # Create the DB target table (as specified in the JSON data schema)
    # if table not exists yet
    create_tables = SqlAlchemyCreateObjectOperator(
        task_id="create_table",
        data_schema_name=dag_id,
        data_table_name=f"{dag_id}_{tables['dwh_stadsdelen']}",
        ind_table=True,
        # when set to false, it doesn't create indexes; only tables
        ind_extra_index=True,
    )

    # 6. DWH STADSDELEN SOURCE
    # Check for changes to merge in target table by using CDC
    change_data_capture = PgComparatorCDCOperator(
        task_id="change_data_capture",
        source_table=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new",
        target_table=f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}",
        use_pg_copy=True,
        key_column="id",
        use_key=True,
    )

    # 7. DWH STADSDELEN SOURCE
    # Clean up; delete temp table
    clean_up = PostgresOperator(
        task_id="clean_up",
        sql=SQL_DROP_TMP_TABLE,
        params={"tablename": f"{dag_id}_{to_snake_case(tables['dwh_stadsdelen'])}_new"},
    )

    # 8. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)


# FLOW
(
    slack_at_start
    >> load_dwh
    >> check_count
    >> provenance_dwh_data
    >> create_tables
    >> change_data_capture
    >> clean_up
)
# [swap_schema, clean_up] >> grant_db_permissions
[clean_up] >> grant_db_permissions
dag.doc_md = """
    #### DAG summary
    This DAG processes data about waste objects and related context execution planning
    of waste collection.
    A part of the data orginates is push by team Ruimte to the referentiedatabase
    A part of the data orginates from DWH stadsdelen by means of a database connection
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/huishoudelijkafval.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/huishoudelijkafval.html
    Example geosearch:
    https://api.data.amsterdam.nl/geosearch/?x=130917&y=486064&datasets=huishoudelijkafval/container
"""
