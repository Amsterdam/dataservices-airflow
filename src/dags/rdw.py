from pathlib import Path
from typing import Iterable

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from postgres_on_azure_operator import PostgresOnAzureOperator
from common import SHARED_DIR, MessageOperator, default_args, quote_string
from common.path import mk_dir
from contact_point.callbacks import get_contact_point_on_failure_callback
from environs import Env
from http_fetch_operator import HttpFetchOperator
from more_ds.network.url import URL
from ogr2ogr_operator import Ogr2OgrOperator
from postgres_permissions_operator import PostgresPermissionsOperator
from provenance_rename_operator import ProvenanceRenameOperator
from sql.rdw import SQL_CREATE_TMP_TABLE, SQL_SWAP_TABLE

# Source defaults to 1000 records per request.
# There is no unlimited.
DATA_LIMIT: int = 100_000_000
DATA_SELECTIONS: dict[str, Iterable[str]] = {
    "basis": [
        "kenteken",
        "voertuigsoort",
        "massa_rijklaar",
        "toegestane_maximum_massa_voertuig",
        "inrichting",
        "datum_eerste_toelating",
        "lengte",
        "maximum_massa_samenstelling",
    ],
    "brandstof": ["kenteken", "brandstof_omschrijving", "emissiecode_omschrijving"],
    "assen": ["kenteken", "aantal_assen", "as_nummer", "technisch_toegestane_maximum_aslast"],
    "carrosserie": ["kenteken", "type_carrosserie_europese_omschrijving"],
}

dag_id: str = "rdw"
description: str = (
    "Contextuele informatie over een RDW geregistreerd voertuig op basis van het kenteken."
)
variables: dict = Variable.get(dag_id, deserialize_json=True)
endpoints: dict = variables["data_endpoints"]
tmp_dir: str = f"{SHARED_DIR}/{dag_id}"
env: Env = Env()
rdw_base_url: URL = URL(env("AIRFLOW_CONN_RDW_BASE_URL"))


with DAG(
    dag_id,
    description=description,
    default_args=default_args,
    user_defined_filters={"quote": quote_string},
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id=dag_id),
) as dag:

    # 1. Post info message on slack
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
    )

    # 2. Create temp directory to store files
    mkdir = mk_dir(Path(tmp_dir))

    # 3. Download csv
    download_data = [
        HttpFetchOperator(
            task_id=f"download_{resource}",
            endpoint=f"{endpoint}?$select={','.join(DATA_SELECTIONS[resource])}&$LIMIT={DATA_LIMIT}",  # noqa E501
            http_conn_id="rdw_conn_id",
            tmp_file=f"{tmp_dir}/{resource}.csv",
            output_type="text",
            verify=False,
        )
        for resource, endpoint in endpoints.items()
    ]

    # 4. Import csv into database
    import_data = [
        Ogr2OgrOperator(
            task_id=f"import_{resource}",
            target_table_name=f"{dag_id}_{resource}_download",
            input_file=f"{tmp_dir}/{resource}.csv",
            s_srs=None,
            auto_detect_type="YES",
            mode="PostgreSQL",
            fid="id",
        )
        for resource in endpoints
    ]

    keepalive_kwargs = {
        "keepalives": 1,
        "keepalives_idle": 60,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }

    # 5. SETUP tmp TABLE
    create_tmp_table = PostgresOnAzureOperator(
        task_id="create_tmp_table",
        sql=SQL_CREATE_TMP_TABLE,
        autocommit=True,
        parameters={**keepalive_kwargs},
    )

    # 6. Rename COLUMNS based on provenance (if specified)
    provenance = ProvenanceRenameOperator(
        task_id="provenance_col_rename",
        dataset_name=dag_id,
        prefix_table_name=f"{dag_id}_",
        postfix_table_name="_new",
        rename_indexes=False,
        pg_schema="public",
    )

    # 7. SWAP tmp to target (no CDC because of size)
    swap_table = PostgresOnAzureOperator(
        task_id="swap_table",
        sql=SQL_SWAP_TABLE,
        params={"tablename": f"{dag_id}_{dag_id}"},
    )

    # 8. Remove downloaded files
    clean_up_files = BashOperator(task_id="clean_up_files", bash_command=f"rm -r {tmp_dir}/*")

    # 9. Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

# FLOW

slack_at_start >> mkdir >> download_data

for (get_data, load_data) in zip(download_data, import_data):

    [get_data >> load_data] >> create_tmp_table

create_tmp_table >> provenance >> swap_table >> clean_up_files >> grant_db_permissions


dag.doc_md = """
    #### DAG summary
    This DAG contains data about RDW registered vehicles.
    #### Mission Critical
    Classified as 2 (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at n.papilaya@amsterdam.nl
    Naomy Papilaya
    Product Owner Online formulieren
    Dienstverlening
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    https://api.data.amsterdam.nl/v1/docs/datasets/rdw.html
    https://api.data.amsterdam.nl/v1/docs/wfs-datasets/rdw.html
    Example geosearch:
    N.A.
"""
