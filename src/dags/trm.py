import pathlib
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator

from contact_point.callbacks import get_contact_point_on_failure_callback
from postgres_check_operator import PostgresCheckOperator, PostgresValueCheckOperator
from postgres_files_operator import PostgresFilesOperator
from postgres_permissions_operator import PostgresPermissionsOperator

from common import (
    vsd_default_args,
    slack_webhook_token,
    DATAPUNT_ENVIRONMENT,
    SHARED_DIR,
    MessageOperator,
)

from common.sql import (
    SQL_TABLE_RENAMES,
    SQL_CHECK_COUNT,
    SQL_CHECK_COLNAMES,
)

dag_id = "trm"
data_path = pathlib.Path(__file__).resolve().parents[1] / "data" / dag_id


def checker(records, pass_value):
    found_colnames = set(r[0] for r in records)
    return found_colnames >= set(pass_value)


with DAG(
    dag_id,
    default_args=vsd_default_args,
    template_searchpath=["/"],
    on_failure_callback=get_contact_point_on_failure_callback(dataset_id="spoorlijnen")
) as dag:

    extract_zips = []
    extract_shps = []
    convert_shps = []
    remove_drops = []
    check_counts = []
    check_colnames = []
    load_dumps = []
    rename_cols = []
    tram_colnames = ["ogc_fid", "wkb_geometry", "volgorde"]
    metro_colnames = ["ogc_fid", "wkb_geometry", "kge"]
    tmp_dir = f"{SHARED_DIR}/{dag_id}"

    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {dag_id} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mkdir = BashOperator(task_id="mkdir", bash_command=f"mkdir -p {tmp_dir}")

    for name, zip_name, shp_name, mincount, colnames in (
        (
            "tram",
            "Tram KGEs.zip",
            "KGE_hartlijnen_Amsterdam_2.054.shp",
            4200,
            tram_colnames,
        ),
        ("metro", "Metro KGEs.zip", "Metro KGEs.shp", 700, metro_colnames),
    ):

        extract_zips.append(
            BashOperator(
                task_id=f"extract_zip_{name}",
                bash_command=f'unzip -o "{data_path}/{zip_name}" -d {tmp_dir}/',
            )
        )

        extract_shps.append(
            BashOperator(
                task_id=f"extract_shp_{name}",
                bash_command="ogr2ogr -f 'PGDump' -nlt GEOMETRY -t_srs EPSG:28992 "
                f" -s_srs EPSG:28992 -nln {dag_id}_{name}_new "
                f"{tmp_dir}/{dag_id}_{name}.sql "
                f"'{tmp_dir}/{shp_name }'",
            )
        )

        convert_shps.append(
            BashOperator(
                task_id=f"convert_shp_{name}",
                bash_command="iconv -f iso-8859-1 -t utf-8 "
                f"{tmp_dir}/{dag_id}_{name}.sql > "
                f"{tmp_dir}/{dag_id}_{name}.utf8.sql",
            )
        )

        remove_drops.append(
            BashOperator(
                task_id=f"remove_drops_{name}",
                bash_command=f'perl -i -ne "print unless /DROP TABLE/" '
                f"{tmp_dir}/{name}.utf8.sql",
            )
        )

        check_counts.append(
            PostgresCheckOperator(
                task_id=f"check_count_{name}",
                sql=SQL_CHECK_COUNT,
                params=dict(tablename=f"{dag_id}_{name}_new", mincount=mincount),
            )
        )

        check_colnames.append(
            PostgresValueCheckOperator(
                task_id=f"check_colnames_{name}",
                sql=SQL_CHECK_COLNAMES,
                pass_value=colnames,
                result_checker=checker,
                params=dict(tablename=f"{dag_id}_{name}_new"),
            )
        )

    drop_tables = PostgresOperator(
        task_id="drop_tables",
        sql="DROP TABLE IF EXISTS trm_tram_new, trm_metro_new",
    )

    load_dumps = PostgresFilesOperator(
        task_id="load_dumps",
        sql_files=[
            f"{tmp_dir}/{dag_id}_metro.utf8.sql",
            f"{tmp_dir}/{dag_id}_tram.utf8.sql",
        ],
    )

    remove_entities = BashOperator(
        task_id="remove_entities",
        bash_command=r"sed -i -- 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g;' "
        f"{tmp_dir}/{dag_id}_tram.utf8.sql",
    )

    rename_tables = PostgresOperator(
        task_id="rename_tables",
        sql=SQL_TABLE_RENAMES,
        params=dict(tablenames=["trm_metro", "trm_tram"]),
    )

    # Grant database permissions
    grant_db_permissions = PostgresPermissionsOperator(task_id="grants", dag_name=dag_id)

slack_at_start >> mkdir >> extract_zips

for extract_zip, extract_shp, convert_shp, remove_drop in zip(
    extract_zips, extract_shps, convert_shps, remove_drops
):
    extract_zip >> extract_shp >> convert_shp >> remove_drop

for check_count, check_colname in zip(check_counts, check_colnames):
    check_count >> check_colname


remove_drops >> remove_entities >> drop_tables >> load_dumps >> check_counts
check_colnames >> rename_tables >> grant_db_permissions
