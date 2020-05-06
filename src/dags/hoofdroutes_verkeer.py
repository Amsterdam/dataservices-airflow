import logging
from dataclasses import dataclass
import operator
from pathlib import Path
from typing import Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from common import default_args
from common.db import get_engine
from common.http import download_file
from postgres_check_operator import (
    PostgresColumnNamesCheckOperator,
    PostgresCountCheckOperator,
    PostgresGeometryTypeCheckOperator,
)
from postgres_check_operator import (
    PostgresMultiCheckOperator,
    COUNT_CHECK,
    COLNAMES_CHECK,
    GEO_CHECK,
)
from postgres_rename_operator import PostgresTableRenameOperator
from check_helpers import make_params
from schematools.importer.geojson import GeoJSONImporter
from schematools.introspect.geojson import introspect_geojson_files
from schematools.types import DatasetSchema

logger = logging.getLogger(__name__)
dag_id = "hoofdroutes_verkeer"
DATASET_NAME = "hoofdroutes"


@dataclass
class Route:
    name: str
    url: str
    geometry_type: str
    columns: list
    schema_table_name: Optional[str] = None
    db_table_name: Optional[str] = None
    post_process: Optional[str] = None

    def __post_init__(self):
        if self.schema_table_name is None:
            self.schema_table_name = self.name
        if self.db_table_name is None:
            name = self.name.replace("-", "_")
            self.db_table_name = f"{DATASET_NAME}_{name}"

    @property
    def tmp_db_table_name(self):
        return f"{self.db_table_name}_new"


ROUTES = [
    Route(
        "routes-gevaarlijke-stoffen",
        "https://api.data.amsterdam.nl/dcatd/datasets/ZtMOaEZSOnXM9w/purls/1",
        geometry_type="MultiLineString",
        columns=["id", "geometry", "title", "type"],
        post_process=[
            """
            UPDATE hoofdroutes_routes_gevaarlijke_stoffen_new
                SET geometry = ST_CollectionExtract(ST_MakeValid("geometry"), 2)
                WHERE NOT ST_IsValid("geometry")""",
            "ALTER TABLE hoofdroutes_routes_gevaarlijke_stoffen_new ADD COLUMN id SERIAL PRIMARY KEY",
            """ALTER TABLE hoofdroutes_routes_gevaarlijke_stoffen ALTER COLUMN geometry TYPE
                geometry(MultiLineString, 28992) using ST_Transform(ST_SetSRID(geometry, 4326), 28992)""",
        ],
    ),
    Route(
        "tunnels-gevaarlijke-stoffen",
        "https://api.data.amsterdam.nl/dcatd/datasets/ZtMOaEZSOnXM9w/purls/2",
        geometry_type="Point",
        columns=["id", "geometry", "title", "categorie", "type"],
        post_process=[
            "ALTER TABLE hoofdroutes_tunnels_gevaarlijke_stoffen_new ADD COLUMN id SERIAL PRIMARY KEY",
            """ALTER TABLE hoofdroutes_tunnels_gevaarlijke_stoffen ALTER COLUMN geometry TYPE
                geometry(Point, 28992) using ST_Transform(ST_SetSRID(geometry, 4326), 28992)""",
        ],
    ),
    Route(
        "u-routes",
        "https://api.data.amsterdam.nl/dcatd/datasets/ZtMOaEZSOnXM9w/purls/3",
        schema_table_name="u-routes_relation",  # name in the generated schema differs
        geometry_type="MultiLineString",
        columns=["id", "geometry", "name", "route", "type"],
        post_process=[
            """ALTER TABLE hoofdroutes_u_routes ALTER COLUMN geometry TYPE
                geometry(MultiLineString, 28992) using ST_Transform(ST_SetSRID(geometry, 4326), 28992)"""
        ],
    ),
]

DROP_TMPL = """
    {% for tablename in params.tablenames %}
    DROP TABLE IF EXISTS {{ tablename }} CASCADE;
    {% endfor %}
"""

TABLES_TO_DROP = [r.tmp_db_table_name for r in ROUTES]


def _load_geojson(postgres_conn_id):
    """As airflow executes tasks at different hosts,
    these tasks need to happen in a single call.

    Otherwise, the (large) file is downloaded by one host,
    and stored in the XCom table to be shared between tasks.
    """
    tmp_dir = Path(f"/tmp/{dag_id}")
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # 1. download files
    files = {}
    for route in ROUTES:
        dest = f"{tmp_dir}/{route.name}.geojson"
        logger.info("Downloading %s to %s", route.url, dest)
        download_file(route.url, dest, http_conn_id=None)
        files[route.name] = dest

    # 2. generate schema ("schema introspect geojson *.geojson")
    schema = introspect_geojson_files("gevaarlijke-routes", files=list(files.values()))
    schema = DatasetSchema.from_dict(schema)  # TODO: move to schema-tools?

    # XXX This is not running as one transaction atm, but autocommitting per chunk
    # 3. import data
    db_engine = get_engine()
    importer = GeoJSONImporter(schema, db_engine, logger=logger)
    for route in ROUTES:
        geojson_path = files[route.name]
        logger.info("Importing %s into %s", route.name, route.tmp_db_table_name)
        importer.load_file(
            geojson_path,
            table_name=route.schema_table_name,
            db_table_name=route.tmp_db_table_name,
            truncate=True,  # when reexecuting the same task
        )
        if route.post_process:
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            hook.run(route.post_process)


with DAG(dag_id, default_args=default_args) as dag:

    count_checks = []
    colname_checks = []
    geo_checks = []
    check_counts = []
    check_geos = []
    check_columns = []
    renames = []

    drop_old_tables = PostgresOperator(
        task_id="drop_old_tables", sql=DROP_TMPL, params=dict(tablenames=TABLES_TO_DROP)
    )

    import_geojson = PythonOperator(
        task_id="import_geojson",
        python_callable=_load_geojson,
        op_args=[default_args.get("postgres_conn_id", "postgres_default")],
    )

    for route in ROUTES:
        count_checks.append(
            COUNT_CHECK.make_check(
                check_id=f"count_check_{route.name}",
                pass_value=3,
                params=dict(table_name=route.tmp_db_table_name),
                result_checker=operator.ge,
            )
        )

        colname_checks.append(
            COLNAMES_CHECK.make_check(
                check_id=f"colname_check_{route.name}",
                parameters=[route.tmp_db_table_name],
                pass_value=set(route.columns),
                result_checker=operator.ge,
            )
        )

        geo_checks.append(
            GEO_CHECK.make_check(
                check_id=f"geo_check_{route.name}",
                params=dict(
                    table_name=route.tmp_db_table_name,
                    geotype=route.geometry_type.upper(),
                ),
                pass_value=1,
            )
        )

    checks = count_checks + colname_checks + geo_checks
    multi = PostgresMultiCheckOperator(
        task_id="multi", checks=checks, params=make_params(checks)
    )

    # Can't chain operator >> list >> list,
    # so need to create these in a single loop
    for route in ROUTES:
        check_counts.append(
            PostgresCountCheckOperator(
                task_id=f"check_count_{route.name}",
                table_name=route.tmp_db_table_name,
                min_count=3,
            )
        )

        check_geos.append(
            PostgresGeometryTypeCheckOperator(
                task_id=f"check_geo_{route.name}",
                table_name=route.tmp_db_table_name,
                geometry_type=route.geometry_type,
            )
        )

        check_columns.append(
            PostgresColumnNamesCheckOperator(
                task_id=f"check_columns_{route.name}",
                table_name=route.tmp_db_table_name,
                column_names=route.columns,
            )
        )

        renames.append(
            PostgresTableRenameOperator(
                task_id=f"rename_{route.name}",
                old_table_name=route.tmp_db_table_name,
                new_table_name=route.db_table_name,
            )
        )


for check_count, check_geo, check_column, rename in zip(
    check_counts, check_geos, check_columns, renames
):
    check_count >> check_geo >> check_column >> rename

drop_old_tables >> import_geojson >> check_counts
