import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import default_args
from common.db import get_engine
from common.http import download_file
from postgres_check_operator import (
    PostgresColumnNamesCheckOperator,
    PostgresCountCheckOperator,
    PostgresGeometryTypeCheckOperator,
    PostgresTableRenameOperator,
)
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
        columns=["geometry", "title", "type"],
    ),
    Route(
        "tunnels-gevaarlijke-stoffen",
        "https://api.data.amsterdam.nl/dcatd/datasets/ZtMOaEZSOnXM9w/purls/2",
        geometry_type="Point",
        columns=["geometry", "title", "categorie", "type"],
    ),
    Route(
        "u-routes",
        "https://api.data.amsterdam.nl/dcatd/datasets/ZtMOaEZSOnXM9w/purls/3",
        schema_table_name="u-routes_relation",  # name in the generated schema differs
        geometry_type="MultiLineString",
        columns=["id", "geometry", "name", "route", "type"],
    ),
]


def _load_geojson():
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


with DAG(dag_id, default_args=default_args) as dag:
    import_geojson = PythonOperator(
        task_id="import_geojson", python_callable=_load_geojson
    )

    # Can't chain operator >> list >> list,
    # so need to create these in a single loop
    for route in ROUTES:
        check_count = PostgresCountCheckOperator(
            task_id=f"check_count_{route.name}",
            table_name=route.tmp_db_table_name,
            min_count=3,
        )

        check_geo = PostgresGeometryTypeCheckOperator(
            task_id=f"check_geo_{route.name}",
            table_name=route.tmp_db_table_name,
            geometry_type=route.geometry_type,
        )

        check_columns = PostgresColumnNamesCheckOperator(
            task_id=f"check_columns_{route.name}",
            table_name=route.tmp_db_table_name,
            column_names=route.columns,
        )

        rename = PostgresTableRenameOperator(
            task_id=f"rename_{route.name}",
            old_table_name=route.tmp_db_table_name,
            new_table_name=route.db_table_name,
        )

        import_geojson >> check_count >> check_geo >> check_columns >> rename
