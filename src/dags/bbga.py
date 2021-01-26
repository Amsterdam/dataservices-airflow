import csv
import itertools
import shutil
import tempfile
from csv import Dialect
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Dict, Tuple, TypedDict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from common import DATAPUNT_ENVIRONMENT, MessageOperator, default_args, slack_webhook_token
from http_fetch_operator import HttpFetchOperator
from pgcomparator_cdc_operator import PgComparatorCDCOperator
from postgres_create_tables_like_operator import PostgresCreateTablesLikeOperator
from postgres_insert_csv_operator import FileTable, PostgresInsertCsvOperator
from sqlalchemy_create_object_operator import SqlAlchemyCreateObjectOperator

FileStem = str
UrlPath = str

DAG_ID = "bbga"
TMP_DIR_BASE = Path("/tmp")
TMP_TABLE_PREFIX = "tmp_"
VARS = Variable.get(DAG_ID, deserialize_json=True)
data_endpoints: Dict[FileStem, UrlPath] = VARS["data_endpoints"]
table_mappings: Dict[FileStem, str] = VARS["table_mappings"]

assert set(data_endpoints.keys()) == set(
    table_mappings.keys()
), "Both mappings should have the same set of keys."


class bbga_dialect(Dialect):
    """Describe the usual properties of BBGA CSV files."""

    delimiter = ";"
    quotechar = '"'
    doublequote = True
    skipinitialspace = False
    lineterminator = "\n"
    quoting = csv.QUOTE_NONE


ColName = str


class CsvDef(TypedDict):
    dialect: Dialect
    fieldnames: Tuple[ColName, ...]
    composite: Dict[ColName, Tuple[ColName, ...]]


CSV_DEFINITIONS: Dict[FileStem, CsvDef] = {
    "metadata_latest_and_greatest": {
        "dialect": csv.unix_dialect,
        "fieldnames": (
            "sort",
            "begrotings_programma",
            "thema",
            "variabele",
            "label",
            "label_kort",
            "definitie",
            "bron",
            "peildatum",
            "verschijningsfrequentie",
            "rekeneenheid",
            "symbool",
            "groep",
            "format",
            "berekende_variabelen",
            "thema_kerncijfertabel",
            "tussenkopje_kerncijfertabel",
            "kleurenpalet",
            "legenda_code",
            "sd_minimum_bev_totaal",
            "sd_minimum_wvoor_bag",
            "topic_area",
            "label_1",
            "definition",
            "reference_date",
            "frequency",
        ),
        "composite": {},
    },
    "bbga_latest_and_greatest": {
        "dialect": bbga_dialect,
        "fieldnames": (
            "jaar",
            "gebiedcode_15",
            "indicator_definitie_id",
            "waarde",
        ),
        "composite": {"id": ("indicator_definitie_id", "jaar", "gebiedcode_15")},
    },
    "bbga_std_latest_and_greatest": {
        "dialect": bbga_dialect,
        "fieldnames": (
            "jaar",
            "indicator_definitie_id",
            "gemiddelde",
            "standaardafwijking",
            "bron",
        ),
        "composite": {"id": ("indicator_definitie_id", "jaar")},
    },
}

assert set(table_mappings.keys()) == set(
    CSV_DEFINITIONS.keys()
), "Both mappings should have the same set of keys."


with DAG(dag_id=DAG_ID, default_args=default_args) as dag:
    slack_at_start = MessageOperator(
        task_id="slack_at_start",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=f"Starting {DAG_ID} ({DATAPUNT_ENVIRONMENT})",
        username="admin",
    )

    mk_tmp_dir = PythonOperator(
        # The resulting tmp dir is returned via XCom using the task_id
        task_id="mk_tmp_dir",
        python_callable=tempfile.mkdtemp,
        op_kwargs=dict(prefix="bbga_", dir=TMP_DIR_BASE),
    )

    download_data = [
        HttpFetchOperator(
            task_id=f"download_{file_stem}",
            endpoint=url_path,
            http_conn_id="api_data_amsterdam_conn_id",
            tmp_file=f"{file_stem}.csv",
            xcom_tmp_dir_task_ids="mk_tmp_dir",
        )
        for file_stem, url_path in data_endpoints.items()
    ]

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema", data_schema_name=DAG_ID
    )

    postgres_create_tables_like = PostgresCreateTablesLikeOperator(
        task_id="postgres_create_tables_like",
        table_name_regex=f"^{DAG_ID}_.*",
        prefix=TMP_TABLE_PREFIX,
    )

    def _transform_csv_files(**kwargs) -> None:
        """Transform CSV files to have suitable headers and columns for DB insertion.

        The previously downloaded CSV files do not necessarily have column names that can be
        directly mapped to the column names of their corresponding DB tables. This function
        inserts an appropriate header with the proper column names (matching those in the DB).

        In addition it creates additional columns for composite keys. This requirement is a side
        effect of having used schema-tools to create the DB tables from the BBGA Amsterdam
        Schema definition.

        ..note:: Dialect of resulting CSV files is always 'unix', regardless of the input dialect.
        """
        tmp_dir = Path(kwargs["task_instance"].xcom_pull(task_ids="mk_tmp_dir"))
        for file_stem in CSV_DEFINITIONS:
            csv_file = tmp_dir / f"{file_stem}.csv"
            with open(csv_file, newline="") as src, NamedTemporaryFile(
                mode="w", newline="", dir=csv_file.parent, delete=False
            ) as dst:
                csv_def = CSV_DEFINITIONS[file_stem]
                reader = csv.DictReader(
                    src, fieldnames=csv_def["fieldnames"], dialect=csv_def["dialect"]
                )
                next(reader)  # skip header
                output_fieldnames = tuple(
                    itertools.chain(csv_def["fieldnames"], csv_def["composite"].keys())
                )

                writer = csv.DictWriter(
                    dst, fieldnames=output_fieldnames, dialect=csv.unix_dialect
                )
                writer.writeheader()
                for row in reader:
                    for col_name in csv_def["composite"]:
                        row[col_name] = "|".join(
                            map(lambda cn: row[cn], csv_def["composite"][col_name])
                        )
                    writer.writerow(row)
            Path(dst.name).rename(csv_file)

    transform_csv_files = PythonOperator(
        task_id="transform_csv_files",
        python_callable=_transform_csv_files,
        provide_context=True,  # Ensure we can use XCom to retrieve the previously created tmp dir
    )

    data = tuple(
        FileTable(file=Path(f"{file_stem}.csv"), table=f"{TMP_TABLE_PREFIX}{table}")
        for file_stem, table in table_mappings.items()
    )
    postgres_insert_csv = PostgresInsertCsvOperator(
        task_id="postgres_insert_csv", data=data, base_dir_task_id="mk_tmp_dir"
    )

    change_data_capture = [
        PgComparatorCDCOperator(
            task_id=f"change_data_capture_{table}",
            source_table=f"{TMP_TABLE_PREFIX}{table}",
            target_table=table,
        )
        for table in table_mappings.values()
    ]

    rm_tmp_tables = PostgresOperator(
        task_id="rm_tmp_tables",
        sql="DROP TABLE {tables}".format(
            tables=", ".join(map(lambda s: f"{TMP_TABLE_PREFIX}{s}", table_mappings.values()))
        ),
    )

    def _rm_tmp_dir(**kwargs) -> None:
        tmp_dir = kwargs["task_instance"].xcom_pull(task_ids="mk_tmp_dir")
        shutil.rmtree(tmp_dir)

    rm_tmp_dir = PythonOperator(
        task_id="rm_tmp_dir",
        python_callable=_rm_tmp_dir,
        provide_context=True,  # Ensure we can use XCom to retrieve the previously created tmp dir.
    )

    (
        slack_at_start
        >> mk_tmp_dir
        >> download_data
        >> sqlalchemy_create_objects_from_schema
        >> postgres_create_tables_like
        >> transform_csv_files
        >> postgres_insert_csv
        >> change_data_capture
        >> rm_tmp_tables
        >> rm_tmp_dir
    )
