import csv
import hashlib
import itertools
import shutil
import sys
from csv import Dialect
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Dict, Tuple, Type, TypedDict

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
TMP_DIR = Path("/tmp") / DAG_ID
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


def int_hash(data: str) -> int:
    """Calculate an 8 byte BLAKE2s digest/hash as a number.

    .. warning:: By default BLAKE2s has a digest size of 32 bytes. Though PostgreSQL can
       easily store numbers with that kind of precision (eg ``NUMERIC(78)`` in this particular
       case) that would no longer be a number that ``pg_comparator`` considers a 'simple integer'.

       Hence we have chosen to generate an 8 byte BLAKE2s digest. That digest can still be
       stored in PostgreSQL's largest 'simple integer', namely the ``BIGINT``. However by making
       the digest size 4 times smaller than default we do increase the chance of collisions.

       I'm not yet sure if that is a wise decision. Should we get a collision within our
       dataset PostgreSQL will notify us with a uniqueness constraint error on the column that
       this function is used for. We won't get incorrect results, but things will break.

    Args:
        data: String to calculate the hash from.

    Returns:
        A integer representing the digest/hash.
    """
    return int.from_bytes(
        # BIGINT is an 8 byte signed integer. Hence our digest should be signed as well.
        hashlib.blake2s(data.encode(), digest_size=8).digest(),
        sys.byteorder,
        signed=True,
    )


def concat_columns(*column_values: object) -> str:
    """Concatenate column values into "|" separated string.

    Args:
        *column_values: any number of column values as long as they can each be casted
            to a ``str``.

    Returns:
        "|" separated string of ``column_values``.
    """
    return "|".join(map(str, column_values))


def hash_columns(*column_values: object) -> int:
    """Hash column values into an integer hash value.

    Args:
        *column_values: any number of column values as long as they can each be casted
            to a ``str``.

    Returns:
        A number representing the hash value of the ``column_values``
    """
    return int_hash(concat_columns(column_values))


ColName = str


@dataclass
class FuncCols:
    """Simple mapping from function to the columns the function should operate on."""

    func: Callable
    columns: Tuple[ColName, ...]


class CsvDef(TypedDict):
    dialect: Type[Dialect]
    fieldnames: Tuple[ColName, ...]
    derived: Dict[ColName, FuncCols]


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
        # We don't actually need a `cdc_id` derived column for this table. However to ensure we
        # can call the `PgComparatorCDCOperator` on all tables with the same set of parameters,
        # they all need a `cdc_id`.
        "derived": {"cdc_id": FuncCols(hash_columns, ("variabele",))},
    },
    "bbga_latest_and_greatest": {
        "dialect": bbga_dialect,
        "fieldnames": (
            "jaar",
            "gebiedcode_15",
            "indicator_definitie_id",
            "waarde",
        ),
        # As this table has over 6 million rows, the `cdc_id` derived column becomes a necessity
        # for us to be able to call `PgComparatorCDCOperator` with the incredibly faster
        # `--pg-copy` option. That option requires a key column with an integer value. That is
        # what we define here.
        #
        # The `id` column here is by virtue of an `identifier` property in the schema.
        # Schema-tools automatically creates an `id` column of type VARCHAR for us to fill with
        # a value derived from the columns specified in the `identifier` property.
        "derived": {
            "id": FuncCols(concat_columns, ("indicator_definitie_id", "jaar", "gebiedcode_15")),
            "cdc_id": FuncCols(hash_columns, ("indicator_definitie_id", "jaar", "gebiedcode_15")),
        },
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
        # We don't actually need a `cdc_id` derived column for this table. However to ensure we
        # can call the `PgComparatorCDCOperator` on all tables with the same set of parameters,
        # they all need a `cdc_id`.
        #
        # The `id` column here is by virtue of an `identifier` property in the schema.
        # Schema-tools automatically creates an `id` column of type VARCHAR for us to fill with
        # a value derived from the columns specified in the `identifier` property.
        "derived": {
            "id": FuncCols(concat_columns, ("indicator_definitie_id", "jaar")),
            "cdc_id": FuncCols(hash_columns, ("indicator_definitie_id", "jaar")),
        },
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

    def _mk_tmp_dir(**_: Any) -> str:
        TMP_DIR.mkdir(parents=True, exist_ok=True)
        return TMP_DIR.as_posix()

    mk_tmp_dir = PythonOperator(
        # The resulting tmp dir is returned via XCom using the task_id
        task_id="mk_tmp_dir",
        python_callable=_mk_tmp_dir,
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

    def rm_tmp_tables(postfix: str) -> PostgresOperator:
        return PostgresOperator(
            task_id=f"rm_tmp_tables{postfix}",
            sql="DROP TABLE IF EXISTS {tables}".format(
                tables=", ".join(map(lambda s: f"{TMP_TABLE_PREFIX}{s}", table_mappings.values()))
            ),
        )

    rm_tmp_tables_pre = rm_tmp_tables("_pre")

    sqlalchemy_create_objects_from_schema = SqlAlchemyCreateObjectOperator(
        task_id="sqlalchemy_create_objects_from_schema", data_schema_name=DAG_ID
    )

    add_cdc_ids = [
        PostgresOperator(
            task_id=f"add_cdc_id_to_{table}",
            sql=f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS cdc_id BIGINT UNIQUE NOT NULL ",
        )
        for table in table_mappings.values()
    ]

    postgres_create_tables_like = PostgresCreateTablesLikeOperator(
        task_id="postgres_create_tables_like",
        table_name_regex=f"^{DAG_ID}_.*",
        prefix=TMP_TABLE_PREFIX,
    )

    def _transform_csv_files(**kwargs: Any) -> None:
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
                # Ignore the existing header and replace it with a header of our own that matches
                # the columns names of table this CSV is going to be inserted in.
                #
                # TODO We should probably add a sanity check here before replacing the header
                next(reader)  # skip existing header
                output_fieldnames = tuple(
                    itertools.chain(csv_def["fieldnames"], csv_def["derived"].keys())
                )

                writer = csv.DictWriter(
                    dst, fieldnames=output_fieldnames, dialect=csv.unix_dialect
                )
                writer.writeheader()
                for row in reader:
                    # Calculate derived columns
                    for col_name, func_cols in csv_def["derived"].items():
                        row[col_name] = func_cols.func(*map(lambda cn: row[cn], func_cols.columns))
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
            use_pg_copy=True,
            key_column="cdc_id",
            use_key=True,
        )
        for table in table_mappings.values()
    ]

    rm_tmp_tables_post = rm_tmp_tables("_post")

    def _rm_tmp_dir(**kwargs: Any) -> None:
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
        # Ensure we don't have any lingering tmp tables from a previously failed run.
        >> rm_tmp_tables_pre
        >> sqlalchemy_create_objects_from_schema
        >> add_cdc_ids
        >> postgres_create_tables_like
        >> transform_csv_files
        >> postgres_insert_csv
        >> change_data_capture
        >> rm_tmp_tables_post
        >> rm_tmp_dir
    )
