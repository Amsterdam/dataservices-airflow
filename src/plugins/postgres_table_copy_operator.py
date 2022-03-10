import itertools
import operator
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Callable, Final, Optional, cast

from airflow.exceptions import AirflowFailException
from airflow.models import XCOM_RETURN_KEY, BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from environs import Env
from more_ds.network.url import URL
from psycopg2 import extras, sql
from schematools import TMP_TABLE_POSTFIX
from schematools.exceptions import SchemaObjectNotFound
from schematools.types import DatasetSchema
from schematools.utils import dataset_schema_from_url, to_snake_case
from xcom_attr_assigner_mixin import XComAttrAssignerMixin

env = Env()
SCHEMA_URL: Final = URL(env("SCHEMA_URL"))


@dataclass(frozen=True)
class Table:
    """Holding class for table name and schema."""

    schema: str
    name: str

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class TableMapping:
    """Simple mapping from source table to target table."""

    source: Table
    target: Table
    view_name: Optional[str] = None


@dataclass
class Statement:
    """Simple grouping of SQL statement with associated logging message."""

    sql: str
    log_msg: str


SELECT_TABLE_FOR_VIEW_NAME: Final[
    str
] = """
    -- Query to find the table associated with a view
    -- pg_depend: system table in postgres that holds dependency relations
    -- pg_rewrite: system table the holds information about views
    -- pg_class: system table with information about views and tables
    -- d.refobjid: oid of the referenced object
    -- d.objid: object having a dependency
    -- r.oid: oid of a rule (info about view) attached
    -- r.ev_class: table/view that the rule applies to
    -- v.oid: referring to the view
    -- d.classid: oid of the system catalog depedent is in
    -- d.refclassid: oid of system catalog dependency is in
    SELECT DISTINCT d.refobjid::regclass AS ref_object
    FROM pg_depend AS d
    JOIN pg_rewrite AS r ON r.oid = d.objid
    JOIN pg_class AS v ON v.oid = r.ev_class
    WHERE v.relkind = 'v'
      AND d.classid = 'pg_rewrite'::regclass
      AND d.refclassid = 'pg_class'::regclass
      AND d.deptype = 'n'
      AND NOT (v.oid = d.refobjid)
      AND v.oid = %(view_or_table_name)s::regclass
    """


class PostgresTableCopyOperator(BaseOperator, XComAttrAssignerMixin):
    """Copy table to another table, create target table structure if needed from source.

    The premise here is that the source table is a temporary table and the target table or view
    is the final result. In copying, it not only copies the table definition, but optionally also
    the data. To prevent duplicate key errors it has has the option to truncate the target table
    first. This is necessary if the target already exists and contains data. Lastly
    it has the ability to drop the source (=temporary) table.

    .. note:: A strange assumption is being made by this operator. It does require you to specify
       both source (temporary) and target tables. However for finding any junction tables it
       assumes that all these junction tables have a TMP_TABLE_POSTFIX postfix.
       Makes you wonder why that assumption is not made for original source table?
       Or even better, why that wasn't made configurable.
    """

    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
        nested_db_table_names: Optional[list[str]] = None,
        dataset_name: Optional[str] = None,
        source_schema_name: str = "public",
        target_schema_name: str = "public",
        truncate_target: bool = True,
        drop_target_if_unequal: bool = False,
        copy_data: bool = True,
        drop_source: bool = True,
        task_id: str = "copy_table",
        postgres_conn_id: str = "postgres_default",
        database: Optional[str] = None,
        autocommit: bool = False,
        xcom_task_ids: Optional[str] = None,
        xcom_attr_assigner: Callable[[Any, Any], None] = lambda o, x: None,
        xcom_key: str = XCOM_RETURN_KEY,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgresTableCopyOperator.

        Args:
            dataset_name: Name of dataset as defined in Amsterdam schema. Used to do a lookup
            of the columns to cope with a possible datatype mismatch due to a different
            column order between target and source table regarding data copy.
            source_table_name: Name of the temporary table that needs to be copied.
            source_schema_name: Name of the schema of the source table.
            target_table_name: Name of the table that needs to be created and data copied to.
            nested_db_table_names: Optional set of names for nested tables that
                need to be processed.
            target_schema_name: Name of the schema of the target table.
            truncate_target: Whether the target table should be truncated before being copied to.
            drop_target_if_unequal: Whether the target table should be dropped if it is not
                equal to the source table wrt. column types, names and positions
            copy_data: Whether to copied data. If set to False only the table definition is copied.
            drop_source: Whether to drop the source table after the copy process.
            task_id: Task ID
            postgres_conn_id: The PostgreSQL connection id.
            database: Name of the databas to use (if different from database from connection id)
            autocommit: What to set the connection's autocommit setting to.
            xcom_task_ids: The id of the task that is providing the xcom info.
            xcom_attr_assigner: Callable tha can be provided to assign new values
                to object attributes.
            xcom_key: Key use to grab the xcom info, defaults to the airflow
                default `return_value`.
            *args:
            **kwargs:
        """
        super().__init__(*args, task_id=task_id, **kwargs)
        self.dataset_name = dataset_name
        self.source_table_name = source_table_name
        self.source_schema_name = source_schema_name
        self.target_table_name = target_table_name
        self.nested_db_table_names: list[str] = (
            nested_db_table_names if nested_db_table_names is not None else []
        )
        self.target_schema_name = target_schema_name
        self.truncate_target = truncate_target
        self.drop_target_if_unequal = drop_target_if_unequal
        self.copy_data = copy_data
        self.drop_source = drop_source
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.autocommit = autocommit

        self.xcom_task_ids = xcom_task_ids
        self.xcom_attr_assigner = xcom_attr_assigner
        self.xcom_key = xcom_key

        # Some checks for valid values
        assert (source_table_name is None) is (
            target_table_name is None
        ), "Source and target should be both None or both provided."

        # Here we build on the previous assertion
        assert bool(source_table_name) ^ (
            xcom_task_ids is not None
        ), "Either table names or xcom_task_ids should be provided."

        if not copy_data and drop_source:
            raise AirflowFailException(
                "Configuration error: source data will not be copied, "
                "even though source table will be dropped."
            )

        if dataset_name is None and not drop_target_if_unequal:
            raise AirflowFailException(
                "Configuration error: not knowing the columnnames "
                "and drop_target_if_unequal is False "
                "can lead to DatatypeMismatch exceptions "
                "when the schema of a table changes."
            )

    def _validate_tables_equality(
        self, cursor: Any, all_table_copies: list[TableMapping]
    ) -> dict[TableMapping, list[str]]:
        """Validate data structure between target and source table."""
        diffs = defaultdict(list)
        for table_mapping in all_table_copies:
            # First make sure the target table exists
            cursor.execute(
                "SELECT to_regclass(%s)",
                (table_mapping.target.full_name,),
            )

            if cursor.fetchone()["to_regclass"] is None:
                self.log.info(
                    "Target table %s not found, skipping equality check.", table_mapping.target
                )
                continue

            self.log.info(
                "Executing equality check between %s and %s",
                table_mapping.target,
                table_mapping.source,
            )

            cursor.execute(
                """
                    WITH src AS
                      (SELECT data_type as dtt_source,
                              ordinal_position as ord_pos_source,
                              column_name as col_name_source
                       FROM information_schema.columns
                       WHERE table_name = %(source_table_name)s
                        AND table_schema = %(source_schema_name)s),
                         tgt AS
                      (SELECT data_type as dtt_target,
                              ordinal_position as ord_pos_target,
                              column_name as col_name_target
                       FROM information_schema.columns
                       WHERE table_name = %(target_table_name)s
                        AND table_schema = %(target_schema_name)s)
                    SELECT *
                    FROM src
                    FULL OUTER JOIN tgt on src.col_name_source = tgt.col_name_target
            """,
                {
                    "source_table_name": table_mapping.source.name,
                    "source_schema_name": table_mapping.source.schema,
                    "target_table_name": table_mapping.target.name,
                    "target_schema_name": table_mapping.target.schema,
                },
            )

            results = cursor.fetchall()

            for row in results:
                if row["col_name_source"] is None:
                    diffs[table_mapping].append(
                        f"{row['col_name_target']} is missing in {table_mapping.source}"
                    )
                if row["col_name_target"] is None:
                    diffs[table_mapping].append(
                        f"{row['col_name_source']} is missing in {table_mapping.target}"
                    )
                if row["ord_pos_source"] != row["ord_pos_target"]:
                    diffs[table_mapping].append(
                        f"{row['col_name_source']} has position {row['ord_pos_source']}"
                        f" but in target it has position {row['ord_pos_target']}"
                    )
                if row["dtt_source"] != row["dtt_target"]:
                    diffs[table_mapping].append(
                        f"{row['col_name_source']} has data type {row['dtt_source']}"
                        f" but in target it has data type {row['dtt_target']}"
                    )

        return diffs

    def _calculate_columns_for_diffs(
        self, diffs: dict[TableMapping, list[str]]
    ) -> dict[TableMapping, list[str]]:
        """Determine what columns are involved in a copy operation.

        If a dataset_name is defined, we try to get the column names from the dataset.
        """
        columns_per_table_mapping = {}

        for table_mapping, _diff_check in diffs.items():

            # Try to execute copy based on explicit table names
            # if target table not already dropped.
            columns = []
            if self.dataset_name and not self.drop_target_if_unequal:

                self.log.info(
                    """Trying to do data copy based on explicit column names
                        in Amsterdam schema definition."""
                )

                dataset: DatasetSchema = dataset_schema_from_url(SCHEMA_URL, self.dataset_name)

                # Get the table_id from the full sql table name
                table_id = table_mapping.target.name.split(f"{self.dataset_name}_")[1]

                # We do not take nested/through tables into account,
                # because this can get very hairy. In that case, `columns` stays empty,
                # and table copy will be done based on all columns (*).
                try:
                    table = dataset.get_table_by_id(
                        table_id, include_nested=False, include_through=False
                    )
                except SchemaObjectNotFound:
                    continue

                for field in table.fields:
                    if field.name == "schema" or field.nm_relation is not None:
                        continue
                    columns.append(
                        to_snake_case(
                            f"{field.name}_id" if field.relation is not None else field.name
                        )
                    )
            columns_per_table_mapping[table_mapping] = columns

        return columns_per_table_mapping

    def _find_actual_table_name(
        self, cursor: Any, view_or_table_name: str
    ) -> tuple[str, str, bool]:
        """Finds out if a name is referring to a view or to regular table.

        If the view_or_table_name is just a regular table, the name is returned as-is
        and the database schema is considered to be `public`.

        Args:
            cursor: DB-API cursor
            view_or_table_name: name of a postgres db object

        Returns:
            name of database schema
            name of the table
            boolean indicating if view_or_table_name is referring to a view
        """
        cursor.execute("SELECT to_regclass(%s)", (view_or_table_name,))
        if cursor.fetchone()["to_regclass"] is None:
            return "public", view_or_table_name, False

        cursor.execute(SELECT_TABLE_FOR_VIEW_NAME, {"view_or_table_name": view_or_table_name})
        result = cursor.fetchone()
        if result is None:
            return "public", view_or_table_name, False

        ref_object = ""
        try:
            ref_object = result["ref_object"]
            schema_name, table_name = ref_object.split(".")
            return schema_name, table_name, True
        except ValueError:
            self.log.error("View '%s' references a table in the public schema", ref_object)
            raise

    def execute(self, context: dict[str, Any]) -> None:  # noqa: C901, D102
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        # Use the mixin class _assign to assign new values, if provided.
        self._assign(context)

        with closing(hook.get_conn()) as conn:
            if hook.supports_autocommit:
                self.log.debug("Setting autocommit to '%s'.", self.autocommit)
                hook.set_autocommit(conn, self.autocommit)

            with closing(conn.cursor(cursor_factory=extras.RealDictCursor)) as cursor:

                original_target_table_name = self.target_table_name

                (
                    self.target_schema_name,
                    self.target_table_name,
                    is_view,
                ) = self._find_actual_table_name(cursor, self.target_table_name)

                if is_view and self.drop_target_if_unequal:
                    raise AirflowFailException(
                        f"Dropping the target `{original_target_table_name}` "
                        "is not allowed for views."
                    )

                # Start a list to hold copy information
                table_copies: list[TableMapping] = []
                if self.source_table_name is not None and self.target_table_name is not None:
                    table_copies.extend(
                        [
                            TableMapping(
                                source=Table(
                                    name=self.source_table_name, schema=self.source_schema_name
                                ),
                                target=Table(
                                    name=self.target_table_name, schema=self.target_schema_name
                                ),
                                view_name=original_target_table_name if is_view else None,
                            ),
                        ]
                    )

                # Find the cross-tables for n-m relations, we assume they have
                # a name that start with f"{source_table_name}_"

                # the underscore must be escaped because of it's special meaning in a like
                # the exclamation mark was used as an escape character because
                # a backslash was not interpreted as an escape
                cursor.execute(
                    """
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = %(table_schema)s
                            AND tablename like %(table_name)s ESCAPE '!'
                    """,
                    {
                        "table_schema": self.source_schema_name,
                        "table_name": f"{self.source_table_name}!_%",
                    },
                )

                junction_tables = cast(
                    tuple[str, ...],
                    tuple(map(operator.itemgetter("tablename"), cursor.fetchall())),
                )

                if junction_tables:
                    self.log.info("Found the following junction tables: %r", junction_tables)
                else:
                    self.log.info("Did not find any junction tables.")

                linked_table_copies: list[TableMapping] = []
                for source_table_name in junction_tables + tuple(self.nested_db_table_names):
                    original_target_junction_table_name = source_table_name.replace(
                        TMP_TABLE_POSTFIX, ""
                    )
                    (
                        target_schema_name,
                        target_table_name,
                        junction_is_view,
                    ) = self._find_actual_table_name(cursor, original_target_junction_table_name)
                    linked_table_copies.append(
                        TableMapping(
                            source=Table(name=source_table_name, schema=self.source_schema_name),
                            target=Table(name=target_table_name, schema=target_schema_name),
                            view_name=original_target_junction_table_name
                            if junction_is_view
                            else None,
                        )
                    )

                # Check if there are structural differences between targets and sources.
                all_table_copies: list[TableMapping] = table_copies + linked_table_copies
                diff_results = self._validate_tables_equality(cursor, all_table_copies)

                for table_mapping, diff_check in diff_results.items():

                    if diff_check:

                        self.log.info(
                            "Different data structure detected between %s.%s and %s.%s!",
                            table_mapping.source.schema,
                            table_mapping.source.name,
                            table_mapping.target.schema,
                            table_mapping.target.name,
                        )
                        for index, diff_finding in enumerate(diff_check):
                            self.log.info("%s: %s", index, diff_finding)

                        if table_mapping.view_name is not None:
                            raise AirflowFailException(
                                f"Dropping {table_mapping.view_name} "
                                "is not allowed, because it is a view."
                            )

                        # Drop target table if there are differences in columns
                        # this is done regardless of drop_target_if_unequal setting
                        self.log.info(
                            "Dropping table %s.%s",
                            table_mapping.target.schema,
                            table_mapping.target.name,
                        )
                        cursor.execute(
                            sql.SQL("DROP TABLE {table_schema}.{table_name}").format(
                                table_schema=sql.Identifier(table_mapping.target.schema),
                                table_name=sql.Identifier(table_mapping.target.name),
                            )
                        )

                statements: list[Statement] = [
                    Statement(
                        sql="""
                    CREATE TABLE IF NOT EXISTS {target_schema_name}.{target_table_name}
                    (
                        LIKE {source_schema_name}.{source_table_name} INCLUDING ALL
                    )
                    """,
                        log_msg="Creating new table (if not exists) "
                        "'{target_schema_name}.{target_table_name}' "
                        "using table '{source_schema_name}.{source_table_name}' as a template.",
                    )
                ]

                if self.truncate_target:
                    statements.append(
                        Statement(
                            sql="TRUNCATE TABLE {target_schema_name}.{target_table_name} CASCADE",
                            log_msg="Truncating table '{target_schema_name}.{target_table_name}'.",
                        )
                    )

                columns_for_diffs = self._calculate_columns_for_diffs(diff_results)

                if self.copy_data:
                    statements.append(
                        Statement(
                            sql="""
                        INSERT INTO {target_schema_name}.{target_table_name}
                            ({to_columns})
                        SELECT {from_columns}
                        FROM {source_schema_name}.{source_table_name}
                        """,
                            log_msg="Copying all data from table "
                            "'{source_schema_name}.{source_table_name}' "
                            "to table '{target_schema_name}.{target_table_name}'.",
                        )
                    )

                if self.drop_source:
                    statements.append(
                        Statement(
                            sql="DROP TABLE IF EXISTS "
                            "{source_schema_name}.{source_table_name} CASCADE",
                            log_msg="Dropping table (if exists) "
                            "'{source_schema_name}.{source_table_name}'.",
                        )
                    )

                for table_mapping in itertools.chain(table_copies, linked_table_copies):

                    columns = columns_for_diffs.get(table_mapping)
                    from_columns = None
                    to_columns = None

                    if columns:
                        columns_idents = sql.SQL(", ").join([sql.Identifier(c) for c in columns])
                        from_columns = columns_idents
                        to_columns = columns_idents

                    for stmt in statements:
                        self.log.info(
                            stmt.log_msg.format(  # noqa: G001
                                source_schema_name=table_mapping.source.schema,
                                source_table_name=table_mapping.source.name,
                                target_schema_name=table_mapping.target.schema,
                                target_table_name=table_mapping.target.name,
                            )
                        )

                        # NB. schema and table cannot be combined into one `sql.Identifier`.
                        sql_args = {
                            "source_schema_name": sql.Identifier(table_mapping.source.schema),
                            "source_table_name": sql.Identifier(table_mapping.source.name),
                            "target_schema_name": sql.Identifier(table_mapping.target.schema),
                            "target_table_name": sql.Identifier(table_mapping.target.name),
                            "from_columns": from_columns,
                            "to_columns": to_columns,
                        }

                        # Unfortunately, we need some argument fiddling here, because
                        # to_columns/from_columns should be an empty string or '*'
                        # when there are no columns defined
                        # and psycopg2.sql does not seem to allow that.
                        sql_stmt = stmt.sql
                        if from_columns is None:
                            sql_stmt = sql_stmt.replace("{from_columns}", "*")
                            del sql_args["from_columns"]

                        if to_columns is None:
                            sql_stmt = sql_stmt.replace("({to_columns})", "")
                            del sql_args["to_columns"]

                        self.log.info("SQL %s; args %s", sql_stmt, sql_args)
                        cursor.execute(sql.SQL(sql_stmt).format(**sql_args))

            if not hook.get_autocommit(conn):
                self.log.debug("Committing transaction.")
                conn.commit()
        for output in hook.conn.notices:
            self.log.info(output)
