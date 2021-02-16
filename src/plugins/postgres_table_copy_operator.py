import itertools
import operator
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowFailException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2 import sql


@dataclass
class TableMapping:
    """Simple mapping from source table to target table."""

    source: str
    target: str


@dataclass
class Statement:
    """Simple grouping of SQL statement with associated logging message."""

    sql: str
    log_msg: str


class PostgresTableCopyOperator(BaseOperator):
    """Copy table to another table, create target table structure if needed from source.

    The premise here is that the source table is a temporary table and the target table is the
    final result. In copying, it not only copies the the table definition, but optionally also
    the data. To prevent duplicate key errors it has has the option to truncate the target table
    first. This is necessary if the target already exists and contains data. Lastly
    it has the ability to drop the source (=temporary) table.

    .. note:: A strange assumption is being made by this operator. It does require you to specify
       both source (temporary) and target tables. However for finding any junction tables it
       assumes that all these junction tables have a "_new" postfix. Makes you wonder why that
       assumption is not made for original source table? Or even better, why that wasn't made
       configurable.
    """

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        source_table_name: str,
        target_table_name: str,
        truncate_target: bool = True,
        copy_data: bool = True,
        drop_source: bool = True,
        task_id: str = "copy_table",
        postgres_conn_id: str = "postgres_default",
        database: Optional[str] = None,
        autocommit: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgresTableCopyOperator.

        Args:
            source_table_name: Name of the temporary table that needs to be copied.
            target_table_name: Name of the table that needs to be created and data copied to.
            truncate_target: Whether the target table should be truncated before being copied to.
            copy_data: Whether to copied data. If set to False only the table definition is copied.
            drop_source: Whether to drop the source table after the copy process.
            task_id: Task ID
            postgres_conn_id: The PostgreSQL connection id.
            database: Name of the databas to use (if different from database from connection id)
            autocommit: What to set the connection's autocommit setting to.
            *args:
            **kwargs:
        """
        super().__init__(*args, task_id=task_id, **kwargs)
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.truncate_target = truncate_target
        self.copy_data = copy_data
        self.drop_source = drop_source
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.autocommit = autocommit

        if not copy_data and drop_source:
            raise AirflowFailException(
                "Configuration error: source data will not be copied, "
                "even though source table will be dropped."
            )

    def execute(self, context: Dict[str, Any]) -> None:  # noqa: C901
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        with closing(hook.get_conn()) as conn:
            if hook.supports_autocommit:
                self.log.debug("Setting autocommit to '%s'.", self.autocommit)
                hook.set_autocommit(conn, self.autocommit)

            # Start a list to hold copy information
            table_copies: List[TableMapping] = [
                TableMapping(source=self.source_table_name, target=self.target_table_name),
            ]

            # Find the cross-tables for n-m relations, we assume they have
            # a name that start with f"{source_table_name}_"

            with closing(conn.cursor()) as cursor:
                # the underscore must be escaped because of it's special meaning in a like
                # the exclamation mark was used as an escape chacater because
                # a backslash was not interpreted as an escape
                cursor.execute(
                    """
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public' AND tablename like %(table_name)s ESCAPE '!'
                    """,
                    dict(table_name=f"{self.source_table_name}!_%"),
                )

                junction_tables = tuple(map(operator.itemgetter("tablename"), cursor.fetchall()))
                if junction_tables:
                    self.log.info(
                        f"Found the following junction tables: '{', '.join(junction_tables)}'."
                    )
                else:
                    self.log.info("Did not found any junction tables.")

                junction_table_copies: List[TableMapping] = []
                for source_table_name in junction_tables:
                    target_table_name = source_table_name.replace("_new", "")
                    junction_table_copies.append(
                        TableMapping(source_table_name, target_table_name)
                    )

                statements: List[Statement] = [
                    Statement(
                        sql="""
                    CREATE TABLE IF NOT EXISTS {target_table_name}
                    (
                        LIKE {source_table_name} INCLUDING CONSTRAINTS INCLUDING INDEXES
                    )
                    """,
                        log_msg="Creating new table '{target_table_name}' "
                        "using table '{source_table_name}' as a template.",
                    )
                ]
                if self.truncate_target:
                    statements.append(
                        Statement(
                            sql="TRUNCATE TABLE {target_table_name} CASCADE",
                            log_msg="Truncating table '{target_table_name}'.",
                        )
                    )
                if self.copy_data:
                    statements.append(
                        Statement(
                            sql="""
                        INSERT INTO {target_table_name}
                        SELECT *
                        FROM {source_table_name}
                        """,
                            log_msg="Copying all data from table '{source_table_name}' "
                            "to table '{target_table_name}'.",
                        )
                    )
                if self.drop_source:
                    statements.append(
                        Statement(
                            sql="DROP TABLE IF EXISTS {source_table_name} CASCADE",
                            log_msg="Dropping table '{source_table_name}'.",
                        )
                    )
                for table_mapping in itertools.chain(table_copies, junction_table_copies):
                    for stmt in statements:
                        self.log.info(
                            stmt.log_msg.format(
                                source_table_name=table_mapping.source,
                                target_table_name=table_mapping.target,
                            )
                        )
                        cursor.execute(
                            sql.SQL(stmt.sql).format(
                                source_table_name=sql.Identifier(table_mapping.source),
                                target_table_name=sql.Identifier(table_mapping.target),
                            )
                        )
            if not hook.get_autocommit(conn):
                self.log.debug("Committing transaction.")
                conn.commit()
        for output in hook.conn.notices:
            self.log.info(output)
