import operator
from contextlib import closing
from typing import Any, Dict, Tuple

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2 import sql


class PostgresCreateTablesLikeOperator(BaseOperator):
    """Create tables using existing tables as a template.

    The tables to use as a template are selected by means of a POSIX regex, passed in as the
    ``table_name_regex``. The newly created tables will be named by prepending a ``prefix`` to
    the template table names.

    .. note:: It will not automatically insert an underscore (``"_"``) between the``prefix`` and
       the name of the template table. If you want one, you should make it part of the ``prefix.

    Should the newly, to be created, tables already exist, it will skip the attempt and not error
    out. It will however notify you of this fact by issuing a logging statement.
    """

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        table_name_regex: str,
        prefix: str = "tmp_",
        postgres_conn_id: str = "postgres_default",
        autocommit: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgresCreateTablesLikeOperator.

        Args:
            table_name_regex: A POSIX regular expression to select tables that should be used
                as a template to create other tables. A common regex is to use a DAG/dataset
                prefix. For example ``"^bbga_.*"`` (mind the ^)
            prefix: The prefix to use for the newly created tables. The newly created tables will
                be named <prefix> + <existing_table_name>.
            postgres_conn_id: The PostgreSQL connection id.
            autocommit: What to set the connection's autocommit setting to before executing
                the query.
            *args:
            **kwargs:
        """
        super().__init__(*args, **kwargs)
        self.table_name_regex = table_name_regex
        self.prefix = prefix
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit

    def execute(self, context: Dict[str, Any]) -> Tuple[str, ...]:
        """Create tables using existing tables as a template.

        Examples:
            Create new tables with a 'tmp_' prefix for each of the existing tables
            that have their name start with 'bbga_':

            create_tables_like = PostgresCreateTablesLikeOperator(
                task_id="create_tables_like", table_name_regex=f"^bbga_.*"
            )

        Args:
            context: execution context

        Returns:
            Names of newly created tables.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with closing(hook.get_conn()) as conn:
            if hook.supports_autocommit:
                self.log.debug("Setting autocommit to '%s'.", self.autocommit)
                hook.set_autocommit(conn, self.autocommit)
            with closing(conn.cursor()) as cursor:
                cursor.execute(
                    """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                      AND tablename ~ %(table_name_regex)s""",
                    dict(table_name_regex=self.table_name_regex),
                )
                template_tables = tuple(map(operator.itemgetter("tablename"), cursor.fetchall()))
                self.log.info(
                    "Creating new tables with prefix='%s' using these tables as templates: '%s'.",
                    self.prefix,
                    ", ".join(template_tables),
                )
                new_tables = tuple(map(lambda tt: f"{self.prefix}{tt}", template_tables))
                for new_table, template_table in zip(new_tables, template_tables):
                    cursor.execute(
                        sql.SQL(
                            """
                            CREATE TABLE IF NOT EXISTS {new_table}
                            (
                                LIKE {template_table} INCLUDING ALL
                            )"""
                        ).format(
                            new_table=sql.Identifier(new_table),
                            template_table=sql.Identifier(template_table),
                        )
                    )
                    self.log.info("Created table '%s'.", new_table)
            if not hook.get_autocommit(conn):
                self.log.debug("Committing transaction.")
                conn.commit()
        # In case the tables we intended to create already existed, PostgreSQL will issue notices
        # by virtue of having used `IF NOT EXISTS`.
        for output in hook.conn.notices:
            self.log.info(output)
        return new_tables
