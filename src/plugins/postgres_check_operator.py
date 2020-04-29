import operator
import re

from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.check_operator import CheckOperator, ValueCheckOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.decorators import apply_defaults

RE_SAFE_IDENTIFIER = re.compile(r"\A[a-z][a-z0-9_\-]+\Z", re.I)


class PostgresCheckOperator(CheckOperator):
    """The output of a single query is compased against a row."""

    template_fields = ("sql",)
    template_ext = (".sql",)

    def __init__(self, sql, parameters=(), conn_id="postgres_default", **kwargs):
        super().__init__(sql=sql, conn_id=conn_id, **kwargs)
        self.parameters = parameters

    def execute(self, context=None):
        """Overwritten to support SQL 'parameters' for safe SQL escaping."""
        self.log.info(
            "Executing SQL check: %s with %s", self.sql, repr(self.parameters)
        )
        records = self.get_db_hook().get_first(self.sql, self.parameters)

        if records != [1]:  # Avoid unneeded "SELECT 1" in logs
            self.log.info("Record: %s", records)
        if not records:
            raise AirflowException("Check failed, query returned no records")
        elif not all([bool(r) for r in records]):
            raise AirflowException(
                "Test failed.\nQuery:\n{query}\nResults:\n{records!s}".format(
                    query=self.sql, records=records
                )
            )

        self.log.info("Success.")

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.conn_id)


class PostgresCountCheckOperator(PostgresCheckOperator):
    """Check the number of records in a table."""

    def __init__(
        self,
        table_name,
        min_count,
        postgres_conn_id="postgres_default",
        task_id="check_count",
        **kwargs,
    ):
        check_safe_name(table_name)
        super().__init__(
            sql=f'SELECT COUNT(*) >= %s FROM "{table_name}"',
            parameters=(min_count,),  # params is jinja, parameters == sql!
            conn_id=postgres_conn_id,
            task_id=task_id,
            **kwargs,
        )


class PostgresGeometryTypeCheckOperator(PostgresCheckOperator):
    """Check the geometry type of a table."""

    def __init__(
        self,
        table_name,
        geometry_type,
        geometry_column="geometry",
        postgres_conn_id="postgres_default",
        task_id="check_geo",
        **kwargs,
    ):
        check_safe_name(table_name)
        check_safe_name(geometry_column)
        super().__init__(
            # using GeometryType() returns "POINT", ST_GeometryType() returns 'ST_Point'
            sql=(
                f"SELECT 1 WHERE NOT EXISTS ("
                f'SELECT FROM "{table_name}" WHERE'
                f' "{geometry_column}" IS null '
                f' OR NOT ST_IsValid("{geometry_column}") '
                f' OR GeometryType("{geometry_column}") != %s'
                ")"
            ),
            parameters=(geometry_type.upper(),),  # params is jinja, parameters == sql!
            conn_id=postgres_conn_id,
            task_id=task_id,
            **kwargs,
        )


class PostgresValueCheckOperator(ValueCheckOperator):
    """Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: str
    :param conn_id: reference to the Postgres database
    :type conn_id: str
    :param result_checker: function (if not None) to be used to compare result with pass_value
    :type result_checker: Function
    """

    def __init__(
        self,
        sql,
        pass_value,
        parameters=(),
        conn_id="postgres_default",
        result_checker=None,
        *args,
        **kwargs,
    ):
        super().__init__(sql=sql, pass_value=pass_value, conn_id=conn_id, **kwargs)
        self.parameters = parameters
        self.pass_value = pass_value  # avoid str() cast
        self.result_checker = result_checker

    def execute(self, context=None):
        self.log.info(
            "Executing SQL value check: %s with %r", self.sql, self.parameters
        )
        records = self.get_db_hook().get_records(self.sql, self.parameters)

        if not records:
            raise AirflowException("The query returned None")

        checker = self.result_checker or operator.eq
        if not checker(records, self.pass_value):
            raise AirflowException(f"{records} != {self.pass_value}")

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.conn_id)


class PostgresColumnNamesCheckOperator(PostgresValueCheckOperator):
    """Check whether the expected column names are present."""

    def __init__(
        self,
        table_name,
        column_names,
        conn_id="postgres_default",
        task_id="check_column_names",
        **kwargs,
    ):
        check_safe_name(table_name)
        super().__init__(
            sql=(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = 'public' AND table_name = %s"
                " ORDER BY column_name"
            ),
            parameters=(table_name,),  # params is jinja, parameters == sql!
            pass_value=[
                [col] for col in sorted(column_names)
            ],  # each col is in a separate row
            conn_id=conn_id,
            task_id=task_id,
            **kwargs,
        )


class PostgresTableRenameOperator(PostgresOperator):
    """Rename a table"""

    @apply_defaults
    def __init__(
        self,
        old_table_name: str,
        new_table_name: str,
        postgres_conn_id="postgres_default",
        task_id="rename_table",
        **kwargs,
    ):
        check_safe_name(old_table_name)
        check_safe_name(new_table_name)
        super().__init__(
            task_id=task_id, sql=[], postgres_conn_id=postgres_conn_id, **kwargs
        )
        self.old_table_name = old_table_name
        self.new_table_name = new_table_name

    def execute(self, context):
        # First get all index names, so it's known which indices to rename
        hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        with hook.get_cursor() as cursor:
            cursor.execute(
                "SELECT indexname FROM pg_indexes"
                " WHERE schemaname = 'public' AND indexname like %s"
                " ORDER BY indexname;",
                (f"%{self.old_table_name}%",),
            )
            indexes = list(cursor.fetchall())

        index_renames = [
            (
                row["indexname"],
                re.sub(
                    pattern=_get_complete_word_pattern(self.old_table_name),
                    repl=self.new_table_name,
                    string=row["indexname"],
                    count=1,
                ),
            )
            for row in indexes
        ]

        backup_table = f"{self.new_table_name}_old"

        # Define the SQL to execute by the super class.
        # This supports executing multiple statements in a single transaction:
        self.sql = [
            f"ALTER TABLE IF EXISTS {self.new_table_name} RENAME TO {backup_table}",
            f"ALTER TABLE {self.old_table_name} RENAME TO {self.new_table_name}",
            f"DROP TABLE IF EXISTS {backup_table}",
        ] + [
            f"ALTER INDEX {old_index} RENAME TO {new_index}"
            for old_index, new_index in index_renames
        ]

        return super().execute(context)


def check_safe_name(sql_identifier):
    """Check whether an identifier can safely be used inside an SQL statement.
    This avoids causing SQL injections when the identifier ever becomes user-input.
    """
    if not RE_SAFE_IDENTIFIER.match(sql_identifier):
        raise RuntimeError(f"Unsafe input used as table/field-name: {sql_identifier}")


def _get_complete_word_pattern(word):
    """Create a search pattern that looks for whole words only."""
    return r"{word}".format(word=re.escape(word))
