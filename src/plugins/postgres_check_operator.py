from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.operators.check_operator import (
    CheckOperator,
    ValueCheckOperator,
)
from airflow.utils.decorators import apply_defaults


class PostgresCheckOperator(CheckOperator):
    """
    Performs checks against Postgres. The ``PostgresCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
    the count ``== 0``. You can craft much more complex query that could,
    for instance, check that the table has the same number of rows as
    the source table upstream, or that the count of today's partition is
    greater than yesterday's partition, or that a set of metrics are less
    than 3 standard deviation for the 7 day average.

    This operator can be used as a data quality check in your pipeline, and
    depending on where you put it in your DAG, you have the choice to
    stop the critical path, preventing from
    publishing dubious data, or on the side and receive email alerts
    without stopping the progress of the DAG.

    :param sql: the sql to be executed
    :type sql: str
    :param postgres_conn_id: reference to the Postgres database
    :type postgres_conn_id: str
    """

    template_fields = ("sql",)
    template_ext = (".sql",)

    @apply_defaults
    def __init__(self, sql, postgres_conn_id="postgres_default", *args, **kwargs):
        super(PostgresCheckOperator, self).__init__(sql=sql, *args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class PostgresValueCheckOneOperator(ValueCheckOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: str
    """

    template_fields = (
        "sql",
        "pass_value",
    )
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        pass_value,
        tolerance=None,
        postgres_conn_id="postgres_default",
        *args,
        **kwargs,
    ):
        super(PostgresValueCheckOperator, self).__init__(
            sql=sql, pass_value=pass_value, tolerance=tolerance, *args, **kwargs
        )
        self.postgres_conn_id = postgres_conn_id

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)


class PostgresValueCheckOperator(BaseOperator):
    """
    Performs a simple value check using sql code.

    :param sql: the sql to be executed
    :type sql: str
    :param pass_value: the sql to be executed
    :type pass_value: Any
    :param result_checker: function (if not None) to be used to compare result with pass_value
    :type result_checker: Function
    """

    template_fields = (
        "sql",
        "pass_value",
    )
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        sql,
        pass_value,
        postgres_conn_id="postgres_default",
        result_checker=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.pass_value = pass_value
        self.postgres_conn_id = postgres_conn_id
        self.result_checker = result_checker

    def execute(self, context=None):
        self.log.info("Executing SQL value check: %s", self.sql)
        records = self.get_db_hook().get_records(self.sql)

        if not records:
            raise AirflowException("The query returned None")

        checker = self.result_checker or (
            lambda records, pass_value: records == pass_value
        )
        if not checker(records, self.pass_value):
            raise AirflowException(f"{records} does not match {self.pass_value}")

    def get_db_hook(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)
