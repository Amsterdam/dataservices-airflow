import operator
from dataclasses import dataclass
from functools import partial
from string import Template
from typing import Any, Callable, ClassVar, Final, Iterable, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.taskinstance import Context
from airflow.operators.sql import SQLCheckOperator, SQLValueCheckOperator
from airflow.utils.decorators import apply_defaults
from check_helpers import check_safe_name, make_params
from postgres_on_azure_hook import PostgresOnAzureHook


@dataclass
class Check:
    """Object definition of Checker."""
    check_id: str
    sql: str
    result_fetcher: Callable
    pass_value: Any
    template_fields: ClassVar = ["sql"]
    params: dict[str, str] = None
    parameters: list[str] = None
    result_checker: Callable = None


def record_by_name(colname, records):
    """Show column value."""
    return records[0][colname]


def flattened_records(records):
    """Helper to iterate records as a list."""
    return [r[0] for r in records]


def first_flattened_record(records):
    return flattened_records(records)[0]


def flattened_records_as_set(records):
    return {r[0] for r in records}


class CheckFactory:
    def __init__(self, sql, result_fetcher=lambda x: x):
        self.sql = sql
        self.result_fetcher = result_fetcher

    def make_check(self, check_id, pass_value, params=None, parameters=None, result_checker=None):
        """We use the string.Template interpolation here, because
        that does not interfere with the jinja2 templating"""
        return Check(
            check_id,
            Template(self.sql).safe_substitute(check_id=check_id),
            self.result_fetcher,
            pass_value,
            params,
            parameters,
            result_checker,
        )


COUNT_CHECK: Final = CheckFactory(
    "SELECT COUNT(*) AS count FROM {{ params['$check_id'].table_name }}",
    result_fetcher=partial(record_by_name, "count"),
)

COLNAMES_CHECK: Final = CheckFactory(
    """
    SELECT column_name FROM information_schema.columns
     WHERE table_schema = %s AND table_name = %s
     ORDER BY column_name
""",
    result_fetcher=flattened_records_as_set,
)

GEO_CHECK: Final = CheckFactory(
    """
  {% set lparams = params['$check_id'] %}
  {% set geo_column = lparams.geo_column|default("geometry", true) %}
  SELECT 1 WHERE NOT EXISTS (
      SELECT FROM {{ lparams.table_name }} WHERE
        {{ geo_column }} IS null
        {% if lparams.check_valid|default(true) %} OR ST_IsValid({{ geo_column }}) = false {% endif %}
          OR GeometryType({{ geo_column }})
        {% if lparams.geotype is string %}
          <> '{{ lparams.geotype }}'
        {% else %}
          NOT IN ({{ lparams.geotype | map('quote') | join(", ") }})
        {% endif %}
      )
""",
    result_fetcher=first_flattened_record,
)


class PostgresMultiCheckOperator(BaseOperator):
    """This operator can be used to fire a number of checks at once.
    This is more efficient than firing up all checks separately.
    There is one caveat. Because we want to use the efficient and
    lazily evaluated jinja templating, the template parameters have
    to be collected into one dict, because Airflow does the parameter
    interpolation only once. So, when params are used, they have to
    be collected with the 'make_params' function and fed into the
    operator constructor.
    """

    # We use the possibilty to have nested template fields here
    template_fields: Iterable[str] = ["checks"]

    def __init__(
        self,
        dataset_name: Optional[str] = None,
        postgres_conn_id="postgres_default",
        checks=[],
        *args,
        **kwargs,
    ):
        """Initialize.

        Args:
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.

        Executes:
            (SQL) data checks on database.
        """
        self.dataset_name = dataset_name
        self.postgres_conn_id = postgres_conn_id
        self.checks = checks
        self.params = kwargs.get("params", {}) | make_params(self.checks)
        # Since Airflow 2.2.0 the super class will serialize params dict for
        # a DAG/Task by the `private` method `_serialize_params_dict`. However
        # because the multichecker is using nested params, the self.params (after
        # the function `make_params` has `dictionary-ized` the params) has to
        # fed explicitly to the `params` parameter of Super.
        super().__init__(params=self.params, *args, **kwargs)

    def execute(self, context: Context) -> None:
        """Execute instance."""

        hook = self.get_db_hook(context)

        for checker in self.checks:
            self.log.info("Executing SQL check: %s with %s", checker.sql, repr(checker.parameters))
            records = hook.get_records(checker.sql, checker.parameters)

            if not records:
                raise AirflowException("The query returned None")

            checker_function = checker.result_checker or operator.eq
            if not checker_function(checker.result_fetcher(records), checker.pass_value):
                raise AirflowException(f"{records} != {checker.pass_value}")

    def get_db_hook(self, context: Context) -> None:
        """Return database connection hook."""
        return PostgresOnAzureHook(
            dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id
        )


class PostgresCheckOperator(SQLCheckOperator):
    """The output of a single query is compased against a row."""

    template_fields = ("sql",)
    template_ext = (".sql",)

    def __init__(
        self,
        sql,
        parameters=(),
        dataset_name: Optional[str] = None,
        conn_id="postgres_default",
        **kwargs,
    ):
        super().__init__(sql=sql, conn_id=conn_id, **kwargs)
        self.parameters = parameters
        self.dataset_name = dataset_name

    def execute(self, context: Context):
        """Overwritten to support SQL 'parameters' for safe SQL escaping."""
        self.log.info("Executing SQL check: %s with %s", self.sql, repr(self.parameters))
        records = self.get_db_hook(context=context).get_first(self.sql, self.parameters)

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

    def get_db_hook(self, context):
        return PostgresOnAzureHook(
            dataset_name=self.dataset_name, context=context, postgres_conn_id=self.conn_id
        )


class PostgresCountCheckOperator(PostgresCheckOperator):
    """Check the number of records in a table."""

    def __init__(
        self,
        table_name,
        min_count,
        dataset_name: Optional[str] = None,
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
            dataset_name=dataset_name ** kwargs,
        )


class PostgresGeometryTypeCheckOperator(PostgresCheckOperator):
    """Check the geometry type of a table."""

    def __init__(
        self,
        table_name,
        geometry_type,
        geometry_column="geometry",
        dataset_name: Optional[str] = None,
        postgres_conn_id="postgres_default",
        task_id="check_geo",
        **kwargs,
    ):
        check_safe_name(table_name)
        check_safe_name(geometry_column)
        super().__init__(
            # using GeometryType() returns "POINT", ST_GeometryType() returns 'ST_Point'
            sql=(
                "SELECT 1 WHERE NOT EXISTS ("
                f'SELECT FROM "{table_name}" WHERE'
                f' "{geometry_column}" IS null '
                f' OR NOT ST_IsValid("{geometry_column}") '
                f' OR GeometryType("{geometry_column}") != %s'
                ")"
            ),
            parameters=(geometry_type.upper(),),  # params is jinja, parameters == sql!
            conn_id=postgres_conn_id,
            task_id=task_id,
            dataset_name=dataset_name,
            **kwargs,
        )


class PostgresValueCheckOperator(SQLValueCheckOperator):
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
        dataset_name: Optional[str] = None,
        conn_id="postgres_default",
        result_checker=None,
        *args,
        **kwargs,
    ):
        super().__init__(sql=sql, pass_value=pass_value, conn_id=conn_id, **kwargs)
        self.parameters = parameters
        self.pass_value = pass_value  # avoid str() cast
        self.result_checker = result_checker
        self.dataset_name = dataset_name

    def execute(self, context: Context):
        self.log.info("Executing SQL value check: %s with %r", self.sql, self.parameters)
        records = self.get_db_hook(context=context).get_records(self.sql, self.parameters)

        if not records:
            raise AirflowException("The query returned None")

        checker = self.result_checker or operator.eq
        if not checker(records, self.pass_value):
            raise AirflowException(f"{records} != {self.pass_value}")

    def get_db_hook(self, context):
        return PostgresOnAzureHook(
            dataset_name=self.dataset_name, context=context, postgres_conn_id=self.postgres_conn_id
        )


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
            pass_value=[[col] for col in sorted(column_names)],  # each col is in a separate row
            conn_id=conn_id,
            task_id=task_id,
            **kwargs,
        )
