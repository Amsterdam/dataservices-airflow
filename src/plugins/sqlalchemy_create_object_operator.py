import os
import re
from re import Pattern
from typing import Any, Callable, Final, Optional, Union

from airflow.models import XCOM_RETURN_KEY
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from common.db import define_dataset_name_for_azure_dbuser, pg_params
from environs import Env
from more_ds.network.url import URL
from schematools.cli import _get_engine
from schematools.importer.base import BaseImporter
from schematools.utils import dataset_schema_from_url, to_snake_case
from xcom_attr_assigner_mixin import XComAttrAssignerMixin

env = Env()
SCHEMA_URL: Final = URL(env("SCHEMA_URL"))
MATCH_ALL: Final = re.compile(r".*")


class SqlAlchemyCreateObjectOperator(BaseOperator, XComAttrAssignerMixin):
    """Create PostgreSQL objects based on an Amsterdam schema using SQLAlchemy.

    This operator takes a JSON data schema definition and DB connection to create the specified
    tables and/or an index. The latter is based on the Identifier object in the data JSON schema
    i.e. "identifier": ["identificatie", "volgnummer"] which is common for temporal data. And on
    the Relation specification in the schema where "many-to-many" intersection tables can be
    defined. Note: The table creation alone leads to the creation of a PK which is already
    indexed by default. Note2: A geometry column type, when specified in the JSON schema,
    also defaults to an index in the database.
    """

    template_fields = [
        "data_table_name",
    ]

    @apply_defaults  # type: ignore [misc]
    def __init__(
        self,
        data_schema_name: str,
        dataset_name: Optional[str] = None,
        data_table_name: Optional[Union[str, Pattern]] = MATCH_ALL,
        pg_schema: Optional[str] = None,
        db_table_name: Optional[str] = None,
        ind_table: bool = True,
        ind_extra_index: bool = True,
        xcom_task_ids: Optional[str] = None,
        xcom_attr_assigner: Callable[[Any, Any], None] = lambda o, x: None,
        xcom_key: str = XCOM_RETURN_KEY,
        *args: Any,
        **kwargs: Any,
    ):
        """Initialize SqlAlchemyCreateObjectOperator.

        Args:
            data_schema_name: Name of the schema to derive PostgreSQL objects from.
            dataset_name: Name of the dataset as known in the Amsterdam schema.
                Since the DAG name can be different from the dataset name, the latter
                can be explicity given. Only applicable for Azure referentie db connection.
                Defaults to None. If None, it will use the execution context to get the
                DAG id as surrogate. Assuming that the DAG id equals the dataset name
                as defined in Amsterdam schema.
            data_table_name: Table(s) to create PostgreSQL objects for. This can either be a
                string of the format ``<prefix>_<scheme_table_name>``. Where ``prefix`` generally
                is the dataset name/abbreviation and ``schema_table_name`` the name of the table
                as specified in the schema. Or it can be a compiled regular expression. The latter
                is mostly useful if you want to create PostgreSQL objects for all tables in a
                given schema. This is also the default value (all tables).
            pg_schema: Defines DB schema for the connection. If NONE it defaults to 'public'.
            db_table_name: Defines the table name to create. This can be different from the table
                name as defined in the data schema i.e. [table_name]_new
            ind_table: Whether to creates indices (as specified in the schema).
            ind_extra_index: Whether to created additional indices.
            xcom_task_ids: The id of the task that is providing the xcom info.
            xcom_attr_assigner: Callable tha can be provided to assign new values
                to object attributes.
            xcom_key: Key use to grab the xcom info, defaults to the airflow
                default `return_value`.
            *args:
            **kwargs:
        """
        super().__init__(*args, **kwargs)
        self.data_schema_name = data_schema_name
        self.data_table_name = data_table_name
        self.dataset_name = dataset_name
        self.pg_schema = pg_schema
        self.db_table_name = db_table_name
        self.ind_table = ind_table
        self.ind_extra_index = ind_extra_index
        self.xcom_task_ids = xcom_task_ids
        self.xcom_attr_assigner = xcom_attr_assigner
        self.xcom_key = xcom_key
        assert (data_table_name is not None) ^ (
            xcom_task_ids is not None
        ), "Either data_table_name or xcom_task_ids should be provided."

    def execute(self, context: dict[str, Any]) -> None:
        """Executes the ``generate_db_object`` method from schema-tools.

        Which leads to the creation of tables and/or an index on the identifier (as specified in
        the data JSON schema). By default both tables and the identifier and 'many-to-many
        table' indexes are created. By setting the boolean indicators in the method parameters,
        tables or an identifier index (per table) can be created.
        """
        # Use the mixin class _assign to assign new values, if provided.
        # This needs to operate first, it can change the data_table_name.
        self._assign(context)

        # NB. data_table_name could have been changed because of xcom info
        if isinstance(self.data_table_name, str):
            self.data_table_name = re.compile(re.escape(self.data_table_name))
        else:
            self.data_table_name = self.data_table_name

        # If Azure
        # To cope with a different logic for defining the Azure referentie db user.
        # If CloudVPS is not used anymore, then this extra route can be removed.
        if os.environ.get("AZURE_TENANT_ID") is not None:
            self.dataset_name = define_dataset_name_for_azure_dbuser(
                dataset_name=self.dataset_name, context=context
            )

        # chop off -X and remove all trailing spaces
        # for database connection extra params must be omitted.
        default_db_conn = pg_params(dataset_name=self.dataset_name)

        # setup the database schema for the database connection
        kwargs = {"pg_schemas": [self.pg_schema]} if self.pg_schema is not None else {}
        engine = _get_engine(default_db_conn, **kwargs)

        dataset_schema = dataset_schema_from_url(
            SCHEMA_URL, self.data_schema_name, prefetch_related=True
        )

        importer = BaseImporter(dataset_schema, engine, logger=self.log)
        self.log.info(
            "schema_name='%s', engine='%s', db_schema='%s', ind_table='%s', ind_extra_index='%s'.",
            self.data_schema_name,
            engine,
            self.pg_schema if self.pg_schema else "public",
            self.ind_table,
            self.ind_extra_index,
        )

        if self.data_table_name is None:
            return

        for table in dataset_schema.tables:
            self.log.info("Considering table '%s'.", table.name)
            cur_table = f"{self.data_schema_name}_{table.name}"

            if re.fullmatch(self.data_table_name, to_snake_case(cur_table)):
                self.log.info(
                    "Generating PostgreSQL objects for table: '%s' in DB schema: '%s'",
                    table.name,
                    self.pg_schema if self.pg_schema else "public",
                )
                importer.generate_db_objects(
                    table.id,
                    db_table_name=self.db_table_name,
                    db_schema_name=self.pg_schema,
                    ind_tables=self.ind_table,
                    ind_extra_index=self.ind_extra_index,
                )
            else:
                self.log.info("Skipping table '%s' (reason: no match).", table.name)
                continue
