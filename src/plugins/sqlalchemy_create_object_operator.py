import json
import re
import urllib.request
from re import Pattern
from typing import Any, Callable, Dict, Optional, Union
from airflow.models import XCOM_RETURN_KEY
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from common import SCHEMA_INDEX_FILE_OBJECTSTORE
from environs import Env
from more_ds.network.url import URL
from schematools.cli import _get_engine
from schematools.importer.base import BaseImporter
from schematools.types import DatasetSchema, SchemaType
from schematools.utils import schema_fetch_url_file
from xcom_attr_assigner_mixin import XComAttrAssignerMixin

env = Env()
# split is used to remove url params, if exists.
# for database connection the url params must be omitted.
DEFAULT_DB_CONN = env.str("AIRFLOW_CONN_POSTGRES_DEFAULT").split("?")[0]
SCHEMA_URL = URL(env("SCHEMA_URL"))


class SqlAlchemyCreateObjectOperator(BaseOperator, XComAttrAssignerMixin):
    """Create PostgreSQL objects based on an Amsterdam schema using SQLAlchemy

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
        data_table_name: Optional[Union[str, Pattern]] = re.compile(r".*"),
        data_schema_version: Optional[str] = None,
        db_conn: str = DEFAULT_DB_CONN,
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
            data_table_name: Table(s) to create PostgreSQL objects for. This can either be a
                string of the format ``<prefix>_<scheme_table_name>``. Where ``prefix`` generally
                is the dataset name/abbreviation and ``schema_table_name`` the name of the table
                as specified in the schema. Or it can be a compiled regular expression. The latter
                is mostly useful if you want to create PostgreSQL objects for all tables in a
                given schema. This is also the default value (all tables).
            db_conn: DB connection URL.
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

        self.data_schema_version = data_schema_version
        self.db_conn = db_conn
        self.ind_table = ind_table
        self.ind_extra_index = ind_extra_index
        self.xcom_task_ids = xcom_task_ids
        self.xcom_attr_assigner = xcom_attr_assigner
        self.xcom_key = xcom_key
        assert (data_table_name is not None) ^ (
            xcom_task_ids is not None
        ), "Either data_table_name or xcom_task_ids should be provided."

    def get_schema(self, data_schema_version: Optional[str]) -> Any:
        """If a schema version is specified, i.e. 2.1.0, the version number
        is used to determine path to schema specification.
        Else it defaults to current schema (latest)

        Args:
            data_schema_version: Name of a specific schema version
        Raises KeyError:
            When schema name cannot be located in the index.json
                file in the schema objectstore.
        Returns:
            Path to schema
        """
        if data_schema_version:
            return SCHEMA_URL / self.data_schema_version / self.data_schema_name

        schema_index_file = SCHEMA_URL / SCHEMA_INDEX_FILE_OBJECTSTORE
        request = urllib.request.Request(schema_index_file)
        try:
            with urllib.request.urlopen(request) as response:
                schema_paths = json.loads(response.read().decode("utf-8"))
                return SCHEMA_URL / schema_paths[self.data_schema_name]
        except KeyError as ex:
            raise KeyError(
                f"""Failed to locate path for {str(ex)}.
                Please check `index.json` in schema objectstore."""
            ) from ex

    def execute(self, context: Dict[str, Any]) -> None:
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
            self.data_table_name = re.compile(self.data_table_name)
        else:
            self.data_table_name = self.data_table_name

        data_schema_url = self.get_schema(self.data_schema_version)
        data = schema_fetch_url_file(data_schema_url)
        engine = _get_engine(self.db_conn)
        parent_schema = SchemaType(data)
        dataset_schema = DatasetSchema(parent_schema)
        importer = BaseImporter(dataset_schema, engine)
        self.log.info(
            "data_schema_url='%s', engine='%s', ind_table='%s', ind_extra_index='%s'.",
            data_schema_url,
            engine,
            self.ind_table,
            self.ind_extra_index,
        )

        if self.data_table_name is None:
            return

        for table in dataset_schema.tables:
            self.log.info("Considering table '%s'.", table.name)
            cur_table = f"{self.data_schema_name}_{table.name}"

            if re.fullmatch(self.data_table_name, cur_table):
                self.log.info("Generating PostgreSQL objects for table '%s'.", table.name)
                importer.generate_db_objects(
                    table.id,
                    ind_tables=self.ind_table,
                    ind_extra_index=self.ind_extra_index,
                )
            else:
                self.log.info("Skipping table '%s' (reason: no match).", table.name)
                continue
