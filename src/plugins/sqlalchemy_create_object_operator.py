import re
from re import Pattern
from typing import Union

from airflow.models.baseoperator import BaseOperator
from environs import Env
from schematools.cli import _get_engine
from schematools.importer.base import BaseImporter
from schematools.types import DatasetSchema, SchemaType
from schematools.utils import schema_fetch_url_file

from common.url import URL

env = Env()
# split is used to remove url params, if exists.
# for database connection the url params must be omitted.
DEFAULT_DB_CONN = env("AIRFLOW_CONN_POSTGRES_DEFAULT").split("?")[0]
SCHEMA_URL = URL(env("SCHEMA_URL"))


class SqlAlchemyCreateObjectOperator(BaseOperator):
    """Create PostgreSQL objects based on an Amsterdam schema using SQLAlchemy

    This operator takes a JSON data schema definition and DB connection to create the specified
    tables and/or an index. The latter is based on the Identifier object in the data JSON schema
    i.e. "identifier": ["identificatie", "volgnummer"] which is common for temporal data. And on
    the Relation specification in the schema where "many-to-many" intersection tables can be
    defined. Note: The table creation alone leads to the creation of a PK which is already
    indexed by default. Note2: A geometry column type, when specified in the JSON schema,
    also defaults to an index in the database.
    """

    def __init__(
        self,
        data_schema_name: str,
        data_table_name: Union[str, Pattern] = re.compile(r".*"),
        db_conn: str = DEFAULT_DB_CONN,
        ind_table: bool = True,
        ind_extra_index: bool = True,
        *args,
        **kwargs,
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
            *args:
            **kwargs:
        """
        super().__init__(*args, **kwargs)
        self.data_schema_name = data_schema_name
        if isinstance(data_table_name, str):
            self.data_table_name = re.compile(data_table_name)
        else:
            self.data_table_name = data_table_name

        self.db_conn = db_conn
        self.ind_table = ind_table
        self.ind_extra_index = ind_extra_index

    def execute(self, context=None):
        """Executes the ``generate_db_object`` method from schema-tools.

        Which leads to the creation of tables and/or an index on the identifier (as specified in
        the data JSON schema). By default both tables and the identifier and 'many-to-many
        table' indexes are created. By setting the boolean indicators in the method parameters,
        tables or an identifier index (per table) can be created.
        """
        data_schema_url = SCHEMA_URL / self.data_schema_name / self.data_schema_name
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

        for table in data["tables"]:
            self.log.info("Considering table '%s'.", table["id"])
            cur_table = f"{self.data_schema_name}_{table['id']}"
            if re.fullmatch(self.data_table_name, cur_table):
                self.log.info("Generating PostgreSQL objects for table '%s'.", table["id"])
                importer.generate_db_objects(
                    table["id"],
                    ind_tables=self.ind_table,
                    ind_extra_index=self.ind_extra_index,
                )
            else:
                self.log.info("Skipping table '%s' (reason: no match).", table["id"])
                continue
