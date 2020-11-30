from airflow.models.baseoperator import BaseOperator

from schematools.utils import schema_fetch_url_file
from schematools.importer.base import BaseImporter
from schematools.types import DatasetSchema, SchemaType
from schematools.cli import _get_engine

from environs import Env

env = Env()
# split is used to remove url params, if exists.
# for database connection the url params must be omitted.
default_db_conn = env("AIRFLOW_CONN_POSTGRES_DEFAULT").split("?")[0]


class SqlAlchemyCreateObjectOperator(BaseOperator):
    """ This operator takes a JSON data schema definition and DB connection to create the specified
    tables and/or an index. The latter is based on the Identifier object in the data JSON schema i.e.
    "identifier": ["identificatie", "volgnummer"] which is common for temporal data.
    Note: The table creation alone leads to the creation of a PK which is already indexed by default.
    Note2: A geometry column type, when specified in the JSON schema, also defaults to an index in the database.
    """

    def __init__(
        self,
        data_schema_name,
        data_table_name,
        data_schema_env=None,
        db_conn=default_db_conn,
        ind_table=True,
        ind_identifier_index=True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.data_schema_name = data_schema_name
        self.data_table_name = data_table_name
        # Optionals
        self.data_schema_env = f"{data_schema_env}." if data_schema_env else ""
        self.db_conn = db_conn
        self.ind_table = ind_table
        self.ind_identifier_index = ind_identifier_index


    def execute(self, context=None):
        """ Executes the 'generate_db_object' method from schema-tools.
        Which leads to the creation of tables and/or an index on the identifier (as specified in the data JSON schema).
        By default both tables and the identifier index are created.
        By setting the boolean indicators in the method parameters, tables or an identifier index (per table) can be created.
        """
        data_schema_url = f"https://{self.data_schema_env}schemas.data.amsterdam.nl/datasets/{self.data_schema_name}/{self.data_schema_name}"
        data = schema_fetch_url_file(data_schema_url)
        engine = _get_engine(self.db_conn)
        parent_schema = SchemaType(data)
        dataset_schema = DatasetSchema(parent_schema)
        importer = BaseImporter(dataset_schema, engine)

        for table in data["tables"]:
            if self.data_schema_name + '_' + table["id"] == f"{self.data_table_name}":
                importer.generate_db_objects(
                    table["id"],
                    ind_tables=self.ind_table,
                    ind_identifier_index=self.ind_identifier_index,
                )
            else:
                continue