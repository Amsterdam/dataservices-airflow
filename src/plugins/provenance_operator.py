from requests.exceptions import HTTPError
from schematools.provenance.create import ProvenaceIteration
import json, jsonschema, logging, requests, re
from string_utils import slugify
from string_utils.manipulation import camel_case_to_snake

from airflow.models.baseoperator import BaseOperator

LOGGER = logging.getLogger("airflow.task")


class ProvenanceOperator(BaseOperator):
    """ 
    This operator processes provenance elements that are defined in a dataschema voor a dataset.
    This operator will rename column names in the initial .sql output of OGR2OGR 'PGdumps' 
    After rename, the .sql can be applied on the database. 
    """

    def __init__(
        self, metadataschema, source_file, table_to_get_columns=None, *args, **kwargs,
    ):
        """ constructor """
        self.metadataschema = metadataschema
        self.source_file = source_file
        self.table_to_get_columns = table_to_get_columns
        super().__init__(*args, **kwargs)

    def execute(self, context):
        """ overwrite execute method from BaseOperator """

        def get_column_names(metadataschema, table_to_get_columns=None):
            """Return the column names (in a dictory) from the dataschema incl. provenance translated column names.
                Because in the metadataschema the fieldnames are camelCase, the names have to be translated to snake_case
                in order to use in the database.             
            """

            result = []
            # get the metadataschema
            with open(metadataschema, "r") as f:
                metadataschema = json.load(f)

            LOGGER.info("loaded metadataschema > " + str(metadataschema))

            # apply the provenace method to get the column names including, if defined, the provenance names defined on dataset, table and column (field) level
            try:
                instance = ProvenaceIteration(metadataschema)
                columns = instance.final_dic
                LOGGER.info("extracted data (incl. provenance) > " + str(columns))

            except (jsonschema.ValidationError, jsonschema.SchemaError, KeyError) as e:
                self.log.error(e)

            # list only the columns
            for table in columns["tables"]:
                table_info = {}
                table_info["table"] = table["table"]
                table_info["properties"] = []
                columns_set = {}
                # check: get only the columns from a specific table if given as argument
                if (
                    table_to_get_columns
                    if table_to_get_columns is not None
                    else table["table"]
                ) not in table["table"]:
                    continue

                for property in table["properties"]:
                    for key, value in property.items():

                        # TO DO: move the 'camelCase to snake_case' translation to schema-tools?
                        # translate camelCase to snake_case for the column names in order to use it in database
                        key = camel_case_to_snake(key)
                        value = camel_case_to_snake(value)
                        columns_set[key] = value

                table_info["properties"].append(columns_set)
                result.append(table_info)

            LOGGER.info("extracted column names (incl. provenance) > " + str(result))
            return result

        # 1. get field names out of metadataschema incl. provenance
        columns = get_column_names(self.metadataschema, self.table_to_get_columns)

        # 2. read source (.sql) file to translate
        with open(self.source_file) as f:
            file = f.read()

        # 3. translate columns based on field names from metadataschema
        for column in columns:
            for property in column["properties"]:
                for new_value, old_value in property.items():
                    file = file.replace(
                        slugify(old_value, separator="_"),
                        slugify(new_value, separator="_"),
                    )
        result = file

        LOGGER.info(f"writing to {self.source_file} > " + str(result))

        # 4. save file (.sql)
        with open(self.source_file, "wt") as f:
            f.write(result)
            f.close()
