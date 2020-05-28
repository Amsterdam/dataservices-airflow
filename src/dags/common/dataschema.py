from requests.exceptions import HTTPError
from schematools.provenance.create import ProvenaceIteration
import json, jsonschema, logging, requests


LOGGER = logging.getLogger("airflow.task")


def get_data_dataschema(dataschema):
    """Return data in json from URL or File"""
    response = requests.get(dataschema)
    LOGGER.info("my response is " + str(response))
    try:
        LOGGER.info("my dataschema is " + str(dataschema))
        response = requests.get(dataschema)
        response.raise_for_status()

    except HTTPError as http_err:
        LOGGER.error((f"HTTP error occurred: {http_err}"))
    except Exception as err:
        LOGGER.error(f"Other error occurred: {err}")

    else:
        data = json.loads(response.text)
        data = response.json()

    LOGGER.info("my data is " + str(data))
    return data


def get_column_names_for_ogr2ogr_sql_select(
    dataschema, file_to_save, source_filename, source_missing_cols=None, skip_cols=None
):
    """Creates a string to use in ogr2ogr bash cmd within the SQL select option (-sql) \
        It returns the renamed source columns as alias for the orginal (which is stated in the provenance element in the datasetschema) 
        I.e. orginal_column_name AS renamed_column_name, [orginal_column_name AS renamed_column_name]
        """
    # get the data as URL or File
    data = get_data_dataschema(dataschema)
    # get the table columns with provenance defined in schema
    try:
        instance = ProvenaceIteration(data)
        data = instance.final_dic

    except (jsonschema.ValidationError, jsonschema.SchemaError, KeyError) as e:
        LOGGER.error(
            "Something went wrong with schematools ({0}): {1}".format(
                e.errno, e.strerror
            )
        )

    # process data to get the column namens and its alias
    try:
        columns = []
        for table in data["tables"]:
            for property in table["properties"]:
                for key, value in property.items():
                    # check if the column needs to added to the table
                    if value not in (skip_cols if skip_cols is not None else ""):
                        # if there is a space replace it with an underscore
                        if value in (
                            source_missing_cols
                            if source_missing_cols is not None
                            else ""
                        ):
                            columns.append("NULL as " + key.replace(" ", "_"))

                        else:
                            columns.append(
                                value.replace(" ", "_") + " as " + key.replace(" ", "_")
                            )

        columns = ",".join(columns)

        output_to_save = []
        output_to_save.append(columns)
        output_to_save.insert(0, "select")
        output_to_save.append(f" from {source_filename}")
        output_to_save = " ".join(output_to_save)

        with open(file_to_save, "w") as f:
            f.write(output_to_save)

    except IOError as e:
        LOGGER.error("I/O error({0}): {1}".format(e.errno, e.strerror))
