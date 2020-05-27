import json, jsonschema
from .utils import schema_fetch_url_file
from schematools.provenance.create import ProvenaceIteration


def get_column_names_for_ogr2ogr_sql_select(
    dataschema, file_to_save, source_filename, source_missing_cols=(), skip_cols=()
):
    """Creates a string to use in ogr2ogr bash cmd within the SQL select option (-sql) \
        It returns the renamed source columns as alias for the orginal (which is stated in the provenance element in the datasetschema) 
        I.e. orginal_column_name AS renamed_column_name, [orginal_column_name AS renamed_column_name]
        """
    # get the data as URL or File
    data = schema_fetch_url_file(dataschema)

    # get the table columns with provenance defined in schema
    try:
        instance = ProvenaceIteration(data)
        data = instance.final_dic

    except (jsonschema.ValidationError, jsonschema.SchemaError, KeyError) as e:
        print(str(e), err=True)
        exit(1)

    # process data to get the column namens and its alias
    try:
        output = "select "
        for table in data["tables"]:
            for property in table["properties"]:
                for k, v in property.items():
                    # check if the column needs to added to the table
                    if v not in skip_cols:
                        # if there is a space replace it with an underscore
                        if v in source_missing_cols:
                            output += "NULL as " + v.replace(" ", "_") + ","
                        else:
                            output += (
                                k.replace(" ", "_") + " as " + v.replace(" ", "_") + ","
                            )

        # remove last character(comma)
        ouput_to_save = output[:-1]
        ouput_to_save += f" from {source_filename}"

        file = open(file_to_save, "w")
        file.write(ouput_to_save)
        file.close()

    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
