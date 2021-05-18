import pandas as pd
import re

import pprint

pp = pprint.PrettyPrinter()

# Configuration
# Sheet that contains the reference data
SHEET_NAME = "Achterkant"
FAQ_SHEET_NAME = "FAQ"
METADATA_SHEET_NAME = "Metadata"

# Columns that contain the items
TEXT = "Kerntekst"
DESCRIPTION = "Toelichting"
QUESTION = "Vraag"
ANSWER = "Antwoord"
PROPERTY = "Eigenschap"
VALUE = "Waarde"

# Colums that contain the item properties
PROPERTIES = [
    ("Theme", ["Thema", "Subthema 1", "Subthema 2"]),
    ("Area", ["Stadsdeel"]),
    ("Type", ["Type."]),
    ("Level", ["Niveau "]),
    ("Source", ["(bestuurlijke)  bron "]),
]

# Columns that contain attributes
ATTRIBUTES = [
    (
        "Image",
        [
            "Afbeelding 1",
            "Afbeelding 2",
            "Afbeelding 3",
            "Afbeelding 4",
            "Afbeelding 5",
        ],
    ),
    ("Link", ["Download 1", "Download 2"]),
    ("SourceLink", ["(bestuurlijke)  bron "]),
]

# Table names to write new HIOR data to
ITEMS_TABLE = "hior_items_new"
PROPS_TABLE = "hior_properties_new"
ATTRS_TABLE = "hior_attributes_new"
FAQ_TABLE = "hior_faq_new"
METADATA_TABLE = "hior_metadata_new"


def import_row(id, series):
    # Add 2 to id because lines start at 1. Index starts at 0 and 1 line if for the header
    id = id + 2
    text = "" if pd.isnull(series[TEXT]) else series[TEXT]
    description = "" if pd.isnull(series[DESCRIPTION]) else series[DESCRIPTION]

    if len(text) == 0:
        # Skip lines with empty TEXT field
        pp.pprint(f"Warning: line {id} - Missing {TEXT}")
        return ({}, [], [])

    item_properties = []
    for (name, keys) in PROPERTIES:
        values = [series[key] for key in keys]
        for value in [value for value in values if not (pd.isnull(value) or value == "")]:
            item_properties.append({"item_id": id, "name": name, "value": value})

    item_attributes = []
    for (name, keys) in ATTRIBUTES:
        values = [series[key] for key in keys]
        for value in [value for value in values if not (pd.isnull(value) or value == "")]:
            item_attributes.append({"item_id": id, "name": name, "value": value})

    # Post process
    for property in item_properties:
        value = property["value"]
        if isinstance(value, str):
            # Levels are stored as "1. <Level name>", convert to "<Level name>"
            if property["name"] == "Level":
                value = re.sub(r"^\d\. ", "", value)
            # Uniform values, transform string like "aap noot " to "Aap Noot"
            value = value.title().strip()
            property["value"] = value

    for attribute in item_attributes:
        value = attribute["value"]
        if isinstance(value, str):
            value = re.sub(r"\\", "/", value)  # Correction for Windows path names
            attribute["value"] = value.strip()

    # Check validity
    isValid = True
    for (name, _) in PROPERTIES:
        props = [property["value"] for property in item_properties if property["name"] == name]
        isPropValid = len(props) > 0
        if not isPropValid:
            pp.pprint(f"Warning: line {id} - Missing property {name}")
            isValid = False

    if not isValid:
        return ({}, [], [])

    item = {"id": id, "text": text, "description": description}
    return (item, item_properties, item_attributes)


def import_faq_row(id, series):
    # Add 2 to id because lines start at 1. Index starts at 0 and 1 line if for the header
    id = id + 2
    question = "" if pd.isnull(series[QUESTION]) else series[QUESTION].strip()
    answer = "" if pd.isnull(series[ANSWER]) else series[ANSWER].strip()

    if len(question) == 0 or len(answer) == 0:
        # Skip lines with empty TEXT field
        pp.pprint(f"Warning: line {id} - Missing Q: {QUESTION} or A: {ANSWER}")
        return {}

    return {"id": id, "question": question, "answer": answer}


def import_meta_row(id, series):
    # Add 2 to id because lines start at 1. Index starts at 0 and 1 line if for the header
    id = id + 2
    property = "" if pd.isnull(series[PROPERTY]) else series[PROPERTY].strip()
    value = "" if pd.isnull(series[VALUE]) else series[VALUE]

    if len(property) == 0:
        # Skip lines with empty TEXT field
        pp.pprint(f"Warning: line {id} - Missing property: {property} or value: {value}")
        return {}

    return {"id": id, "property": property, "value": value}


def import_file(filename):
    # Import the HIOR Excel file
    df = pd.read_excel(filename, sheet_name=[SHEET_NAME, FAQ_SHEET_NAME, METADATA_SHEET_NAME])

    items = []
    properties = []
    attributes = []
    metadata = []
    for row in df[SHEET_NAME].iterrows():
        id, series = row

        (item, itemProperties, itemAttributes) = import_row(id, series)
        if item != {}:
            items.append(item)
            properties = properties + itemProperties
            attributes = attributes + itemAttributes

    faqs = []
    for row in df[FAQ_SHEET_NAME].iterrows():
        id, series = row

        faq = import_faq_row(id, series)
        if faq != {}:
            faqs.append(faq)

    for row in df[METADATA_SHEET_NAME].iterrows():
        id, series = row

        meta = import_meta_row(id, series)
        if meta != {}:
            metadata.append(meta)

    # Report summary; unique property values, #items and #properties
    for (name, _) in PROPERTIES:
        values = set([property["value"] for property in properties if property["name"] == name])
        pp.pprint(f"{name} - {len(values)} values")

    pp.pprint(f"Total items {len(items)}")
    pp.pprint(f"Total properties {len(properties)}")
    pp.pprint(f"Total attributes {len(attributes)}")
    pp.pprint(f"Total faq {len(faqs)}")
    pp.pprint(f"Total metadata {len(metadata)}")
    return (items, properties, attributes, faqs, metadata)


def get_value(item, field):
    # Values are stored as strings, '...'. Convert any containg quotes to double quotes
    value = item[field]
    if isinstance(value, str):
        value = value.replace("'", '"')
        value = f"'{value}'"
    return str(value)


def get_insert(table_name, collection, fields):
    insert_into = f"""
INSERT INTO {table_name}
    ({', '.join(fields)})
VALUES"""
    for i, item in enumerate(collection):
        values = [get_value(item, field) for field in fields]
        insert_into += f"""{"," if i > 0 else ""}
    ({', '.join(values)})"""
    insert_into += ";"
    return insert_into


def write_inserts(out_dir, items, properties, attributes, faq, metadata):
    # Write import statements
    # INSERT INTO table
    #     (fieldA, fieldB, ...)
    # VALUES
    #     (valueA, valueB, ...)
    #     (valueA, valueB, ...);
    for (collection, table_name) in [
        (items, ITEMS_TABLE),
        (properties, PROPS_TABLE),
        (attributes, ATTRS_TABLE),
        (faq, FAQ_TABLE),
        (metadata, METADATA_TABLE),
    ]:
        with open(f"{out_dir}/{table_name}.sql", "w") as f:
            fields = collection[0].keys()
            f.write(get_insert(table_name, collection, fields))


def import_hior(input_xls, out_dir):

    items, properties, attributes, faq, metadata = import_file(input_xls)
    write_inserts(out_dir, items, properties, attributes, faq, metadata)
