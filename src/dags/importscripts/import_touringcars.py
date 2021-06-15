import json
import logging

from geojson import GeometryCollection, loads

LOGGER = logging.getLogger("airflow.task")


def json2geojson(data, in_file):
    """
    Transform json input to geojson
    """

    # Metadata setup
    TOURINGCARS = [
        {
            "id": "doorrijhoogtes",
            "root": "max_doorrijhoogtes",
            "element": "max_doorrijhoogte",
            "properties": ["title", "Maximale_doorrijhoogte"],
        },
        {
            "id": "haltes",
            "root": "in_uitstaphaltes",
            "element": "in_uitstaphalte",
            "properties": ["title", "Bijzonderheden", "Busplaatsen"],
        },
        {
            "id": "parkeerplaatsen",
            "root": "parkeerplaatsen",
            "element": "parkeerplaats",
            "properties": ["title", "Bijzonderheden", "Busplaatsen", "linkurl", "linknaam"],
        },
        {
            "id": "wegwerkzaamheden",
            "root": "wegwerkzaamheden",
            "element": "wegopbreking",
            "properties": ["title", "opmerkingen", "linkurl", "linknaam"],
        },
        {
            "id": "verplichteroutes",
            "root": "verplichte_routes",
            "element": "verplichte_route",
            "properties": ["title"],
        },
        {
            "id": "aanbevolenroutes",
            "root": "aanbevolen_routes",
            "element": "aanbevolen_route",
            "properties": ["title"],
        },
    ]

    geojson_features = []
    dataset = {}
    element = {}
    properties = []

    # setup processing variables
    for tables in TOURINGCARS:

        # get the set that matches the given input parameter 'in_file'
        if (tables.get("id")) in in_file:

            # set variables
            dataset = data[tables["root"]]
            element = tables["element"]
            properties = tables["properties"]

    # processing
    for rows in dataset:

        row = rows[element]
        geometry = {}
        multipolygon = None
        point = None
        multilinestring = None
        geo = loads(row["Lokatie"])

        if geo.is_valid:
            LOGGER.info("Geometry checking...OK valid")
        else:
            LOGGER.exception("Missing geometry: No Lokatie element present in source")
            break

        properties = {key: row[key] for key in properties}

        if type(geo) is not GeometryCollection:

            geometry = geo

            # add geo element to feature list
            geojson_features.append(
                {
                    "type": "Feature",
                    "geometry": geometry,
                    "properties": properties,
                }
            )

        # if the type is a collection of elements, then each geo element gets added to the feature list separately
        elif type(geo) is GeometryCollection:

            for geo_element in geo["geometries"]:

                geometry = geo_element

                # add each geo element to the feature list
                geojson_features.append(
                    {
                        "type": "Feature",
                        "geometry": geometry,
                        "properties": properties,
                    }
                )

    geojson = {"type": "FeatureCollection", "features": [feature for feature in geojson_features]}
    return geojson


def import_touringcars(in_file, out_file):
    """
    Loading json file and calling function to transform json to geojson
    """

    data = json.load(open(in_file))

    geojson = json2geojson(data, in_file)
    output = open(out_file, "w")

    LOGGER.info(f"writing output file: {out_file}")
    json.dump(geojson, output)
    LOGGER.info(f"done, output is: {geojson}")
