import logging
import simplejson as json
from geojson import Point

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# These types are checked against the data of the $OPENBARE_VERLICHTING_DATA_TYPES_SRC url.
ID_TYPES_MAP = {
    "1": {"external_name": "Klok", "internal_name": "Klok",},
    "2": {"external_name": "Overspanning", "internal_name": "Overspanning",},
    "3": {"external_name": "Gevel Armaturen", "internal_name": "Gevel_Armatuur",},
    "4": {"external_name": "OVL Objecten", "internal_name": "Overig_lichtpunt",},
    "5": {"external_name": "Grachtmast", "internal_name": "Grachtmast",},
    "10": {"external_name": "Schijnwerpers", "internal_name": "Schijnwerpers",},
}


def json2geojson(data,):
    features = []
    for element in data:
        objecttype_id = element.get("objecttype")
        geometry = Point((element.get("lon"), element.get("lat")), srid=4326)

        try:
            type_mapping = ID_TYPES_MAP[objecttype_id]
        except KeyError:
            raise RuntimeError(
                f"objecttype={objecttype_id} not found, unable to import {element!r}: "
            ) from None
        assert type_mapping is not None
        type_name = type_mapping["internal_name"]

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "type_id": objecttype_id,
                    "type_name": type_name,
                    "objectnummer": element["objectnummer"],
                },
            }
        )

    log.info(f"features count {len(features)} objects")

    geojson = {"type": "FeatureCollection", "features": [f for f in features]}
    return geojson


def check_types_as_expected(types: list):
    """
    The transformation scripts runs under the assumption that the remote objects have a set mapping.
    If that is not the case this function will detect that the local mapping is incorrect and will fail.
    :param types: list, e.g. [{'code': '1', 'naam': 'Klok'}, ...]
    :return:
    """
    actual_types = {
        type["code"]: type["naam"] for type in types
    }  # { '1': 'Klok', ... }

    for code, mapping in ID_TYPES_MAP.items():
        actual_type = actual_types.get(code)
        assert actual_type is not None, f"new type definition does not have type {code}"
        expected_type = mapping["external_name"]
        assert (
            actual_type == expected_type
        ), f"expected: {expected_type}, got: {actual_type}"


def import_openbare_verlichting(in_file, type_file, out_file):

    types = json.load(open(type_file))
    check_types_as_expected(types)

    data = json.load(open(in_file))

    geojson = json2geojson(data,)
    output = open(out_file, "w")

    log.info("writing output...")
    json.dump(geojson, output)
    log.info("done")
