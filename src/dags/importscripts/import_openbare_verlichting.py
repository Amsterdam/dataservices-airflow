import logging
import simplejson as json
from geojson import Point

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# These types are checked against the data of the $OPENBARE_VERLICHTING_DATA_TYPES_SRC url.
# Own administration of objecttypes is not used to compair with source administration.
# To do: remove dictionary when SIA has confirmed
# ID_TYPES_MAP = {
#     "1": {"external_name": "Klok", "internal_name": "Klok",},
#     "2": {"external_name": "Overspanning", "internal_name": "Overspanning",},
#     "3": {"external_name": "Gevel Armaturen", "internal_name": "Gevel_Armatuur",},
#     "4": {"external_name": "LSD Objecten", "internal_name": "Overig_lichtpunt",},
#     "5": {"external_name": "Grachtmast", "internal_name": "Grachtmast",},
#     "10": {"external_name": "Schijnwerpers", "internal_name": "Schijnwerpers",},
# }

ID_TYPES_MAP = {}


def generate_source_types_mapping(types):
    """ 
    generates objecttypes mapping based on source file 
    used in json2geojson to lookup name on objecttype id
    i.e. ["1": {"code":"1","naam":"Klok"}, "10": {"code":"10","naam":"Schijnwerpers"}, ...]
    """
    for type in types:
        ID_TYPES_MAP[type["code"]] = type


def json2geojson(data, types):

    features = []
    for element in data:
        objecttype_id = element.get("objecttype")
        type_map = ID_TYPES_MAP.get(objecttype_id)
        geometry = Point((element.get("lon"), element.get("lat")), srid=4326)

        features.append(
            {
                "type": "Feature",
                "geometry": geometry,
                "properties": {
                    "object_id": element.get("objectid"),
                    "objecttype": element.get("objecttype"),
                    "objecttype_omschrijving": type_map["naam"],
                    "objectnummer": element.get("objectnummer"),
                    "breedtegraad": element.get("lat"),
                    "lengtegraad": element.get("lon"),
                    "storingstatus": element.get("storingstatus"),
                    "meldingstatus": element.get("meldingstatus"),
                },
            }
        )

    log.info(f"features count {len(features)} objects")

    geojson = {"type": "FeatureCollection", "features": [f for f in features]}
    return geojson


# def check_types_as_expected(types: list):
#     """
#     The transformation scripts runs under the assumption that the remote objects have a set mapping.
#     If that is not the case this function will detect that the local mapping is incorrect and will fail.
#     :param types: list, e.g. [{'code': '1', 'naam': 'Klok'}, ...]
#     :return:
#     """
#     actual_types = {
#         type["code"]: type["naam"] for type in types
#     }  # { '1': 'Klok', ... }

#     print(actual_types)

#     for code, mapping in ID_TYPES_MAP.items():
#         actual_type = actual_types.get(code)

#         assert actual_type is not None, f"new type definition does not have type {code}"
#         expected_type = mapping["external_name"]

#         assert (
#             actual_type == expected_type
#         ), f"expected: {expected_type}, got: {actual_type}"


def import_openbare_verlichting(in_file, type_file, out_file):

    # Discrepancy check between own type administration and source is disabled: Source is leading.
    # To do: remove def check_types_as_expected when SIA has confirmed
    # types = json.load(open(type_file))
    # check_types_as_expected(types)

    data = json.load(open(in_file))
    types = json.load(open(type_file))
    generate_source_types_mapping(types)

    geojson = json2geojson(data, types)
    output = open(out_file, "w")

    log.info("writing output...")
    json.dump(geojson, output)
    log.info("done")
