import simplejson as json
from geojson import GeometryCollection, MultiPolygon, Polygon, loads


def json2geojson(data):
    mileuzones = data["milieuzones"]
    features = []
    for element in mileuzones:
        if "milieuzone" in element:
            milieuzone = element["milieuzone"]
            multipolygon = None
            geo = loads(milieuzone["geo"])
            # Extract multipolygon
            if type(geo) is MultiPolygon:
                multipolygon = geo
            elif type(geo) is GeometryCollection:
                for f in geo["geometries"]:
                    if type(f) is Polygon:
                        multipolygon = MultiPolygon(f)
                        break
                    elif type(f) is MultiPolygon:
                        multipolygon = f
                        break

            if multipolygon is None:
                raise Exception("Missing (multi)polygon")

            properties = {key: milieuzone[key] for key in ("id", "verkeerstype", "vanafdatum")}
            features.append(
                {
                    "type": "Feature",
                    "geometry": multipolygon,
                    "properties": properties,
                }
            )

    geojson = {"type": "FeatureCollection", "features": [f for f in features]}
    return geojson


def import_milieuzones(in_file, out_file):
    data = json.load(open(in_file))
    geojson1 = json2geojson(data)
    output = open(out_file, "w")
    json.dump(geojson1, output)
