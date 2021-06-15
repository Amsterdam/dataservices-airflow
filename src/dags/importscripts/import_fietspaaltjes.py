import json

import requests
from jsonpointer import resolve_pointer

INSERT_TMPL = """
    INSERT INTO fietspaaltjes_fietspaaltjes_new (
        {names}
    )
    VALUES
        {values}
"""

MAPPING = {
    # "id": "/ref/2013",
    "id": "/ref/current",
    "geometry": "/geo",
    "street": "/location/street",
    "at": "/location/at",
    "area": "/location/area",
    "score_2013": "/score/2013",
    "score_current": "/score/current",
    "count": "/count",
    "paaltjes_weg": "/properties/paaltjes_weg",
    "soort_paaltje": "/properties/soort_paaltje",
    "uiterlijk": "/properties/uiterlijk",
    "type": "/properties/type",
    "ruimte": "/properties/ruimte",
    "markering": "/properties/markering",
    "beschadigingen": "/properties/beschadigingen",
    "veiligheid": "/properties/veiligheid",
    "zicht_in_donker": "/properties/zicht_in_donker",
    "soort_weg": "/properties/soort_weg",
    "noodzaak": "/properties/noodzaak",
}


class ValidationError(Exception):
    pass


class DuplicateError(ValidationError):
    pass


class InvalidValueError(ValidationError):
    pass


def fetch_json():
    url = "https://cdn.endora.nl/mladvies/data_export.json"
    response = requests.get(url)
    data = response.json()
    with open("/tmp/fietspaaltjes.json", "w") as fp:
        json.dump(data, fp)


def q(value):
    if value is None:
        return "null"
    escaped_value = value.replace(r"'", r"''")
    return f"'{escaped_value}'"


def qd(value):
    if value is None:
        return "null"
    return f'"{value}"'


def create_geometry(value):
    lat, lon = value
    if lat is None:
        return "null"
    return f"ST_Transform(ST_GeomFromText('POINT({lon} {lat})', 4326), 28992)"


def create_array(value):
    if not value:
        return "null"
    joined = ", ".join([qd(i) for i in value])
    return f"'{{ {joined} }}'"


def create_count(value):
    if value is None:
        return "null"
    return "null" if value == "nvt" else value


def check_data(field_name, value):
    invalid = {
        "paaltjes_weg": {
            "2013 wel, nu weg",
            "verwijderen paal(en)",
            "verwijderen paal en poer",
        },
        "noodzaak": {"niet functioneel/ niet noodzakelijk", "er past nu een auto door"},
    }

    values = invalid.get(field_name)
    if values is None:
        return

    if set(value) & values:
        raise InvalidValueError


def import_fietspaaltjes(file_path, output_path):

    value_lines = []
    ids = set()

    with open(file_path) as jsonfile:
        data = json.load(jsonfile)
        # geometry = None  # wkb
        for point in data["points"]:
            try:
                values = []
                for field_name, ptr in MAPPING.items():
                    value = resolve_pointer(point, ptr)
                    check_data(field_name, value)
                    if field_name.startswith("id"):
                        if value in ids:
                            raise DuplicateError("Duplicate ID")
                        ids.add(value)
                    if field_name == "count":
                        value = create_count(value)
                    elif field_name == "geometry":
                        value = create_geometry(value)
                    elif ptr.startswith("/properties"):
                        value = create_array(value)
                    else:
                        value = q(value)
                    values.append(value)

                value_line = ", ".join(values)
                value_lines.append(f"({value_line})")
            except ValidationError:
                pass

    with open(output_path, "w") as outfile:
        outfile.write(
            INSERT_TMPL.format(names=", ".join(MAPPING.keys()), values=",\n".join(value_lines))
        )


if __name__ == "__main__":
    fetch_json()
    import_fietspaaltjes("/tmp/fietspaaltjes.json")
