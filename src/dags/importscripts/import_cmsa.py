import csv
import json
import math
import pprint
import re
from pathlib import Path

import pandas as pd

pp = pprint.PrettyPrinter()

# Configuration
CONFIG = {
    # 111    ' Omschrijving\n ', ' Primaire functie(s)\n ', ' Type code\n (V&OR)',
    #        ' Object nr.\n (leverancier)', ' Locatie nr.\n (leverancier)',
    #        ' Standplaatsomschrijving\n', ' Rijksdriehoek\n X-coördinaat (m)',
    #        ' Rijksdriehoek\n Y-coördinaat (m)', ' WGS84 DD\n Latitude (gr)',
    #        ' WGS84 DD\n Longitude (gr)', ' Google Maps\n link coördinaten',
    #        ' Richting\n van het verkeer'
    # 124    ' Omschrijving\n ', ' Primaire functie(s)\n ', ' Type code\n (V&OR)',
    #        ' Object nr.\n (leverancier)', ' Locatie nr.\n (leverancier)',
    #        ' Standplaatsomschrijving\n', ' Rijksdriehoek\n X-coördinaat (m)',
    #        ' Rijksdriehoek\n Y-coördinaat (m)', ' WGS84 DD\n Latitude (gr)',
    #        ' WGS84 DD\n Longitude (gr)', ' Google Maps\n link coördinaten',
    #        ' Richting\n van het verkeer'
    # 127    ' Omschrijving\n ', ' Primaire functie(s)\n ', ' Type code\n (V&OR)',
    #        ' Object nr.\n (leverancier)', ' Locatie nr.\n (leverancier)',
    #        ' Standplaatsomschrijving\n', ' Rijksdriehoek\n X-coördinaat (m)',
    #        ' Rijksdriehoek\n Y-coördinaat (m)', ' WGS84 DD\n Latitude (gr)',
    #        ' WGS84 DD\n Longitude (gr)', ' Google Maps\n link coördinaten',
    #        ' Richting\n van het verkeer'
    "cameras": {
        "sheet_names": ["111", "124", "127"],
        "header": 1,
        "default": {
            "thing": {
                "ref": "Objectnummer Amsterdam",
                "name": " Omschrijving\n ",
                "description": " Omschrijving\n ",
                "purpose": " Primaire functie(s)\n ",
            },
            "location": {
                "ref": " Standplaatsomschrijving\n",
                "name": " Standplaatsomschrijving\n",
                "rd_x": " Rijksdriehoek\n X-coördinaat (m)",
                "rd_y": " Rijksdriehoek\n Y-coördinaat (m)",
                "wgs84_lat": " WGS84 DD\n Latitude (gr)",
                "wgs84_lon": " WGS84 DD\n Longitude (gr)",
            },
        },
        "124": {
            "thing": {"ref": " Object nr.\n (leverancier)"},
            "location": {"ref": " Locatie nr.\n (leverancier)"},
        },
    },
    # Name, Description, Status, Level, Latitude, Longitude, PlaceID, ExpectedStability
    "beacons": {
        "thing": {
            "ref": "Name",
            "name": "Description",
            "description": "Description",
            "purpose": None,
        },
        "location": {
            "ref": "PlaceID",
            "name": "PlaceID",
            "rd_x": None,
            "rd_y": None,
            "wgs84_lat": "Latitude",
            "wgs84_lon": "Longitude",
        },
    },
}

# Table names to write new IoT data to
THINGS_TABLE = "cmsa_sensor_new"
LOCATIONS_TABLE = "cmsa_locatie_new"
# OWNERS_TABLE = "iot_owners_new"


def thing(id, referentie_code, naam, omschrijving, type, doel):
    return {
        "id": id,
        "referentiecode": referentie_code,
        "naam": naam,
        "omschrijving": omschrijving,
        "type": type,
        "doel": doel,
    }


def get_geometry(x, y, code):
    if x is None or y is None:
        return None
    else:
        # transform all geometry to RD, in the DB the geometry type is 28992
        return f"ST_Transform(ST_GeomFromText('POINT({x} {y})', {code}), 28992)"


def location(sensor, referentie_code, naam, rd_x, rd_y, wgs84_lat, wgs84_lon):
    # if there is no RD geometry based data then use WGS84 data
    geometry = (
        get_geometry(wgs84_lon, wgs84_lat, 4326)
        if rd_x is None or rd_y is None
        else get_geometry(rd_x, rd_y, 28992)
    )
    return {
        "sensor_id": sensor,
        "referentiecode": referentie_code,
        "naam": naam,
        "geometry": geometry,
    }


def print_summary(id, things, locations):
    print(
        f"""{id}
  Total things {len(things)}
  Total locations {len(locations)}"""
    )


def import_sensors(filename):
    things = []
    locations = []
    with open(filename, newline="") as jsonfile:
        reader = json.load(jsonfile)
        for row in reader["features"]:
            id = f'{row["properties"]["Soort"]}.{row["id"]}'
            things.append(
                thing(
                    id=id,
                    referentie_code=row["id"],
                    naam=row["properties"]["Objectnummer"],
                    omschrijving=row["properties"]["Soort"],
                    type=row["properties"]["Soort"],
                    doel=row["properties"]["Soort"],
                )
            )
            locations.append(
                location(
                    sensor=id,
                    referentie_code=row["id"],
                    naam=row["properties"]["Objectnummer"],
                    rd_x=None,
                    rd_y=None,
                    wgs84_lat=row["geometry"]["coordinates"][0],
                    wgs84_lon=row["geometry"]["coordinates"][1],
                )
            )
    print_summary(id="Sensors", things=things, locations=locations)
    return (things, locations)


def beacon_value(row, entity, key):
    try:
        return row[CONFIG["beacons"][entity][key]]
    except KeyError:
        return None


def import_beacons(filename):
    things = []
    locations = []
    with open(filename, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for id, row in enumerate(reader):
            if row["Status"] == "ACTIVE" and row["ExpectedStability"] == "STABLE":
                id = f"beacons.{id}"
                # Active and stationary beacons
                things.append(
                    thing(
                        id=id,
                        referentie_code=beacon_value(row, "thing", "ref"),
                        naam=beacon_value(row, "thing", "name"),
                        omschrijving=beacon_value(row, "thing", "description"),
                        type="Beacon",
                        doel=beacon_value(row, "thing", "purpose"),
                    )
                )
                locations.append(
                    location(
                        sensor=id,
                        referentie_code=beacon_value(row, "location", "ref"),
                        naam=beacon_value(row, "location", "name"),
                        rd_x=beacon_value(row, "location", "rd_x"),
                        rd_y=beacon_value(row, "location", "rd_y"),
                        wgs84_lat=beacon_value(row, "location", "wgs84_lat"),
                        wgs84_lon=beacon_value(row, "location", "wgs84_lon"),
                    )
                )
    print_summary(id="Beacons", things=things, locations=locations)
    return (things, locations)


def camera_value(sheet, series, entity, key):
    try:
        value = CONFIG["cameras"][sheet][entity][key]
        print("try value", value)
    except KeyError:
        value = CONFIG["cameras"]["default"][entity][key]
        print("except value", value)
    value = series[value]
    try:
        if math.isnan(value):
            return None
    except TypeError:
        return value
    return value


def import_cameras(filename):
    sheet_names = CONFIG["cameras"]["sheet_names"]
    df = pd.read_excel(filename, sheet_name=sheet_names, header=1)

    things = []
    locations = []
    for sheet in sheet_names:
        print("sheet:", sheet)
        for row in df[sheet].iterrows():
            print("row:", row)
            id, series = row
            print("id:", id, "series:", series)
            try:
                id = f"cameras.{sheet}.{int(id)}"
            except ValueError:
                # End of input
                break

            things.append(
                thing(
                    id=id,
                    referentie_code=camera_value(sheet, series, "thing", "ref"),
                    naam=camera_value(sheet, series, "thing", "name"),
                    omschrijving=camera_value(sheet, series, "thing", "description"),
                    type="Camera",
                    doel=camera_value(sheet, series, "thing", "purpose"),
                )
            )
            locations.append(
                location(
                    sensor=id,
                    referentie_code=camera_value(sheet, series, "location", "ref"),
                    naam=camera_value(sheet, series, "location", "name"),
                    rd_x=camera_value(sheet, series, "location", "rd_x"),
                    rd_y=camera_value(sheet, series, "location", "rd_y"),
                    wgs84_lat=camera_value(sheet, series, "location", "wgs84_lat"),
                    wgs84_lon=camera_value(sheet, series, "location", "wgs84_lon"),
                )
            )
    print_summary(id="Cameras", things=things, locations=locations)
    return (things, locations)


def get_value(item, field):
    # Values are stored as strings, '...'. Convert any containg quotes to double quotes
    value = item[field]
    if value is None:
        return "NULL"
    elif isinstance(value, str):
        if not re.match(r"ST_Transform", value):
            value = value.replace("'", '"')
            value = f"'{value}'"
    return str(value)


def write_inserts(out_dir, things, locations):
    # Write import statements
    # INSERT INTO table
    #     (fieldA, fieldB, ...)
    # VALUES
    #     (valueA, valueB, ...)
    #     (valueA, valueB, ...);
    for (collection, table_name) in [
        (things, THINGS_TABLE),
        (locations, LOCATIONS_TABLE),
    ]:
        with open(f"{out_dir}/{table_name}.sql", "a+") as f:
            fields = collection[0].keys()
            f.write(
                f"""
INSERT INTO {table_name}
    ({', '.join(fields)})
VALUES"""
            )
            for i, item in enumerate(collection):
                values = [get_value(item, field) for field in fields]
                f.write(
                    f"""{"," if i > 0 else ""}
    ({', '.join(values)})"""
                )
            f.write(";")


def import_cmsa(cameras: Path, beacons: Path, sensors: Path, out_dir: Path) -> None:

    for f in out_dir.glob("*.sql"):
        # f.unlink(missing_ok=True)  # only for python 3.8, airflow now needs 3.7
        try:
            f.unlink()
        except FileNotFoundError:
            pass

    for arg, func in zip(
        map(str, (cameras, beacons, sensors)), (import_cameras, import_beacons, import_sensors)
    ):
        things, locations = func(arg)
        write_inserts(out_dir, things, locations)
