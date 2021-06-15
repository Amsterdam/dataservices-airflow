import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import blake2s
from itertools import chain
from typing import Dict, Iterator, List, Optional

import pandas as pd
import requests
from common.db import get_engine
from dateutil import tz
from geoalchemy2 import Geometry, WKTElement
from more_ds.network.url import URL
from shapely.geometry.point import Point

to_zone = tz.gettz("Europe/Amsterdam")


def make_hash(composite_values: List[str], digest_size: int = 5) -> int:
    """The blake2s algorithm is used to generate a single hased value for source
    composite values that uniquely identify a row.
    In case the source itself doesn't provide a single solid identification as key.

    Args:
        composite_values: a list of record values based on
                        which columns uniquely identifiy a record
        digest_size: size to set the max bytes to use for the hash

    Returns:
        a hased value of the composite values in int

    Note:
        The default digest size is for now set to 5 bytes, which is equivalent
        to ~ 10 karakters long.
        Because the id column is preferably of type int, the hased value is converted
        from hex to int.
    """
    return int.from_bytes(
        blake2s("|".join(composite_values).encode(), digest_size=digest_size).digest(),
        byteorder="little",
    )


# ----------------------------------------------------- #
# SCOOTER                                               #
# ----------------------------------------------------- #
@dataclass
class Scooter:
    id: int
    scooter_id: str
    datumtijd_ontvangen: datetime
    indicatie_actueel: bool
    geometrie: Point
    exploitant: str
    status_motor: bool
    status_beschikbaar: Optional[bool] = None
    naam: Optional[str] = None
    max_snelheid: Optional[int] = None
    huidige_locatie: Optional[str] = None
    kenteken: Optional[str] = None
    helm_verplicht: Optional[bool] = None
    helm_aantal_aanwezig: Optional[int] = None
    tarief_start: Optional[int] = None
    tarief_gebruik: Optional[int] = None
    tarief_parkeren: Optional[int] = None


def get_data_scooter_felyx(api_endpoint: str, api_header: Dict) -> Iterator[Scooter]:
    """Retrieves the data from resource felyx
    Because each resource has it's own quirks when calling the API
    each resource has it's own get_data_* method.

    Args:
        api_endpoint: an URL specification for the data service to call
        api_header: a dictionary of headers key value pairs used when calling
            the data endpoint

    Yields:
        A list of Scooter type instances containing felyx data

    """
    try:
        request = requests.get(api_endpoint, headers=api_header, verify=True)
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"{api_endpoint} something went wrong:", e
        ) from e
    else:
        data = json.loads(request.text)["data"]["bikes"]
        for row in data:
            current_time = (
                datetime.now(timezone.utc).astimezone(to_zone).strftime("%Y-%m-%d %H:%M:%S")
            )
            scooter_object = Scooter(
                id=make_hash([row["bike_id"], str(current_time)]),
                scooter_id=row["bike_id"],
                datumtijd_ontvangen=datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S"),
                indicatie_actueel=True,
                geometrie=Point(row["lon"], row["lat"]),
                status_motor=row["is_disabled"],
                status_beschikbaar=False if row["is_reserved"] else True,
                exploitant="felyx",
            )
            yield scooter_object


def get_data_ridecheck(
    token_endpoint: str,
    token_payload: Dict,
    token_header: Dict,
    data_endpoint: str,
    data_headers: Dict,
) -> Iterator[Scooter]:
    """Retrieves the data from resource RIDECHECK
    Because each resource has it's own quirks when calling the API
    each resource has it's own get_data_* method.

    Args:
        token_endpoint: an URL specification for the token service to retreive a
            (time limited) access key
        token_payload: a dictionary of key value pairs elements used in the data body
            when calling the token service
        token_header: a dictionary of headers key value pairs used when calling the
            token endpoint
        data_endpoint: an URL specification of the data service to call
        data_headers: a dictionary of headers key value pairs used when calling
            the data endpoint

    Yields:
        A list of Scooter type instances containing RIDECHECK data

    """
    try:
        token_request = requests.post(
            token_endpoint, params=token_payload, headers=token_header, verify=True
        ).json()
    except ValueError as e:
        raise ValueError(f"{token_endpoint} cannot retrieve data in JSON:", e) from e
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"{token_endpoint} something went wrong:", e
        ) from e
    else:
        token = token_request["access_token"]

    data_headers["Authorization"] = f"Bearer {token}"

    try:
        data_request = requests.get(data_endpoint, headers=data_headers, verify=True)
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            f"{data_endpoint} something went wrong:", e
        ) from e
    else:
        data_object = json.loads(data_request.text)
        for row in data_object:
            if row["city_name"] == "Amsterdam":
                current_time = (
                    datetime.now(timezone.utc).astimezone(to_zone).strftime("%Y-%m-%d %H:%M:%S")
                )
                scooter_object = Scooter(
                    id=make_hash([row["id"], str(current_time)]),
                    scooter_id=row["id"],
                    datumtijd_ontvangen=datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S"),
                    indicatie_actueel=True,
                    geometrie=Point(row["location"]["longitude"], row["location"]["latitude"]),
                    status_motor=True if row["state"] == "on" else False,
                    exploitant="ridecheck",
                    naam=row["name"],
                    max_snelheid=row["speed"],
                    huidige_locatie=row["address"],
                    kenteken=row["license_plate"],
                    helm_verplicht=row["helmet_required"],
                    helm_aantal_aanwezig=row["helmets"],
                    tarief_start=row["starting_fee"],
                    tarief_gebruik=row["driving_fee"],
                    tarief_parkeren=row["parking_fee"],
                )
                yield scooter_object


def import_scooter_data(
    table_name: str,
    felyx_api_endpoint: str,
    felyx_api_header: Dict,
    ridecheck_token_endpoint: str,
    ridecheck_token_payload: Dict,
    ridecheck_token_header: Dict,
    ridecheck_data_endpoint: str,
    ridecheck_data_header: Dict,
) -> None:
    """Union resources felyx and RIDECHECK and imports data into database table

    Args:
        felyx_api_endpoint: an URL specification for the data service to call
        felyx_api_header: a dictionary of headers key value pairs used when calling
            the data endpoint
        ridecheck_token_endpoint: an URL specification for the token service to retreive a
            (time limited) access key
        ridecheck_token_payload: a dictionary of key value pairs elements used in the data body
            when calling the token service
        ridecheck_token_header: a dictionary of headers key value pairs used when calling
            the token endpoint
        ridecheck_data_endpoint: an URL specification of the data service to call
        ridecheck_data_header: a dictionary of headers key value pairs used when
            calling the data endpoint

    Executes:
        SQL insert statements of scooter data into database table

    """
    ridecheck = get_data_ridecheck(
        ridecheck_token_endpoint,
        ridecheck_token_payload,
        ridecheck_token_header,
        ridecheck_data_endpoint,
        ridecheck_data_header,
    )
    felyx = get_data_scooter_felyx(felyx_api_endpoint, felyx_api_header)
    scooter_dataset = chain([row for row in ridecheck], [row for row in felyx])

    df = pd.DataFrame([vars(row) for row in scooter_dataset])
    df["geometrie"] = df["geometrie"].apply(lambda g: WKTElement(g.wkt, srid=4326))
    db_engine = get_engine()
    df.to_sql(
        table_name,
        db_engine,
        if_exists="replace",
        index=False,
        dtype={"geometrie": Geometry(geometry_type="POINT", srid=4326)},
    )
    with db_engine.connect() as connection:
        connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")


# ----------------------------------------------------- #
# AUTO                                                  #
# ----------------------------------------------------- #


@dataclass
class Auto:
    id: int
    auto_id: str
    datumtijd_ontvangen: datetime
    indicatie_actueel: bool
    geometrie: Point
    exploitant: str
    status_beschikbaar: bool
    status_beschikbaar_van: Optional[str] = None
    status_beschikbaar_tot: Optional[str] = None
    status_opladen: Optional[bool] = None
    naam: Optional[str] = None
    huidige_locatie: Optional[str] = None
    kenteken: Optional[str] = None
    merk: Optional[str] = None
    model: Optional[str] = None
    kleur: Optional[str] = None
    opmerking: Optional[str] = None
    brandstof_type: Optional[str] = None
    brandstof_niveau: Optional[int] = None
    bereik: Optional[int] = None
    indicatie_automaat: Optional[str] = None
    indicatie_winterbanden: Optional[bool] = None
    type_slot: Optional[str] = None
    aantal_personen: Optional[int] = None
    tarief_gebruik_uur: Optional[int] = None
    tarief_gebruik_kilometer: Optional[int] = None
    tarief_gebruik_dag: Optional[int] = None
    afbeelding: Optional[str] = None


def get_data_auto_mywheels(api_endpoint: str, api_header: Dict, payload: Dict) -> Iterator[Auto]:
    """Retrieves the data from resource MYWHEELS
    Because each resource has it's own quirks when calling the API
    each resource has it's own get_data_* method.

    Args:
        api_endpoint: an URL specification for the data service to call
        api_header: a dictionary of headers key value pairs used when calling
            the data endpoint
        payload: a dictionary of key value pairs used in het data body when calling
            the data endpoint
            Beware! The payload must be serialized from dict to string
    Yields:
        A list of Auto type instances containing mywheels data

    Note:
        For mywheels the start- and enddate must be provided when calling the api.
        There is a 10 min time window between the start and end.
        The payload must be serialized to be accepted.

    """
    startDate: str = datetime.now(timezone.utc).astimezone(to_zone).strftime("%Y-%m-%d %H:%M")
    delta: datetime = datetime.now(timezone.utc) + timedelta(minutes=10)
    endDate: str = delta.astimezone(to_zone).strftime("%Y-%m-%d %H:%M")

    payload["params"]["timeFrame"]["startDate"] = startDate
    payload["params"]["timeFrame"]["endDate"] = endDate
    payload_serialized = json.dumps(payload)

    try:
        request = requests.request(
            "POST", api_endpoint, headers=api_header, data=payload_serialized, verify=True
        )
    except requests.exceptions.RequestException as e:
        raise requests.exceptions.RequestException(
            "{api_endpoint} something went wrong: {request.status_code}", e
        ) from e
    else:
        data = json.loads(request.text)["result"]["results"]
        for row in data:
            availability = row["availability"]
            row = row["resource"]
            if row["city"] == "Amsterdam":
                current_time = (
                    datetime.now(timezone.utc).astimezone(to_zone).strftime("%Y-%m-%d %H:%M:%S")
                )
                auto_object = Auto(
                    id=make_hash([str(row["id"]), str(current_time)]),
                    auto_id=row["id"],
                    datumtijd_ontvangen=datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S"),
                    indicatie_actueel=True,
                    geometrie=Point(row["longitude"], row["latitude"]),
                    exploitant="mywheels",
                    status_beschikbaar=False if availability["status"] == "unavailable" else True,
                    status_beschikbaar_van=availability["beginAvailable"],
                    status_beschikbaar_tot=availability["endAvailable"],
                    status_opladen=row["charging"],
                    naam=row["alias"],
                    huidige_locatie=" ".join(
                        [row.get("location", "onbekend"), row.get("streetnumber", "")]
                    ).strip(),
                    kenteken=row["registrationPlate"],
                    merk=row["brand"],
                    model=row["model"],
                    kleur=row["color"],
                    opmerking=json.loads(row["advertisement"]).get("info", None)
                    if row["advertisement"]
                    else None,
                    brandstof_type=row["fuelType"],
                    brandstof_niveau=row["fuelLevel"],
                    bereik=row["fuelRange"],
                    indicatie_automaat=row["options"]["automatic"],
                    indicatie_winterbanden=row["options"]["winterTires"],
                    type_slot=",".join(row["locktypes"]),
                    aantal_personen=row["numberOfSeats"],
                    tarief_gebruik_uur=row["price"]["hourRate"],
                    tarief_gebruik_kilometer=row["price"]["kilometerRate"],
                    tarief_gebruik_dag=row["price"]["dayRateTotal"],
                    afbeelding=URL(api_endpoint.split("api")[0])
                    / row.get("pictures")[0].get("large", None)
                    if row.get("pictures")
                    else None,
                )
                yield auto_object


def import_auto_data(
    table_name: str,
    mywheels_api_endpoint: str,
    mywheels_api_header: Dict,
    mywheels_api_payload: Dict,
) -> None:
    """Union resources MYWHEELS and [...] and imports data into database table

    Args:
        mywheels_api_endpoint: an URL specification for the data service to call
        mywheels_api_header: a dictionary of headers key value pairs used when calling
            the data endpoint
        mywheels_api_payload: a dictionary of key value pairs elements used in the data body
            when calling the token service

    Executes:
        SQL insert statements of auto data into database table

    """
    mywheels = get_data_auto_mywheels(
        mywheels_api_endpoint, mywheels_api_header, mywheels_api_payload
    )
    auto_dataset = [row for row in mywheels]

    df = pd.DataFrame([vars(row) for row in auto_dataset])
    df["geometrie"] = df["geometrie"].apply(lambda g: WKTElement(g.wkt, srid=4326))
    db_engine = get_engine()
    df.to_sql(
        table_name,
        db_engine,
        if_exists="replace",
        index=False,
        dtype={"geometrie": Geometry(geometry_type="POINT", srid=4326)},
    )
    with db_engine.connect() as connection:
        connection.execute(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id)")
