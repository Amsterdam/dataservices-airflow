#!/usr/bin/env python
import argparse
import os
import re
from collections import OrderedDict
import requests
import logging
import psycopg2
import urllib.parse
import random

from requests import HTTPError
from simplejson import JSONDecodeError

from common.db import fetch_pg_env_vars

LOGLEVEL = os.environ.get("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL)
log = logging.getLogger(__name__)

pg_env_vars = fetch_pg_env_vars()
dbname = pg_env_vars["PGDATABASE"]
user = pg_env_vars["PGUSER"]
password = pg_env_vars["PGPASSWORD"]
host = pg_env_vars["PGHOST"]
port = pg_env_vars["PGPORT"]

LAADPAAL_MAX_AGE = 8640000  # 100 days

LAADPAAL_DELETE_AGE = 864000  # 10 days

oplaadpunten_providers = {
    "Allego",
    #'NewMotion',
    #'Nuon',
    #'Eneco'
}

base_url = "https://www.allego.eu/api/feature/experienceaccelerator/areas/chargepointmap/getchargepoints"


def get_all_oplaadpunten():
    first_point = "52.287,4.768"
    second_point = "52.425,5.014"
    url = f"{base_url}?firstPoint={first_point}&secondPoint={second_point}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_remote_oplaadpaal(id: str):
    try:
        if "&" in id:
            # TODO Tell / Ask Allego about invalid characters in ID
            # The URL https://www.allego.eu/api/feature/experienceaccelerator/areas/chargepointmap/getchargepoints/P1%20Parking%20V%26D
            # also gives a error on their own website (Parking in former V&D in Amstelveen)
            log.warning(f"Invalid char & in ID {id} for Allego service. Skipping...")
            return None
        url = f'{base_url}/{urllib.parse.quote_plus(id).replace("+", "%20")}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except JSONDecodeError as e:
        return None
    except HTTPError as e:
        log.warning(f"Failed to get oplaadpaal for {url}:", e)
        return None


def update_oplaadpaal(curs, table_name: str, id1: str, status: str):
    sql = f"""
update {table_name} set status = %(status)s, last_status_update = current_timestamp
 where cs_external_id = %(id)s
"""
    curs.execute(sql, {"status": status, "id": id1})
    if curs.rowcount != 1:
        log.error(f"Failed to update oplaadpaal {id1}")
        ret = False
    else:
        ret = True
    return ret


def _make_oplaadpaal_args(opl: dict):
    # Match street with spaces not greedy, then optional housenumber and optional housenumberext
    p = re.compile("^([\w\s]+?)(?:\s+(\d+)\s*([A-Za-z][\w]*)?)?$")
    m = p.match(opl["address"]["addressLine1"])
    if m:
        street = m.group(1)
        housenumber = m.group(2)
        if housenumber and len(housenumber) > 6:
            housenumber = housenumber[:6]
        housnumberext = m.group(3)
        if housnumberext and len(housnumberext) > 6:
            housnumberext = housnumberext[:6]

    else:
        street = opl["address"]["addressLine1"]
        housenumber = None
        housnumberext = None
    country = opl["address"]["country"]
    if country == "NL":
        country = "NLD"
    postalcode = opl["address"]["postalCode"].replace(" ", "")[:6]

    evses = opl["evses"]
    charging_point = len(evses)
    status_list = []
    connector_type_list = []
    vehicle_type_list = []
    charging_capability_list = []
    identification_type_list = []
    charging_cap_max = 0.0

    for evse in evses:
        status_list.append(evse["status"])
        connector_type_list.append(evse["connectorType"])
        charging_cap = float(evse["maxPower"])
        if charging_cap > charging_cap_max:
            charging_cap_max = charging_cap
        charging_capability_list.append(str(charging_cap))
        identification_type_list.append((evse["displayName"]))

    connectivityStatus = opl["connectivityStatus"]
    if connectivityStatus == "Offline":
        status = connectivityStatus
    else:
        if "Available" in status_list:
            status = "Available"
        elif "Occupied" in status_list:
            status = "Occupied"
        else:
            status = ";".join(list(OrderedDict.fromkeys(status_list)))

    args = {
        "cs_external_id": opl["chargePointId"],
        "longitude": opl["location"]["longitude"],
        "latitude": opl["location"]["latitude"],
        "street": street,
        "housenumber": housenumber,
        "housnumberext": housnumberext,
        "postalcode": postalcode,
        "district": opl["address"]["stateProvince"],
        "countryiso": country,
        "city": opl["address"]["city"],
        "provider": opl["cpoOrganisationId"],
        "restrictionsremark": None,
        "charging_point": charging_point,
        "status": status,
        "connector_type": ";".join(list(OrderedDict.fromkeys(connector_type_list))),
        "vehicle_type": ";".join(list(OrderedDict.fromkeys(vehicle_type_list))),
        "charging_capability": ";".join(
            list(OrderedDict.fromkeys(charging_capability_list))
        ),
        "identification_type": ";".join(
            list(OrderedDict.fromkeys(identification_type_list))
        ),
        "charging_cap_max": charging_cap_max,
    }
    return args


def update_complete_oplaadpaal(curs, table_name: str, opl: dict):
    sql = f"""
update {table_name}
  set wkb_geometry = ST_Transform(ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326), 28992)
  , street = %(street)s
  , housenumber = %(housenumber)s
  , housnumberext = %(housnumberext)s
  , postalcode = %(postalcode)s
  , district = %(district)s
  , countryiso = %(countryiso)s
  , city = %(city)s
  , provider = %(provider)s
  , restrictionsremark = %(restrictionsremark)s
  , charging_point = %(charging_point)s
  , status = %(status)s
  , connector_type = %(connector_type)s
  , vehicle_type = %(vehicle_type)s
  , charging_capability = %(charging_capability)s
  , charging_cap_max = %(charging_cap_max)s
  , identification_type = %(identification_type)s
  , last_update = current_timestamp
  , last_status_update = current_timestamp
    where cs_external_id = %(cs_external_id)s
    """
    args = _make_oplaadpaal_args(opl)
    curs.execute(sql, args)
    if curs.rowcount != 1:
        log.error(f"Failedcomplete update oplaadpaal {args['cs_external_id']}")
        ret = False
    else:
        ret = True
    return ret


def get_oplaadpaal(curs, table_name: str, id1: str):
    sql = f"""
select id, cs_external_id, status, (EXTRACT(EPOCH FROM NOW() - last_update))::int as update_age
 from {table_name}
 where cs_external_id = %(id)s
"""
    curs.execute(sql, {"id": id1})
    result = curs.fetchall()
    if result and len(result) > 0:
        return result[0]
    else:
        return None


def create_oplaadpaal(curs, table_name: str, opl: dict):
    sql = f"""
insert into {table_name}(
   cs_external_id
,  wkb_geometry
,  street
,  housenumber
,  housnumberext
,  postalcode
,  district
,  countryiso
,  city
,  provider
,  restrictionsremark
,  charging_point
,  status
,  connector_type
,  vehicle_type
,  charging_capability
,  identification_type
,  charging_cap_max
) values (
   %(cs_external_id)s
,  ST_Transform(ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326), 28992)
,  %(street)s
,  %(housenumber)s
,  %(housnumberext)s
,  %(postalcode)s
,  %(district)s
,  %(countryiso)s
,  %(city)s
,  %(provider)s
,  %(restrictionsremark)s
,  %(charging_point)s
,  %(status)s
,  %(connector_type)s
,  %(vehicle_type)s
,  %(charging_capability)s
,  %(identification_type)s
,  %(charging_cap_max)s
);
    """
    args = _make_oplaadpaal_args(opl)
    curs.execute(sql, args)
    if curs.rowcount != 1:
        log.error(f"Failed to create oplaadpaal {opl['chargePointId']}")
        ret = False
    else:
        ret = True
    return ret


def set_oplaadpalen_unknown(curs, table_name: str):
    """
    Set all those oplaadpalen that were not present in the last update to status 'Unknown'
    """
    sql = f"""
UPDATE {table_name} set status = 'Unknown'
WHERE status in ('Available', 'Occupied')  AND last_status_update <> (select max(last_status_update) from {table_name})
"""
    curs.execute(sql)
    return curs.rowcount


def set_oplaadpalen_deleted(curs, table_name: str, max_requests: int):
    """
    Set oplaadpalen to deleted if they have not been updated for 10 days and it does not return a result when
    we get remote data for the oplaadpunt.

    We only do max_requests requests to get  remote oplaadpunten
    """
    sql = f"""
select id, cs_external_id, status
from  {table_name}
where (EXTRACT(EPOCH FROM NOW() - last_status_update))::int > {LAADPAAL_DELETE_AGE}
and status = 'Unknown'
    """
    curs.execute(sql)
    old_laadpalen = curs.fetchall()
    delete_count = 0
    request_count = 0
    for old_laadpaal in old_laadpalen:
        id1 = old_laadpaal[1]
        if request_count >= max_requests:
            break
        oplaadpaal_remote = get_remote_oplaadpaal(id1)
        request_count += 1
        if oplaadpaal_remote:
            update_complete_oplaadpaal(curs, table_name, oplaadpaal_remote)
        else:
            update_oplaadpaal(curs, table_name, id1, "Deleted")
            delete_count += 1

    return delete_count


def main():
    """
    First we get a list of all chargepoints from Allego in Amsterdam

    Then we update the status of  all the oplaadpunten that are already present in the database
    If it is not present in the database we will get the details from Allego and add it.
    At most we will add max_inserts chargepoints. This is in order not to do too many
    calls to the webservice at the same time.

    If max_inserts is set 100 and the script is run 4 times per hour then we will have loaded all
    the 2269 chargepoints in 6 hours.

    If the last time details were loaded was more then 100 days ago we will reload it, in case it was
    changed in the  last 100 days.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--max_inserts",
        type=int,
        help="Maximum number of new inserts",
        default=100,
    )
    args = parser.parse_args()

    all_oplaadpunten = get_all_oplaadpunten()
    l = len(all_oplaadpunten)
    log.info(f"Loaded {l} oplaadpunten")
    total_inserts = 0
    total_updates = 0
    total_complete_updates = 0
    table_name = "oplaadpalen_new"
    try:
        dsn = f"dbname='{dbname}' user='{user}' password='{password}' host='{host}' port={port}"
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as curs:
                for opl in all_oplaadpunten:
                    id1 = opl["cpId"]
                    status = opl["st"]
                    if status == "Deleted":
                        continue
                    log.debug(f"{id1} {status}")
                    oplaadpaal_db = get_oplaadpaal(curs, table_name, id1)
                    if oplaadpaal_db:
                        # randomize complete update
                        max_age = LAADPAAL_MAX_AGE + random.randint(-10000, 10000)
                        if (
                            oplaadpaal_db[3] > max_age
                            and total_complete_updates < args.max_inserts
                        ):
                            oplaadpaal_remote = get_remote_oplaadpaal(id1)
                            if oplaadpaal_remote:
                                update_complete_oplaadpaal(
                                    curs, table_name, oplaadpaal_remote
                                )
                                total_complete_updates += 1
                        else:
                            update_oplaadpaal(curs, table_name, id1, status)
                            total_updates += 1
                    else:
                        if total_inserts < args.max_inserts:
                            oplaadpaal_remote = get_remote_oplaadpaal(id1)
                            if oplaadpaal_remote:
                                create_oplaadpaal(curs, table_name, oplaadpaal_remote)
                                total_inserts += 1
                        else:
                            pass
            with conn.cursor() as curs:
                total_set_unknown = set_oplaadpalen_unknown(curs, table_name)

            with conn.cursor() as curs:
                total_deletes = set_oplaadpalen_deleted(
                    curs, table_name, args.max_inserts
                )

        log.info(f"Inserted {total_inserts} laadpalen")
        log.info(f"Updated {total_updates} laadpalen")
        log.info(f"Updated completely {total_complete_updates} laadpalen")
        log.info(f"Not updated, set to 'Unknown' {total_set_unknown} laadpalen")
        log.info(f"Deleted' {total_deletes} laadpalen")

    except psycopg2.Error as exc:
        print("Unable to connect to the database", exc)
        exit(1)


if __name__ == "__main__":
    main()

# Example output from Allego laadpunten API :
#
# Laadpalen https://www.allego.eu/nl-nl/consumenten/vind-een-laadpunt ->
#  https://www.allego.eu/api/feature/experienceaccelerator/areas/chargepointmap/getchargepoints?firstPoint=52.394364402625705,4.856992783024908&secondPoint=52.39146726734137,4.8506627697497615
# [
#     {
#         cpId: "NLALLEGO006000",
#         st: "Available",
#         latLng: [
#             52.392244,
#             4.853768
#         ],
#         poiSource: "Allego"
#     },
#     {
#         cpId: "NLALLEGO006001",
#         st: "Available",
#         latLng: [
#             52.392244,
#             4.853768
#         ],
#         poiSource: "Allego"
#     },
#     {
#         cpId: "NLALLEGO001973",
#         st: "Available",
#         latLng: [
#             52.392627,
#             4.853129
#         ],
#         poiSource: "Allego"
#     },
#     {
#         cpId: "Nuon EVB-P1541066",
#         st: "Available",
#         latLng: [
#             52.392249,
#             4.851789
#         ],
#         poiSource: "OplaadpalenNl"
#     },
#     {
#         cpId: "Nuon EVB-P1811451",
#         st: "Available",
#         latLng: [
#             52.393693,
#             4.854871
#         ],
#         poiSource: "OplaadpalenNl"
#     },
#     {
#         cpId: "Nuon EVB-P1811122",
#         st: "Available",
#         latLng: [
#             52.393543,
#             4.854866
#         ],
#         poiSource: "OplaadpalenNl"
#     },
#     {
#         cpId: "NewMotion 02005042",
#         st: "Available",
#         latLng: [
#             52.3915574,
#             4.85271809999995
#         ],
#         poiSource: "OplaadpalenNl"
#     },
#     {
#         cpId: "Alfen ENECO_0013991",
#         st: "Available",
#         latLng: [
#             52.394279,
#             4.854705
#         ],
#         poiSource: "OplaadpalenNl"
#     },
#     {
#         cpId: "Alfen ENECO_0013992",
#         st: "Available",
#         latLng: [
#             52.394279,
#             4.854705
#         ],
#         poiSource: "OplaadpalenNl"
#     }
# ]


#  https://www.allego.eu/api/feature/experienceaccelerator/areas/chargepointmap/getchargepoints/NLALLEGO006000
# {
#     address: {
#         addressLine1: "transformatorweg 28",
#         addressLine2: null,
#         postalCode: "1014 AK",
#         city: "Amsterdam",
#         stateProvince: null,
#         country: "NL"
#     },
#     capabilities: [
#         "CreateTicketAM",
#         "SmoovCompatible",
#         "ViaCommunicator",
#         "SmsResetEnabled"
#     ],
#     contact: {
#         contactCenter: "VANAD NL",
#         email: null,
#         phone: "+318003745337",
#         website: null
#     },
#     evses: [
#         {
#             connectorType: "IEC_62196_T2_COMBO",
#             current: "DC",
#             displayName: "CCS",
#             id: 2,
#             maxPower: 50,
#             status: "Available"
#         },
#         {
#             connectorType: "CHADEMO",
#             current: "DC",
#             displayName: "CHADEMO",
#             id: 1,
#             maxPower: 50,
#             status: "Available"
#         }
#     ],
#     sources: [
#         "Allego"
#     ],
#     cpoOrganisationId: "Allego",
#     id: "NLALLEGO006000",
#     chargePointId: "NLALLEGO006000",
#     chargePointStatus: "Available",
#     connectivityStatus: "Online",
#     location: {
#         latitude: 52.392244,
#         longitude: 4.853768
#     }
# }
