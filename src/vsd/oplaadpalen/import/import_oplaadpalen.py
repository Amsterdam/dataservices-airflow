import argparse
import os.path
import pickle
import sys
import time
from collections import OrderedDict

from zeep import Client

# DEPRECATED SCRIPT : endpoint is deprecated

# python -mzeep https://emobilitygetstatchargingpointvipprdnl.azurewebsites.net/GetStatChargingStationProxy.svc?wsdl
# python -mzeep https://emobilitygetdynchargingpointvipprdnl.azurewebsites.net/GetDynChargingPointProxy.svc?wsdl
# python -m zeep https://api.essent.nl/partner/a2a/cs/getStaticChargingStations?wsdl

wsdl = "https://emobilitygetstatchargingpointvipprdnl.azurewebsites.net/GetStatChargingStationProxy.svc?wsdl"
# wsdl = 'https://emobilitygetdynchargingpointvipprdnl.azurewebsites.net/GetDynChargingPointProxy.svc?wsdl'

# wsdl = 'https://api.essent.nl/partner/a2a/cs/getStaticChargingStations?wsdl'


def get_static_data():
    datafile = "/tmp/static_oplaadpalen.dat"

    if (
        os.path.isfile(datafile)
        and time.time() - os.path.getmtime(datafile) < 24 * 60 * 60
    ):
        fd = open(datafile, "rb")
        result = pickle.load(fd)
        fd.close()
    else:
        client = Client(wsdl)
        location = {
            "Latitude": {"high": 52.431689, "low": 52.282187},
            "Longitude": {"high": 5.079548, "low": 4.738972},
        }

        result = client.service.GetStaticChargingStations_Operation(Location=location)

        fw = open(datafile, "wb")
        pickle.dump(result, fw)
        fw.close()

    return result


def makesrid28992(lat, lon):
    return f"ST_Transform(ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326), 28992)"


def q(val):
    if val is None:
        return "NULL"
    else:
        return f"'{val}'"


def make_insert(**kwargs):
    lat = kwargs.pop("latitude")
    lon = kwargs.pop("longitude")
    wkb_geometry = makesrid28992(lat, lon)

    insert = f"""insert into oplaadpalen_new(
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
) values (
   '{kwargs['cs_external_id']}'
,  {wkb_geometry}
,  {q(kwargs['street'])}
,  {q(kwargs['housenumber'])}
,  {q(kwargs['housenumberext'])}
,  {q(kwargs['postalcode'])}
,  {q(kwargs['district'])}
,  {q(kwargs['countryiso'])}
,  {q(kwargs['city'])}
,  {q(kwargs['provider'])}
,  {q(kwargs['restrictionsremark'])}
,  {kwargs['charging_point']}
,  '{kwargs['status']}'
,  '{kwargs['connector_type']}'
,  '{kwargs['vehicle_type']}'
,  '{kwargs['charging_capability']}'
,  '{kwargs['identification_type']}'
);
"""
    return insert


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=str, help="output file")
    args = parser.parse_args()

    result = get_static_data()
    status = result.Messages.Message[0].Code["_value_1"]
    if status != "Success":
        print("Error getting data: " + result.Messages.Message[0].Message["_value_1"])
        sys.exit(1)

    inserts = []
    for chargingStation in result.ChargingStations.ChargingStation:
        status_list = []
        connector_type_list = []
        vehicle_type_list = []
        charging_capability_list = []
        identification_type_list = []
        charging_point = 0
        for chargingPoint in chargingStation.ChargingPoints.ChargingPoint:
            charging_point += 1
            status_list.append(
                chargingPoint.Status if chargingPoint.Status is not None else "None"
            )
            connector_type_list.append(chargingPoint.ConnectorType)
            vehicle_type_list.append(chargingPoint.VehicleType)
            charging_capability_list.append(chargingPoint.ChargingCapability)
            identification_type_list.append(chargingPoint.IdentificationType)

        postalcode = chargingStation["Location"]["PostalCode"].replace(" ", "")
        street = chargingStation["Location"]["Street"].replace("'", "''")

        kwargs = {
            "cs_external_id": chargingStation["CSExternalID"],
            "latitude": chargingStation["Location"]["Latitude"],
            "longitude": chargingStation["Location"]["Longitude"],
            "street": street,
            "housenumber": chargingStation["Location"]["HouseNumber"],
            "housenumberext": chargingStation["Location"]["HouseNumberExt"],
            "postalcode": postalcode,
            "district": chargingStation["Location"]["District"],
            "countryiso": chargingStation["Location"]["CountryISO"],
            "city": chargingStation["Location"]["City"]["_value_1"]
            if chargingStation["Location"]["City"] is not None
            else "",
            "provider": chargingStation["Provider"],
            "restrictionsremark": chargingStation["RestrictionsRemark"],
            "status": ";".join(list(OrderedDict.fromkeys(status_list))),
            "charging_point": charging_point,
            "connector_type": ";".join(list(OrderedDict.fromkeys(connector_type_list))),
            "vehicle_type": ";".join(list(OrderedDict.fromkeys(vehicle_type_list))),
            "charging_capability": ";".join(
                list(OrderedDict.fromkeys(charging_capability_list))
            ),
            "identification_type": ";".join(
                list(OrderedDict.fromkeys(identification_type_list))
            ),
        }

        inserts.append(make_insert(**kwargs))

        # Write file
        with open(args.output, "w") as f:
            f.write("\n".join(inserts))


if __name__ == "__main__":
    main()
