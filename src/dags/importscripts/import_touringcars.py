import json, logging
from geojson import loads, Polygon, MultiPolygon, GeometryCollection, Point, MultiLineString, LineString

LOGGER = logging.getLogger("airflow.task")


def json2geojson(data, in_file):
    """ Transform of json input to geojson """

    TOURINGCARS = [
        
        {
        "id":"doorrijhoogtes",
        "root":"max_doorrijhoogtes",
        "element":"max_doorrijhoogte",
        "properties":["title", "Maximale_doorrijhoogte"],
        },
        
        {
        "id":"haltes",
        "root":"in_uitstaphaltes",
        "element":"in_uitstaphalte",
        "properties":["title", "Bijzonderheden", "Busplaatsen"],
        },

        {
        "id":"parkeerplaatsen",
        "root":"parkeerplaatsen",
        "element":"parkeerplaats",
        "properties":["title", "Bijzonderheden", "Busplaatsen", "linkurl", "linknaam"],
        },
        
        {
        "id":"wegwerkzaamheden",
        "root":"wegwerkzaamheden",
        "element":"wegopbreking",
        "properties":["title", "opmerkingen", "linkurl", "linknaam"],
        },

        {
        "id":"verplichteroutes",
        "root":"verplichte_routes",
        "element":"verplichte_route",
        "properties":["title"],
        },

        {
        "id":"aanbevolenroutes",
        "root":"aanbevolen_routes",
        "element":"aanbevolen_route",
        "properties":["title"],
        },

    ]
    
    geojson_features = []
    dataset = {}
    element = {}
    properties = []

    # setup processing variables
    for tables in TOURINGCARS:
       
        # get the set that matches the given input parameter 'in_file'
        if (tables.get('id')) in in_file:
            
            # set variables
            dataset = data[tables['root']]
            element = tables['element']
            properties = tables['properties']             
    

    for rows in dataset:

            row = rows[element]                    
            
            geometry = {}
            multipolygon = None
            point = None
            multilinestring = None
            geo = loads(row["Lokatie"])
            print(type(geo))
           

            properties = {
                key: row[key] for key in properties
            }
            
            if type(geo) is not GeometryCollection:

                # Extract multipolygon or points
                if type(geo) is MultiPolygon:
                    multipolygon = geo
                    geometry = geo

                if type(geo) is Point:
                    point = geo
                    geometry = geo
                
                if type(geo) is MultiLineString or type(geo) is LineString:
                    multilinestring = geo
                    geometry = geo
                
                # add geo element to feature list
                geojson_features.append(
                    {"type": "Feature", "geometry": geometry, "properties": properties,}
                )

            # if the type is a collection of elements, then each geo element gets added to the feature list separately
            elif type(geo) is GeometryCollection:
                
                for geo_element in geo["geometries"]:
                    
                    if type(geo_element) is Polygon:
                        multipolygon = MultiPolygon(geo_element)
                        geometry = geo_element
                        break
                    if type(geo_element) is MultiPolygon:
                        multipolygon = geo_element
                        geometry = geo_element
                        break
                    if type(geo_element) is LineString:
                        multilinestring = geo_element
                        geometry = geo_element 
                        
                    # add each geo element to the feature list
                    geojson_features.append(
                            {"type": "Feature", "geometry": geometry, "properties": properties,}
                            )        

            if multipolygon is None and point is None and multilinestring is None:
                LOGGER.error("Missing geometry: (multi)polygon / point / (multi)linestring")
                raise Exception("Missing geometry: (multi)polygon / point / (multi)linestring")            
            
           
    geojson = {"type": "FeatureCollection", "features": [feature for feature in geojson_features]}
    return geojson

def import_touringcars(in_file, out_file):
    """ Loading json file and calling function to transform json to geojson"""

    data = json.load(open(in_file))
   
    geojson = json2geojson(data, in_file)
    output = open(out_file, "w")

    LOGGER.info(f"writing output file: {out_file}")
    json.dump(geojson, output)
    LOGGER.info("done")


