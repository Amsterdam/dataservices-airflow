from typing import Final

# Converting the BBOX to a GEOMETRY datatype
CONVERT_TO_GEOM: Final = """
ALTER TABLE {{ params.tablename }} ADD bbox_new GEOMETRY;
WITH bbox_to_geo AS
(
SELECT ST_MakeEnvelope(arr[1]::double precision,
                       arr[2]::double precision,
                       arr[3]::double precision,
                       arr[4]::double precision,
                       28992) as bbox_geom, id
FROM (SELECT bbox AS arr, id FROM {{ params.tablename }}  ) as subquery

)
UPDATE {{ params.tablename }}
SET bbox_new = bbox_geom
FROM bbox_to_geo
AND {{ params.tablename }}.ID = bbox_to_geo.ID;
ALTER TABLE {{ params.tablename }} DROP COLUMN bbox;
ALTER TABLE {{ params.tablename }} RENAME bbox_new TO bbox;
"""
