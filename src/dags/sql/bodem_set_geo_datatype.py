from typing import Final

# Because geometry is ignore when importing .csv with ogr2ogr, it's set explicitly
SET_GEOM: Final = """
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(POINT, 0) using ST_GeomFromText(CASE WHEN geometrie LIKE 'POINT%' THEN geometrie ELSE NULL END);
    CREATE INDEX {{ params.tablename }}_idx ON {{ params.tablename }} USING GIST (geometrie);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(point, 28992) USING ST_SetSRID(geometrie, 28992);
"""
