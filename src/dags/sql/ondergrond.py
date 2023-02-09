from typing import Final

# convert geometry datatype to geometry (generic) so
# it can process multi geometry types; like multipolygon and
# collections in the same table.
SET_DATATYPE_GEOM_TO_GEOM: Final = """
ALTER TABLE {{ params.tablename }} ALTER COLUMN {{ params.geo_column }}
TYPE GEOMETRY(GEOMETRY, {{ params.srid }}) USING ST_Multi({{ params.geo_column }});
COMMIT;
"""
