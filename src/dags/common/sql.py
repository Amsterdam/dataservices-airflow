from typing import Final

SQL_DROP_TABLE: Final = """
    BEGIN;
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
    COMMIT;
"""

# In case of a collectionGeometry, that is flagged as invalid when using `ST_IsValid`,
# we need to extract the relevant geometry that will be expected in the database.
# To do so, specifiy the value for `geom_type_number` for the geometry type want to extract
# like so: 1 == POINT, 2 == LINESTRING, 3 == POLYGON
SQL_GEOMETRY_VALID: Final = """
    {% set srid = params.srid|default(28992, true) %}
    {% set geo_column = params.geo_column|default("GEOMETRY", true) %}
    UPDATE {{ params.tablename }}
    {% if params.geom_type_number is defined %}
    SET {{ geo_column }} = ST_MakeValid(
      ST_SetSRID(
        ST_GeomFromText(
          ST_AsText(
            ST_CollectionExtract({{ geo_column }}, {{ params.geom_type_number }}))), {{ srid }}))
    {% else %}
    SET {{ geo_column }} = ST_MakeValid({{ geo_column }})
    {% endif %}
    WHERE ST_IsValid({{ geo_column }}) = false;
    COMMIT;
"""

SQL_TABLE_RENAME: Final = """
    {% set geo_column = params.geo_column|default("wkb_geometry", true) %}
    {% set pk = params.pk|default("pk", true) %}
    ALTER TABLE IF EXISTS {{ params.tablename }} RENAME TO {{ params.tablename }}_old;
    ALTER TABLE {{ params.tablename }}_new RENAME TO {{ params.tablename }};
    DROP TABLE IF EXISTS {{ params.tablename }}_old;
    ALTER INDEX {{ params.tablename }}_new_{{ pk }} RENAME TO {{ params.tablename }}_pk;
    ALTER INDEX {{ params.tablename }}_new_{{ geo_column }}_geom_idx
      RENAME TO {{ params.tablename }}_{{ geo_column }}_geom_idx;
"""

SQL_TABLE_RENAMES: Final = """
    {% set geo_column = params.geo_column|default("wkb_geometry", true) %}
    {% for tablename in params.tablenames %}
      ALTER TABLE IF EXISTS {{ tablename }} RENAME TO {{ tablename }}_old;
      ALTER TABLE {{ tablename }}_new RENAME TO {{ tablename }};
      DROP TABLE IF EXISTS {{ tablename }}_old;
      ALTER INDEX {{ tablename }}_new_pk RENAME TO {{ tablename }}_pk;
      ALTER INDEX {{ tablename }}_new_{{ geo_column }}_geom_idx
        RENAME TO {{ tablename }}_{{ geo_column }}_geom_idx;
    {% endfor %}
"""

SQL_CHECK_COUNT: Final = """
    SELECT 1 FROM {{ params.tablename }} HAVING count(*) >= {{ params.mincount }}
"""

SQL_CHECK_GEO: Final = """
  {% set geo_column = params.geo_column|default("wkb_geometry", true) %}
  SELECT 1 WHERE NOT EXISTS (
      SELECT FROM {{ params.tablename }} WHERE
        {% if params.notnull|default(true) %}
        {{ geo_column }} IS null OR
        {% else %}
        {{ geo_column }} IS NOT NULL AND
        {% endif %}
        (
        {% if params.check_valid|default(true) %} ST_IsValid({{ geo_column }}) = false {% endif %}
        {% if params.check_valid|default(true) %} OR {% endif %} ST_GeometryType({{ geo_column }})
        {% if params.geotype is string %}
          <> '{{ params.geotype }}'
        {% else %}
          NOT IN ({{ params.geotype | map('quote') | join(", ") }})
        {% endif %}
        )
      )
"""

SQL_CHECK_COLNAMES: Final = """
    SELECT column_name FROM information_schema.columns WHERE
      table_schema = 'public' AND table_name = '{{ params.tablename }}'
    ORDER BY column_name
"""
