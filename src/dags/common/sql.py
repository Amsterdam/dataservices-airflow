SQL_DROP_TABLE = """
    BEGIN;
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
    COMMIT;
"""

SQL_TABLE_RENAME = """
    {% set pk = params.pk|default("pk", true) %}
    ALTER TABLE IF EXISTS {{ params.tablename }} RENAME TO {{ params.tablename }}_old;
    ALTER TABLE {{ params.tablename }}_new RENAME TO {{ params.tablename }};
    DROP TABLE IF EXISTS {{ params.tablename }}_old;
    ALTER INDEX {{ params.tablename }}_new_{{ pk }} RENAME TO {{ params.tablename }}_pk;
    ALTER INDEX {{ params.tablename }}_new_wkb_geometry_geom_idx
      RENAME TO {{ params.tablename }}_wkb_geometry_geom_idx;
"""

SQL_TABLE_RENAMES = """
    BEGIN;
    {% for tablename in params.tablenames %}
      ALTER TABLE IF EXISTS {{ tablename }} RENAME TO {{ tablename }}_old;
      ALTER TABLE {{ tablename }}_new RENAME TO {{ tablename }};
      DROP TABLE IF EXISTS {{ tablename }}_old;
      ALTER INDEX {{ tablename }}_new_pk RENAME TO {{ tablename }}_pk;
      ALTER INDEX {{ tablename }}_new_wkb_geometry_geom_idx
        RENAME TO {{ tablename }}_wkb_geometry_geom_idx;
    {% endfor %}
    COMMIT;
"""

SQL_CHECK_COUNT = """
    SELECT 1 FROM {{ params.tablename }} HAVING count(*) >= {{ params.mincount }}
"""

SQL_CHECK_GEO = """
  {% set geo_column = params.geo_column|default("wkb_geometry", true) %}
  SELECT 1 WHERE NOT EXISTS (
      SELECT FROM {{ params.tablename }} WHERE
        {{ geo_column }} IS null OR
        ST_IsValid({{ geo_column }}) = false
          OR ST_GeometryType({{ geo_column }}) <> '{{ params.geotype }}'
      )
"""

SQL_CHECK_COLNAMES = """
    SELECT column_name FROM information_schema.columns WHERE
      table_schema = 'public' AND table_name = '{{ params.tablename }}'
    ORDER BY column_name
"""
