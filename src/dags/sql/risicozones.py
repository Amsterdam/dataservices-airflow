# Because geometry is ignored when importing .csv with ogr2ogr, it's set explicitly
SET_GEOM = """
    {% if 'lpgtankstation' in params.tablename or 'bedrijven' in params.tablename or 'lpgvulpunt' in params.tablename or 'lpgafleverzuil' in params.tablename  %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie_punt TYPE geometry(POINT, 0) using ST_GeomFromText(CASE WHEN geometrie_punt LIKE 'POINT%' THEN geometrie_punt ELSE NULL END);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie_punt TYPE geometry(POINT, 28992) USING ST_SetSRID(geometrie_punt, 28992);
    CREATE INDEX {{ params.tablename }}_geom_punt_idx ON {{ params.tablename }} USING GiST (geometrie_punt);
    {% endif %}

    {% if 'aardgasleiding' in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(MULTILINESTRING, 0) using ST_GeomFromText(CASE WHEN geometrie LIKE 'MULTILINESTRING%' THEN geometrie ELSE NULL END);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(MULTILINESTRING, 28992) USING ST_SetSRID(geometrie, 28992);

    {% elif 'infrastructuur' in params.tablename or 'bedrijven' in params.tablename or 'bronnen' in params.tablename or 'contouren' in params.tablename %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(MULTIPOLYGON, 0) using ST_GeomFromText(CASE WHEN geometrie LIKE 'MULTIPOLYGON%' THEN geometrie ELSE NULL END);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(MULTIPOLYGON, 28992) USING ST_SetSRID(geometrie, 28992);

    {% else %}
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(POLYGON, 0) using ST_GeomFromText(CASE WHEN geometrie LIKE 'POLYGON%' THEN geometrie ELSE NULL END);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(POLYGON, 28992) USING ST_SetSRID(geometrie, 28992);

    {% endif %}
    CREATE INDEX {{ params.tablename }}_geom_idx ON {{ params.tablename }} USING GiST (geometrie);
"""

# Drop temp table after use
SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""
