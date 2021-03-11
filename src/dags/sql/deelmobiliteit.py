# Converting geometry type to EPSG 28992
SQL_SET_GEOM = """
    ALTER TABLE {{ params.tablename }}
    ALTER COLUMN geometrie TYPE geometry(GEOMETRY, 28992)
    USING ST_Transform(geometrie,28992);
"""

# Keep a short history time frame of 1 month
SQL_HISTORY_WINDOW = """
    DELETE FROM {{ params.tablename }}
    WHERE 1=1
    AND DATUMTIJD_ONTVANGEN < (SELECT now() - INTERVAL '1 month');
    COMMIT;
"""

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""
