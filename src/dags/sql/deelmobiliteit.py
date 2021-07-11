from typing import Final

# Converting geometry type to EPSG 28992
SQL_SET_GEOM: Final = """
    ALTER TABLE {{ params.tablename }}
    ALTER COLUMN geometrie TYPE geometry(GEOMETRY, 28992)
    USING ST_Transform(geometrie,28992);
"""

# Keep a short history time frame of 1 month
SQL_HISTORY_WINDOW: Final = """
    DELETE FROM {{ params.tablename }}
    AND DATUMTIJD_ONTVANGEN < (SELECT now() - INTERVAL '1 month');
    COMMIT;
"""

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# Because data is versioned based on source ingest time (column: datumtijd_ontvangen)
# the column indicatie_actueel is set to 1 (true) for the latest load (during import)
# other 'older' data ingests are set to value 0 (false).
# This makes it more convenient for the API caller and data analist
# to select the latest version.
SQL_FLAG_NOT_RECENT_DATA: Final = """
    UPDATE  {{ params.tablename }} SET indicatie_actueel = false;
    COMMIT;
"""
