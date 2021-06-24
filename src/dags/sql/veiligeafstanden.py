from typing import Final

# Because geometry is ignore when importing .csv with ogr2ogr, it's set explicitly
# in the geometry column a polygon and point type are present,
# therefore the general geometry type is used.
SET_GEOM: Final = """
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(GEOMETRY, 0)
    USING ST_GeomFromText(
        CASE WHEN geometrie LIKE 'POLYGON%' OR geometrie LIKE 'POINT%' THEN geometrie
        ELSE NULL
        END);
    CREATE INDEX {{ params.tablename }}_geom_idx ON {{ params.tablename }} USING GIST (geometrie);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometrie TYPE geometry(GEOMETRY, 28992)
    USING ST_SetSRID(geometrie, 28992);
"""

# Adding referenced information from parent table to child table
ADD_THEMA_CONTEXT: Final = """
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS thema varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS
    thema_toelichting varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS
    thema_wet_of_regelgeving varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS
    thema_datum_laatste_wijziging varchar(1000);

    WITH {{ params.tablename }}_context as (
    SELECT
    {{ params.parent_table }}.type,
    {{ params.parent_table }}.toelichting,
    {{ params.parent_table }}.wet_of_regelgeving,
    {{ params.parent_table }}.datum_laatste_wijziging,
    {{ params.tablename }}.ID as table_id
    FROM {{ params.parent_table }}
    INNER JOIN {{ params.tablename }}
    ON {{ params.parent_table }}.THEMA_ID = {{ params.tablename }}.TMA_ID
    )
    UPDATE {{ params.tablename }}
    SET thema = {{ params.tablename }}_context.type,
        thema_toelichting =  {{ params.tablename }}_context.toelichting,
        thema_wet_of_regelgeving =  {{ params.tablename }}_context.wet_of_regelgeving,
        thema_datum_laatste_wijziging =  {{ params.tablename }}_context.datum_laatste_wijziging
    FROM {{ params.tablename }}_context
    WHERE {{ params.tablename }}.ID = {{ params.tablename }}_context.table_id;
    COMMIT;
"""

# Removing inrelevant cols from child tables
DROP_COLS: Final = """
    ALTER TABLE {{ params.tablename }} DROP COLUMN IF EXISTS tma_id;
"""

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""
