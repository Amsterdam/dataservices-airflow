from typing import Final

# Because geometry is ignore when importing .csv with ogr2ogr, it's set explicitly
SET_GEOM: Final = """
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometry TYPE geometry(MULTIPOLYGON, 0) using ST_GeomFromText(CASE WHEN geometry LIKE 'MULTIPOLYGON%' THEN geometry ELSE NULL END);
    CREATE INDEX {{ params.tablename }}_idx ON {{ params.tablename }} USING GIST (geometry);
    ALTER TABLE {{ params.tablename }} ALTER COLUMN geometry TYPE geometry(MULTIPOLYGON, 28992) USING ST_SetSRID(geometry, 28992);
"""

# Adding referenced information from parent table to child table
ADD_THEMA_CONTEXT: Final = """
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS thema varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS thema_toelichting varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS thema_wet_of_regelgeving varchar(1000);
    ALTER TABLE {{ params.tablename }} ADD COLUMN IF NOT EXISTS thema_datum_laatste_wijziging varchar(1000);

    WITH {{ params.tablename }}_context as (
    SELECT
    {{ params.parent_table }}.type,
    {{ params.parent_table }}.toelichting,
    {{ params.parent_table }}.wet_of_regelgeving,
    {{ params.parent_table }}.datum_laatste_wijziging,
    {{ params.tablename }}.ID as table_id
    FROM {{ params.parent_table }}
    INNER JOIN {{ params.tablename }} ON {{ params.parent_table }}.THEMA_ID = {{ params.tablename }}.TMA_ID
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
    {% if 'metro' or 'spoorwegen' in params.tablename %}
    ALTER TABLE {{ params.tablename }} DROP COLUMN IF EXISTS naam_industrieterrein;
    {% endif %}
"""
