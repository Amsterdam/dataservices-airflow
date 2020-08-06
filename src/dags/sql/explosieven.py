# Removing some source fields that are replaced by other fields
REMOVE_COLS = """
    ALTER TABLE {{ params.tablename }} DROP COLUMN IF EXISTS objectid, 
        DROP COLUMN IF EXISTS objectid_1, 
        DROP COLUMN IF EXISTS shape_leng,
        DROP COLUMN IF EXISTS shape_area,
        DROP COLUMN IF EXISTS type,
        DROP COLUMN IF EXISTS shape_le_1;
    {% if 'gevrijwaardgebied' in params.tablename %}
    ALTER TABLE {{ params.tablename }} DROP COLUMN IF EXISTS datum;
    {% endif %}
"""

ADD_HYPERLINK_PDF = """
    {% if 'bominslag' in params.tablename %}
    UPDATE {{ params.tablename }} set PDF = CASE WHEN PDF is not null THEN 'https://files.data.amsterdam.nl/bommenkaart/'||PDF end WHERE 1=1;
    COMMIT;
    {% endif %}
"""
