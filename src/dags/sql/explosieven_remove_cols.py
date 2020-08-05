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
