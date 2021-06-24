from typing import Final

# Splitting the data out so each table has it's own records.
# The source file contains all data in one single file.
# In the schema specification the data is devided over tables.
# The ID is the unique stable business identification.
DEL_ROWS: Final = """
{% if 'vrachtauto' in params.tablename %}
DELETE FROM {{ params.tablename }} WHERE ID != '63362';
{% elif 'taxi' in params.tablename %}
DELETE FROM {{ params.tablename }} WHERE ID != '63360';
{% elif 'bestel' in params.tablename %}
DELETE FROM {{ params.tablename }} WHERE ID != '63363';
{% elif 'brom' in params.tablename %}
DELETE FROM {{ params.tablename }} WHERE ID != '63324';
{% elif 'touring' in params.tablename %}
DELETE FROM {{ params.tablename }} WHERE ID != '63361';
{% endif %}
COMMIT;
"""

# Removing inrelevant cols
DROP_COLS: Final = """
    ALTER TABLE {{ params.tablename }} DROP COLUMN IF EXISTS FID;
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT {{ params.tablename }}_pk PRIMARY KEY (ID);
"""
