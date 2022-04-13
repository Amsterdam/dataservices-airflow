from typing import Final

# Removing inrelevant cols from child tables
DROP_COLS: Final = """
    ALTER TABLE {{ params.tablename }}
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS fid,
    DROP COLUMN IF EXISTS f1d,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS bbox;
"""

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# Validate geometry
SQL_GEOM_VALIDATION: Final = """
UPDATE {{ params.tablename }} SET geometry = ST_MakeValid(geometry);
COMMIT;
"""

# Add PK, because its needed for CDC
SQL_ADD_PK: Final = """
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT {{ params.tablename }}_pk PRIMARY KEY (id);
"""

# Set date values DD-MM-YYYY to datetypes so it can be processed
SQL_SET_DATE_DATA_TYPES: Final = """
SET datestyle = "ISO, DMY";
ALTER TABLE {{ params.tablename }}
ALTER COLUMN datum_start_uitvoering TYPE date USING datum_start_uitvoering::date,
ALTER COLUMN datum_einde_uitvoering TYPE date USING datum_einde_uitvoering::date;
"""
