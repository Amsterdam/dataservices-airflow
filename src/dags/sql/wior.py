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

# Due to source data errors, duplicate `ID` values can occur. The source
# maintainer should fix the data.
# To prevent the discontinuation of the whole data set, the duplicate rows
# are removed from processing.
# The `ctid` is used as the native internal identifier to remove a duplicate
# row.
# The duplicates are ordered based on validity of their geometry. True first,
# false last.
SQL_DEL_DUPLICATE_ROWS: Final = """
    WITH duplicates AS (
    SELECT
    CTID,
    ROW_NUMBER () OVER (PARTITION BY {{ params.dupl_id_col }}
                        ORDER BY NOT ST_IsValid({{ params.geom_col }})) dulicate_num,
    ST_IsValid({{ params.geom_col }}) ind_valid
    FROM {{ params.tablename }}
    )
    DELETE FROM {{ params.tablename }}
    WHERE CTID in (
            SELECT CTID FROM duplicates
            WHERE ind_valid = false OR dulicate_num > 1);
    COMMIT;
"""
