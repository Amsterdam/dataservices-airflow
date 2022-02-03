from typing import Final

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# Make geometry valid (if not)
# like for instance: Self-intersection issues
SQL_GEOMETRY_VALID: Final = """
    UPDATE {{ params.tablename }}
    SET GEOMETRY = ST_MakeValid(GEOMETRY)
    WHERE ST_IsValid(GEOMETRY) = false;
    COMMIT;
"""
