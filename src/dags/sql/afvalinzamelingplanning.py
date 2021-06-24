from typing import Final

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""
