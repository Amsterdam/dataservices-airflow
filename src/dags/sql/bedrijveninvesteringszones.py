from typing import Final

UPDATE_TABLE: Final = """
UPDATE {{ params.tablename }}
SET heffingstarief_display =
        CASE WHEN heffingstarief IS NULL THEN NULL
        ELSE concat(E'\u20AC', ' ', cast(heffingstarief as character varying(10)))
        END;
COMMIT;
"""
