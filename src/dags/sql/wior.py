# Removing inrelevant cols from child tables
DROP_COLS = """
    ALTER TABLE {{ params.tablename }}
    DROP COLUMN IF EXISTS id,
    DROP COLUMN IF EXISTS fid,
    DROP COLUMN IF EXISTS f1d,
    DROP COLUMN IF EXISTS algemeen_totaal,
    DROP COLUMN IF EXISTS geplandeuitvoering_omni,
    DROP COLUMN IF EXISTS startdatum_omni,
    DROP COLUMN IF EXISTS einddatum_omni,
    DROP COLUMN IF EXISTS calamiteitverbergenininhoudscherm,
    DROP COLUMN IF EXISTS wkb_geometry,
    DROP COLUMN IF EXISTS status,
    DROP COLUMN IF EXISTS hoofdstatus,
    DROP COLUMN IF EXISTS bbox;
"""

# Removing temp table that was used for CDC (change data capture)
SQL_DROP_TMP_TABLE = """
    DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
"""

# Validate geometry
SQL_GEOM_VALIDATION = """
UPDATE {{ params.tablename }} SET geometry = ST_MakeValid(geometry);
COMMIT;
"""

# Add PK, because its needed for CDC
SQL_ADD_PK = """
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT {{ params.tablename }}_pk PRIMARY KEY (id);
"""

# Set date values DD-MM-YYYY to datetypes so it can be processed
SQL_SET_DATE_DATA_TYPES = """
SET datestyle = "ISO, DMY";
ALTER TABLE {{ params.tablename }}
ALTER COLUMN datum_registratie TYPE date USING datum_registratie::date,
ALTER COLUMN datum_publicatie TYPE date USING datum_publicatie::date,
ALTER COLUMN datum_start_uitvoering TYPE date USING datum_start_uitvoering::date,
ALTER COLUMN datum_einde_uitvoering TYPE date USING datum_einde_uitvoering::date,
ALTER COLUMN datum_akkoord_uitvoeringsvoorwaarden TYPE date USING
    datum_akkoord_uitvoeringsvoorwaarden::date;
"""
