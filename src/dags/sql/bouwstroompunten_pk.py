from typing import Final

# Because ogr2ogr cannot set a PK on an non-integer datatype, the PK is re-defined
ADD_PK: Final = """
    ALTER TABLE {{ params.tablename }} DROP COLUMN ogc_fid;
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT "bouwstroompunten_new_pk" PRIMARY KEY ("id");
"""
