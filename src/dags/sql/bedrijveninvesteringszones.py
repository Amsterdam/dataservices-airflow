from typing import Final

# CREATE TABLE
# the SQL DDL and DML from shp data that is extracted by OGR2OGR
# is translated into a new set of DML's (new insert statements with enriched data)
# therefor the target table must me create within this template in order to run the new insert statements
CREATE_TABLE: Final = """
    DROP TABLE IF EXISTS {{ params.tablename }};
    CREATE TABLE {{ params.tablename }} (
        id integer PRIMARY KEY NOT NULL,
        geometry  geometry(Geometry,28992),
        naam varchar(100),
        type  varchar(100),
        heffingsgrondslag varchar(500),
        heffingstarief integer,
        heffing_valuta_code varchar(10) DEFAULT 'EUR',
        heffing_display varchar(100),
        website varchar(500),
        bijdrageplichtingen integer,
        verordening  varchar(500)
    );
    ALTER TABLE {{ params.tablename }} ADD CONSTRAINT {{ params.tablename }}_naam_unique UNIQUE (naam);
    CREATE INDEX {{ params.tablename }}_geom_idx ON {{ params.tablename }} USING gist (geometry);
"""

UPDATE_TABLE: Final = """
UPDATE {{ params.tablename }}
SET heffing_display =
        CASE WHEN heffingstarief IS NULL THEN NULL
        ELSE concat(E'\u20AC', ' ', cast(heffingstarief as character varying(10)))
        END;
COMMIT;
"""
