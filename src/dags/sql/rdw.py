from typing import Final

# Setting up temp table
# the main table rdw_voertuig has two
# child tables (assen en brandstof) that
# refer to rdw_voertuig and are used as
# an object (array) within the schema definition.
SQL_CREATE_TMP_TABLE: Final = """
DROP TABLE IF EXISTS rdw_voertuig_new CASCADE;
DROP TABLE IF EXISTS rdw_voertuig_assen_new CASCADE;
DROP TABLE IF EXISTS rdw_voertuig_brandstof_new CASCADE;
DROP TABLE IF EXISTS rdw_voertuig_carrosserie_new CASCADE;
CREATE TABLE IF NOT EXISTS rdw_voertuig_new AS (
    SELECT b.id,
        b.kenteken,
        TO_DATE(b.datum_eerste_toelating::text, 'YYYYMMDD') as datum_eerste_toelating,
        b.inrichting,
        b.lengte,
        b.massa_rijklaar,
        b.maximum_massa_samenstelling,
        b.toegestane_maximum_massa_voertuig,
        b.voertuigsoort
    FROM rdw_basis_download b
);
CREATE TABLE IF NOT EXISTS rdw_voertuig_assen_new AS (
    SELECT a.id,
        a.kenteken,
        a.aantal_assen,
        a.as_nummer,
        a.technisch_toegestane_maximum_aslast,
        b.id as parent_id
    FROM rdw_assen_download a
        inner join rdw_voertuig_new b on b.kenteken = a.kenteken
);
CREATE TABLE IF NOT EXISTS rdw_voertuig_brandstof_new AS (
    SELECT br.id,
        br.kenteken,
        br.brandstof_omschrijving,
        br.emissiecode_omschrijving,
        b.id as parent_id
    FROM rdw_brandstof_download br
        inner join rdw_voertuig_new b on b.kenteken = br.kenteken
);
CREATE TABLE IF NOT EXISTS rdw_voertuig_carrosserie_new AS (
    SELECT cs.id,
        cs.kenteken,
        cs.type_carrosserie_europese_omschrijving,
        b.id as parent_id
    FROM rdw_carrosserie_download cs
        inner join rdw_voertuig_new b on b.kenteken = cs.kenteken
);
CREATE INDEX rdw_voertuig_new_idx ON rdw_voertuig_new (kenteken);
CREATE INDEX rdw_voertuig_assen_new_fk_idx ON rdw_voertuig_assen_new (parent_id);
CREATE INDEX rdw_voertuig_brandstof_new_fk_idx ON rdw_voertuig_brandstof_new (parent_id);
CREATE INDEX rdw_voertuig_carrosserie_new_fk_idx ON rdw_voertuig_carrosserie_new (parent_id);
ALTER TABLE rdw_voertuig_new
ADD CONSTRAINT rdw_voertuig_new_pk PRIMARY KEY (id);
DROP TABLE rdw_assen_download;
DROP TABLE rdw_brandstof_download;
DROP TABLE rdw_basis_download;
DROP TABLE rdw_carrosserie_download;
"""

# swap tmp to target
SQL_SWAP_TABLE: Final = """
DROP TABLE IF EXISTS rdw_voertuig;
DROP TABLE IF EXISTS rdw_voertuig_brandstof;
DROP TABLE IF EXISTS rdw_voertuig_assen;
DROP TABLE IF EXISTS rdw_voertuig_carrosserie;
ALTER TABLE
IF EXISTS rdw_voertuig_new RENAME TO rdw_voertuig;
ALTER TABLE
IF EXISTS rdw_voertuig_brandstof_new RENAME TO rdw_voertuig_brandstof;
ALTER TABLE
IF EXISTS rdw_voertuig_assen_new RENAME TO rdw_voertuig_assen;
ALTER TABLE
IF EXISTS rdw_voertuig_carrosserie_new RENAME TO rdw_voertuig_carrosserie;
ALTER INDEX
IF EXISTS rdw_voertuig_new_idx RENAME TO rdw_voertuig_idx;
ALTER INDEX
IF EXISTS rdw_voertuig_assen_new_fk_idx RENAME TO rdw_voertuig_assen_fk_idx;
ALTER INDEX
IF EXISTS rdw_voertuig_brandstof_new_fk_idx RENAME TO rdw_voertuig_brandstof_fk_idx;
ALTER INDEX
IF EXISTS rdw_voertuig_carrosserie_new_fk_idx RENAME TO rdw_voertuig_carrosserie_fk_idx;
ALTER TABLE
IF EXISTS rdw_voertuig RENAME CONSTRAINT rdw_voertuig_new_pk
    TO rdw_voertuig_pk;
"""
