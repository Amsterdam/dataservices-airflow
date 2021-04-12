# merging into one table.
# the data is about 30 million rows
# fastest way to merge three resources

SQL_CREATE_TMP_TABLE = """
DROP TABLE IF EXISTS {{ params.tablename }} CASCADE;
CREATE TABLE IF NOT EXISTS {{ params.tablename }} AS (
    SELECT encode(
            md5(
                b.kenteken || coalesce(a.as_nummer, 0) || coalesce(br.brandstof_omschrijving, 'NA')
            )::text::bytea,
            'hex'
        ) as id,
        b.kenteken,
        TO_DATE(b.datum_eerste_toelating::text, 'YYYYMMDD') as datum_eerste_toelating,
        b.inrichting,
        b.lengte,
        b.massa_rijklaar,
        b.maximum_massa_samenstelling,
        b.toegestane_maximum_massa_voertuig,
        b.voertuigsoort,
        a.aantal_assen,
        a.as_nummer,
        a.technisch_toegestane_maximum_aslast,
        br.brandstof_omschrijving,
        br.emissiecode_omschrijving
    FROM rdw_basis_new b
        LEFT JOIN rdw_assen_new a ON b.kenteken = a.kenteken
        LEFT JOIN rdw_brandstof_new br ON b.kenteken = br.kenteken
);
CREATE INDEX {{ params.tablename }}_idx ON {{ params.tablename }} (kenteken);
ALTER TABLE {{ params.tablename }}
ADD CONSTRAINT {{ params.tablename }}_pk PRIMARY KEY (id);
DROP TABLE rdw_assen_new;
DROP TABLE rdw_brandstof_new;
DROP TABLE rdw_basis_new;
"""

# swap tmp to target
SQL_SWAP_TABLE = """
DROP TABLE IF EXISTS {{ params.tablename }};
ALTER TABLE
IF EXISTS {{ params.tablename }}_new RENAME TO {{ params.tablename }};
ALTER INDEX
IF EXISTS {{ params.tablename }}_new_idx RENAME TO {{ params.tablename }}_idx;
ALTER TABLE
IF EXISTS {{ params.tablename }} RENAME CONSTRAINT {{ params.tablename }}_new_pk
    TO {{ params.tablename }}_pk;
"""
