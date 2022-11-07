from typing import Final

BBGA_SQLITE_TRANSFORM_SCRIPT: Final[
    str
] = """
DROP TABLE IF EXISTS bbga_indicatoren_definities;
/*
 Mind the order of the columns; it matches the order of the fields
 of the CSV file to be imported.
 */
CREATE TABLE IF NOT EXISTS bbga_indicatoren_definities
(
    sort                        BIGINT,
    begrotings_programma        VARCHAR,
    thema                       VARCHAR,
    variabele                   VARCHAR PRIMARY KEY,
    label                       VARCHAR,
    label_kort                  VARCHAR,
    definitie                   VARCHAR,
    bron                        VARCHAR,
    peildatum                   VARCHAR,
    verschijningsfrequentie     VARCHAR,
    rekeneenheid                BIGINT,
    symbool                     VARCHAR,
    groep                       VARCHAR,
    format                      VARCHAR,
    berekende_variabelen        VARCHAR,
    thema_kerncijfertabel       VARCHAR,
    tussenkopje_kerncijfertabel VARCHAR,
    kleurenpalet                BIGINT,
    legenda_code                BIGINT,
    sd_minimum_bev_totaal       BIGINT,
    sd_minimum_wvoor_bag        BIGINT,
    topic_area                  VARCHAR,
    label_1                     VARCHAR,
    definition                  VARCHAR,
    reference_date              VARCHAR,
    frequency                   VARCHAR
);

DROP TABLE IF EXISTS bbga_statistieken;
/*
 Mind the order of the columns; it matches the order of the fields
 of the CSV file to be imported.
 */
CREATE TABLE IF NOT EXISTS bbga_statistieken
(
    jaar                   BIGINT,
    indicator_definitie_id VARCHAR,
    gemiddelde             DOUBLE PRECISION,
    standaardafwijking     DOUBLE PRECISION,
    bron                   VARCHAR
);

DROP TABLE IF EXISTS bbga_kerncijfers;
/*
 Mind the order of the columns; it matches the order of the fields
 of the CSV file to be imported.
 */
CREATE TABLE IF NOT EXISTS bbga_kerncijfers
(
    jaar                   BIGINT,
    gebiedcode_15          VARCHAR,
    indicator_definitie_id VARCHAR,
    waarde                 DOUBLE PRECISION
);

/*
 We have different column names and the tables have already been created
 hence we can skip the header row.
 */
.mode csv
.import metadata_latest_and_greatest.csv bbga_indicatoren_definities --skip 1

.mode list
.separator ";"
.import bbga_std_latest_and_greatest.csv bbga_statistieken --skip 1
.import bbga_latest_and_greatest.csv bbga_kerncijfers --skip 1

/*
 We use Django (REST Framework) to expose an API based on these tables.
 Unfortunately Django does not support composite primary keys. Hence
 we need to create our own. Quite sad. However this whole SQLite3
 exercise is because of the requirement to derive a primary key.
 SQLite3 can not only easily import and export CSV files, it executes
 these transformations so much faster than pure Python code.
 */
ALTER TABLE bbga_kerncijfers
    ADD COLUMN id VARCHAR NOT NULL GENERATED ALWAYS AS (indicator_definitie_id || '|' || jaar || '|' || gebiedcode_15) VIRTUAL;

ALTER TABLE bbga_statistieken
    ADD COLUMN id VARCHAR NOT NULL GENERATED ALWAYS AS (indicator_definitie_id || '|' || jaar) VIRTUAL;

.headers on
.mode csv

/*
 Overwrite the original CSV files with new headers and additional columns
 */
.once metadata_latest_and_greatest.csv
SELECT * FROM bbga_indicatoren_definities;

.once bbga_std_latest_and_greatest.csv
SELECT * FROM bbga_statistieken;

.once bbga_latest_and_greatest.csv
SELECT * FROM bbga_kerncijfers;

.quit
"""  # noqa: E501
