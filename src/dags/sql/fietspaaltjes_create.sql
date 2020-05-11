DROP TABLE IF EXISTS fietspaaltjes_fietspaaltjes_new;

CREATE TABLE fietspaaltjes_fietspaaltjes_new (
    id varchar(32) PRIMARY KEY NOT NULL,
    geometry geometry(Geometry, 28992),
    street varchar(128),
    at varchar(128),
    area varchar(128),
    score_2013 varchar(8),
    score_current varchar(8),
    count integer,
    paaltjes_weg varchar(64)[],
    soort_paaltje varchar(64)[],
    uiterlijk varchar(64)[],
    type varchar(64)[],
    ruimte varchar(64)[],
    markering varchar(64)[],
    beschadigingen varchar(64)[],
    veiligheid varchar(64)[],
    zicht_in_donker varchar(64)[],
    soort_weg varchar(64)[],
    noodzaak varchar(64)[]
);

CREATE INDEX fietspaaltjes_fietspaaltjes_new_wkb_geometry_geom_idx 
    ON fietspaaltjes_fietspaaltjes_new USING gist (geometry);
